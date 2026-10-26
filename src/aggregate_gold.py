"""
Gold Layer Aggregation & Dimensional Modeling Engine
Constructs the Healthcare Star Schema data warehouse and actuarial/clinical data marts:
1. Star Schema Dimensions:
   - Dim_Patient (De-identified demographics, age groups)
   - Dim_Provider (NPI, specialty, facility, in-network status)
   - Dim_Diagnosis (ICD-10 clinical taxonomy, chronic condition flags)
   - Dim_Date (Calendar date dimension: year, quarter, month, day, is_weekend)
   - Dim_Plan (Insurance line of business: Commercial, Medicare, Medicaid)
2. Star Schema Fact:
   - Fact_Claims (Adjudicated claim encounters, financials, lengths of stay)
3. Analytical Marts:
   - Mart 1: Per-Member-Per-Month (PMPM) Cost Analysis
   - Mart 2: 30-Day Hospital Readmission Rate Tracker by Facility
   - Mart 3: Top Clinical Cost Drivers (ICD-10 Spend Analysis)
   - Mart 4: Provider & Facility Adjudication Performance (Denial rates, volumes)

NOTE: This is a student portfolio project using 100% synthetic data.
No real patient, personal, or clinical data is used.
"""

import os
from datetime import datetime, timedelta

try:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window
    from pyspark.sql.types import (
        StructType, StructField, StringType, IntegerType, DateType, BooleanType, DoubleType
    )
    HAS_PYSPARK = True
except ImportError:
    HAS_PYSPARK = False


# -----------------------------------------------------------------------------
# 1. STAR SCHEMA GENERATION
# -----------------------------------------------------------------------------
def build_dim_date(spark, start_date_str: str = "2024-01-01", end_date_str: str = "2026-12-31"):
    """
    Generates a standard healthcare calendar dimension table (Dim_Date).
    Essential for time-series actuarial risk modeling, seasonal epidemiology,
    and CMS reporting periods without complex runtime date parsing.
    """
    if not HAS_PYSPARK:
        return None

    # Generate date sequence via Spark SQL sequence function
    df = (
        spark.sql(f"SELECT sequence(to_date('{start_date_str}'), to_date('{end_date_str}'), interval 1 day) as dates")
        .withColumn("calendar_date", F.explode("dates"))
        .select(
            F.date_format("calendar_date", "yyyyMMdd").cast("int").alias("date_key"),
            F.col("calendar_date"),
            F.year("calendar_date").alias("calendar_year"),
            F.quarter("calendar_date").alias("calendar_quarter"),
            F.month("calendar_date").alias("calendar_month"),
            F.date_format("calendar_date", "MMMM").alias("month_name"),
            F.date_format("calendar_date", "yyyyMM").alias("year_month"),
            F.dayofmonth("calendar_date").alias("day_of_month"),
            F.dayofweek("calendar_date").alias("day_of_week"),
            F.when(F.dayofweek("calendar_date").isin(1, 7), True).otherwise(False).alias("is_weekend")
        )
    )
    return df


def build_dim_plan(spark):
    """
    Builds the Insurance Plan dimension (Dim_Plan) mapping plan types
    to broader Lines of Business (LOB).
    """
    if not HAS_PYSPARK:
        return None

    data = [
        ("Medicare Advantage", "Government - Medicare", True),
        ("Medicaid", "Government - Medicaid", True),
        ("Commercial PPO", "Commercial - PPO", False),
        ("Commercial HMO", "Commercial - HMO", False),
    ]
    schema = StructType([
        StructField("insurance_plan_type", StringType(), False),
        StructField("line_of_business", StringType(), False),
        StructField("is_government_sponsored", BooleanType(), False)
    ])
    return spark.createDataFrame(data, schema)


def build_gold_star_schema(spark, silver_dir: str, bronze_dir: str, gold_dir: str):
    """
    Builds the Enterprise Healthcare Star Schema: Fact_Claims + Dimensions.
    Guarantees no cleartext PHI enters the Gold layer.
    """
    print("[GOLD] Building Healthcare Star Schema (Fact & Dimensions)...")

    # 1. Load Silver & Reference Datasets
    df_claims = _read_table(spark, os.path.join(silver_dir, "silver_claims"))
    df_patients = _read_table(spark, os.path.join(silver_dir, "silver_patients"))
    df_providers = _read_table(spark, os.path.join(bronze_dir, "bronze_providers"))
    df_icd10 = _read_table(spark, os.path.join(bronze_dir, "bronze_icd10"))

    # Filter only valid claims for the analytical data warehouse
    df_valid_claims = df_claims.filter(F.col("quality_status") == "VALID")

    # 2. Build Dim_Patient (PHI-Free Demographic Dimension)
    current_year = 2026
    dim_patient = (
        df_patients
        .withColumn("current_age_approx", F.lit(current_year) - F.col("birth_year"))
        .withColumn(
            "age_group",
            F.when(F.col("current_age_approx") < 18, "0-17")
             .when(F.col("current_age_approx") < 45, "18-44")
             .when(F.col("current_age_approx") < 65, "45-64")
             .otherwise("65+")
        )
        .select(
            "patient_key", "birth_year", "current_age_approx", "age_group",
            "gender", "state", "insurance_plan_type", "effective_date", "is_active"
        )
    )

    # 3. Build Dim_Diagnosis
    dim_diagnosis = (
        df_icd10
        .withColumnRenamed("code", "icd10_code")
        .withColumn("is_chronic", F.col("is_chronic").cast("boolean"))
    )

    # 4. Build Dim_Provider
    dim_provider = (
        df_providers
        .withColumn("in_network_flag", F.col("in_network_flag").cast("boolean"))
    )

    # 5. Build Dim_Date & Dim_Plan
    dim_date = build_dim_date(spark)
    dim_plan = build_dim_plan(spark)

    # 6. Build Fact_Claims
    # Uses broadcast joins on smaller dimension catalogs to eliminate shuffle overhead
    fact_claims = (
        df_valid_claims
        .join(F.broadcast(dim_diagnosis), df_valid_claims.primary_icd10 == dim_diagnosis.icd10_code, "left")
        .select(
            F.col("claim_id"),
            F.col("patient_key"),
            F.col("provider_npi"),
            F.col("primary_icd10").alias("diagnosis_code"),
            F.col("procedure_cpt"),
            F.col("claim_type"),
            F.col("service_date"),
            F.date_format(F.col("service_date"), "yyyyMMdd").cast("int").alias("service_date_key"),
            F.date_format(F.col("service_date"), "yyyyMM").alias("service_year_month"),
            F.col("admit_date"),
            F.col("discharge_date"),
            F.col("length_of_stay_days"),
            F.col("billed_amount"),
            F.col("allowed_amount"),
            F.col("paid_amount"),
            F.col("claim_status"),
            F.coalesce(F.col("is_chronic"), F.lit(False)).alias("is_chronic_condition")
        )
    )

    # Persist Star Schema tables into Gold
    _write_gold_table(fact_claims, os.path.join(gold_dir, "fact_claims"))
    _write_gold_table(dim_patient, os.path.join(gold_dir, "dim_patient"))
    _write_gold_table(dim_provider, os.path.join(gold_dir, "dim_provider"))
    _write_gold_table(dim_diagnosis, os.path.join(gold_dir, "dim_diagnosis"))
    if dim_date is not None:
        _write_gold_table(dim_date, os.path.join(gold_dir, "dim_date"))
    if dim_plan is not None:
        _write_gold_table(dim_plan, os.path.join(gold_dir, "dim_plan"))

    print("[GOLD] Successfully constructed Fact_Claims and Dimension tables.")
    return fact_claims, dim_patient, dim_provider, dim_diagnosis


# -----------------------------------------------------------------------------
# 2. ACTUARIAL & CLINICAL ANALYTICAL MARTS
# -----------------------------------------------------------------------------
def build_pmpm_analytics_mart(spark, gold_dir: str):
    """
    Computes Per-Member-Per-Month (PMPM) Cost Trends by Insurance Plan.
    PMPM = Total Paid Claims / Distinct Active Members in Period.
    Standard actuarial metric for Optum health plan risk management.
    """
    print("[GOLD MART] Calculating Per-Member-Per-Month (PMPM) Cost Trends...")
    fact_claims = _read_table(spark, os.path.join(gold_dir, "fact_claims"))
    dim_patient = _read_table(spark, os.path.join(gold_dir, "dim_patient"))

    joined = (
        fact_claims
        .filter(F.col("claim_status") == "PAID")
        .join(dim_patient, "patient_key", "inner")
    )

    pmpm_mart = (
        joined
        .groupBy("service_year_month", "insurance_plan_type")
        .agg(
            F.sum("paid_amount").alias("total_claims_paid"),
            F.countDistinct("patient_key").alias("active_utilizing_members"),
            F.count("claim_id").alias("total_claims_count")
        )
        .withColumn(
            "pmpm_cost",
            F.round(F.col("total_claims_paid") / F.col("active_utilizing_members"), 2)
        )
        .withColumn(
            "avg_cost_per_claim",
            F.round(F.col("total_claims_paid") / F.col("total_claims_count"), 2)
        )
        .orderBy("service_year_month", "insurance_plan_type")
    )

    _write_gold_table(pmpm_mart, os.path.join(gold_dir, "mart_pmpm_cost_analysis"))
    return pmpm_mart


def build_30day_readmission_mart(spark, gold_dir: str):
    """
    Computes 30-Day Hospital Readmission Rate for Inpatient Encounters by Facility.
    Readmission Flag: Next inpatient admission occurs within 30 days of prior discharge.
    """
    print("[GOLD MART] Calculating 30-Day Hospital Readmission Rates...")
    fact_claims = _read_table(spark, os.path.join(gold_dir, "fact_claims"))
    dim_provider = _read_table(spark, os.path.join(gold_dir, "dim_provider"))

    inpatient_claims = fact_claims.filter(F.col("claim_type") == "INPATIENT")

    # Lag prior discharge date per patient ordered by admission date
    readmit_window = Window.partitionBy("patient_key").orderBy("admit_date")
    claims_with_prior = (
        inpatient_claims
        .withColumn("prior_discharge_date", F.lag("discharge_date", 1).over(readmit_window))
        .withColumn(
            "days_since_prior_discharge",
            F.datediff(F.col("admit_date"), F.col("prior_discharge_date"))
        )
        .withColumn(
            "is_30day_readmission",
            F.when(
                (F.col("days_since_prior_discharge").isNotNull()) & 
                (F.col("days_since_prior_discharge") >= 0) & 
                (F.col("days_since_prior_discharge") <= 30),
                1
            ).otherwise(0)
        )
    )

    facility_readmit_mart = (
        claims_with_prior
        .join(dim_provider, "provider_npi", "left")
        .groupBy("facility_name")
        .agg(
            F.count("claim_id").alias("total_inpatient_admissions"),
            F.sum("is_30day_readmission").alias("total_30day_readmissions")
        )
        .withColumn(
            "readmission_rate_pct",
            F.round((F.col("total_30day_readmissions") / F.col("total_inpatient_admissions")) * 100, 2)
        )
        .orderBy(F.col("readmission_rate_pct").desc())
    )

    _write_gold_table(facility_readmit_mart, os.path.join(gold_dir, "mart_30day_readmissions"))
    return facility_readmit_mart


def build_icd10_cost_drivers_mart(spark, gold_dir: str):
    """
    Analyzes top clinical spend drivers grouped by ICD-10 diagnosis category.
    Identifies high-expenditure chronic diseases (e.g. Diabetes, Hypertension).
    """
    print("[GOLD MART] Calculating Top Clinical Cost Drivers (ICD-10)...")
    fact_claims = _read_table(spark, os.path.join(gold_dir, "fact_claims"))
    dim_diagnosis = _read_table(spark, os.path.join(gold_dir, "dim_diagnosis"))

    joined = (
        fact_claims
        .filter(F.col("claim_status") == "PAID")
        .join(dim_diagnosis, fact_claims.diagnosis_code == dim_diagnosis.icd10_code, "inner")
    )

    icd10_mart = (
        joined
        .groupBy("category", "description", "is_chronic")
        .agg(
            F.count("claim_id").alias("total_claims_count"),
            F.round(F.sum("paid_amount"), 2).alias("total_paid_spend"),
            F.round(F.avg("paid_amount"), 2).alias("avg_claim_cost")
        )
        .orderBy(F.col("total_paid_spend").desc())
    )

    _write_gold_table(icd10_mart, os.path.join(gold_dir, "mart_icd10_cost_drivers"))
    return icd10_mart


def build_provider_performance_mart(spark, gold_dir: str):
    """
    Evaluates provider and facility claims volume, paid totals, and denial rates.
    Helps network managers identify outlier denial rates or billing patterns.
    """
    print("[GOLD MART] Calculating Provider & Facility Performance...")
    fact_claims = _read_table(spark, os.path.join(gold_dir, "fact_claims"))
    dim_provider = _read_table(spark, os.path.join(gold_dir, "dim_provider"))

    provider_mart = (
        fact_claims
        .join(dim_provider, "provider_npi", "left")
        .groupBy("provider_npi", "provider_name", "specialty", "facility_name")
        .agg(
            F.count("claim_id").alias("total_claims_submitted"),
            F.round(F.sum("billed_amount"), 2).alias("total_billed"),
            F.round(F.sum("paid_amount"), 2).alias("total_paid"),
            F.sum(F.when(F.col("claim_status") == "DENIED", 1).otherwise(0)).alias("denied_claims_count")
        )
        .withColumn(
            "denial_rate_pct",
            F.round((F.col("denied_claims_count") / F.col("total_claims_submitted")) * 100, 2)
        )
        .orderBy(F.col("total_claims_submitted").desc())
    )

    _write_gold_table(provider_mart, os.path.join(gold_dir, "mart_provider_performance"))
    return provider_mart


# -----------------------------------------------------------------------------
# 3. HELPER FUNCTIONS
# -----------------------------------------------------------------------------
def _write_gold_table(df, path: str):
    try:
        df.write.format("delta").mode("overwrite").save(path)
    except Exception:
        df.write.format("parquet").mode("overwrite").save(path)


def _read_table(spark, path: str):
    try:
        return spark.read.format("delta").load(path)
    except Exception:
        return spark.read.format("parquet").load(path)
