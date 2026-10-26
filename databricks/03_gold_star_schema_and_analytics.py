# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Gold Layer Analytics & Star Schema
# MAGIC 
# MAGIC **Healthcare Data Engineering & Claims Pipeline**
# MAGIC 
# MAGIC ### Engineering Objectives:
# MAGIC 1. **Dimensional Modeling**: Build the enterprise Star Schema (Fact_Claims + Dimensions).
# MAGIC 2. **Performance Optimization**: Liquid Clustering / Z-Ordering on `(patient_key, service_date)` and Broadcast Joins.
# MAGIC 3. **Actuarial Analytics**: Per-Member-Per-Month (PMPM) cost calculations across insurance lines of business.
# MAGIC 4. **Clinical Analytics**: 30-Day Hospital Readmission rate tracking by medical facility.
# MAGIC 
# MAGIC *Note: Uses synthetic healthcare data for educational/portfolio demonstration.*

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

BRONZE_PATH = "/mnt/healthcare-lake/bronze"
SILVER_PATH = "/mnt/healthcare-lake/silver"
GOLD_PATH = "/mnt/healthcare-lake/gold"

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Construct Star Schema Dimensions & Fact Table
# MAGIC Broadcasts dimension catalogs (`dim_diagnosis`, `dim_provider`) to avoid costly cluster shuffles during fact table assembly.

# COMMAND ----------

df_claims = spark.read.format("delta").load(f"{SILVER_PATH}/silver_claims").filter("quality_status = 'VALID'")
df_patients = spark.read.format("delta").load(f"{SILVER_PATH}/silver_patients")
df_providers = spark.read.format("delta").load(f"{BRONZE_PATH}/bronze_providers")
df_icd10 = spark.read.format("delta").load(f"{BRONZE_PATH}/bronze_icd10")

# Dim_Patient
dim_patient = (
    df_patients
    .withColumn("current_age_approx", F.lit(2026) - F.col("birth_year"))
    .withColumn(
        "age_group",
        F.when(F.col("current_age_approx") < 18, "0-17")
         .when(F.col("current_age_approx") < 45, "18-44")
         .when(F.col("current_age_approx") < 65, "45-64")
         .otherwise("65+")
    )
)
dim_patient.write.format("delta").mode("overwrite").save(f"{GOLD_PATH}/dim_patient")

# Dim_Diagnosis
dim_diagnosis = df_icd10.withColumnRenamed("code", "icd10_code").withColumn("is_chronic", F.col("is_chronic").cast("boolean"))
dim_diagnosis.write.format("delta").mode("overwrite").save(f"{GOLD_PATH}/dim_diagnosis")

# Dim_Provider
df_providers.write.format("delta").mode("overwrite").save(f"{GOLD_PATH}/dim_provider")

# Fact_Claims
fact_claims = (
    df_claims
    .join(F.broadcast(dim_diagnosis), df_claims.primary_icd10 == dim_diagnosis.icd10_code, "left")
    .select(
        F.col("claim_id"),
        F.col("patient_key"),
        F.col("provider_npi"),
        F.col("primary_icd10").alias("diagnosis_code"),
        F.col("procedure_cpt"),
        F.col("claim_type"),
        F.col("service_date"),
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

(
    fact_claims.write
    .format("delta")
    .mode("overwrite")
    .save(f"{GOLD_PATH}/fact_claims")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Liquid Clustering / Z-Ordering (Databricks Optimization)
# MAGIC In Databricks, clustering by `(patient_key, service_date)` colocates claims for the same patient in adjacent Parquet files, allowing Delta data skipping to prune 90%+ of files during member timeline queries.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Optimize file layout via Z-Ordering (or Databricks Liquid Clustering)
# MAGIC OPTIMIZE delta.`/mnt/healthcare-lake/gold/fact_claims`
# MAGIC ZORDER BY (patient_key, service_date);

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Analytical Mart: Per-Member-Per-Month (PMPM) Cost Trends
# MAGIC PMPM is the foundational metric for health plan risk pricing and medical loss ratio (MLR) analysis.

# COMMAND ----------

pmpm_mart = (
    fact_claims
    .filter(F.col("claim_status") == "PAID")
    .join(dim_patient, "patient_key", "inner")
    .groupBy("service_year_month", "insurance_plan_type")
    .agg(
        F.sum("paid_amount").alias("total_claims_paid"),
        F.countDistinct("patient_key").alias("active_utilizing_members"),
        F.count("claim_id").alias("total_claims_count")
    )
    .withColumn("pmpm_cost", F.round(F.col("total_claims_paid") / F.col("active_utilizing_members"), 2))
    .withColumn("avg_cost_per_claim", F.round(F.col("total_claims_paid") / F.col("total_claims_count"), 2))
    .orderBy("service_year_month", "insurance_plan_type")
)

pmpm_mart.write.format("delta").mode("overwrite").save(f"{GOLD_PATH}/mart_pmpm_cost_analysis")
display(pmpm_mart)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Clinical Quality Mart: 30-Day Hospital Readmissions
# MAGIC Uses Window functions (`lag` on discharge date) to identify inpatient hospital readmissions occurring within 30 days.

# COMMAND ----------

inpatient_claims = fact_claims.filter(F.col("claim_type") == "INPATIENT")
readmit_window = Window.partitionBy("patient_key").orderBy("admit_date")

readmit_mart = (
    inpatient_claims
    .withColumn("prior_discharge_date", F.lag("discharge_date", 1).over(readmit_window))
    .withColumn("days_since_prior_discharge", F.datediff(F.col("admit_date"), F.col("prior_discharge_date")))
    .withColumn(
        "is_30day_readmission",
        F.when(
            (F.col("days_since_prior_discharge").isNotNull()) &
            (F.col("days_since_prior_discharge") >= 0) &
            (F.col("days_since_prior_discharge") <= 30), 1
        ).otherwise(0)
    )
    .join(df_providers, "provider_npi", "left")
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

readmit_mart.write.format("delta").mode("overwrite").save(f"{GOLD_PATH}/mart_30day_readmissions")
display(readmit_mart)
