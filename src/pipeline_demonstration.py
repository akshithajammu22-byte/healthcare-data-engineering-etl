"""
End-to-End Healthcare Pipeline Demonstration (PySpark & Delta Lake)
Orchestrates Raw -> Bronze -> Silver -> Gold Medallion Pipeline,
runs Data Quality suite, and displays sample analytical results.

NOTE: This is a student portfolio project using 100% synthetic data.
No real patient or personal data is processed or stored.
"""

import sys
import os

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

import yaml
from src.spark_session import get_spark_session
from src.generate_healthcare_data import generate_and_save_dataset
from src.ingest_bronze import (
    ingest_bronze_claims, ingest_bronze_patients, ingest_bronze_reference_data
)
from src.transform_silver import (
    transform_silver_patients, transform_silver_claims
)
from src.aggregate_gold import (
    build_gold_star_schema,
    build_pmpm_analytics_mart,
    build_30day_readmission_mart,
    build_icd10_cost_drivers_mart,
    build_provider_performance_mart
)
from src.data_quality_checks import HealthcareDataQualitySuite


def run_pipeline():
    print("=" * 70)
    print("  HEALTHCARE DATA ENGINEERING & ETL PIPELINE (PYSPARK DEMO)  ")
    print("  Target Role: Data Engineer (Optum / Healthcare Analytics)  ")
    print("=" * 70)

    # 1. Load Configuration
    config_path = os.path.join(PROJECT_ROOT, "pipeline_config.yaml")
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    raw_dir = os.path.join(PROJECT_ROOT, config["paths"]["raw_landing"])
    bronze_dir = os.path.join(PROJECT_ROOT, config["paths"]["bronze_layer"])
    silver_dir = os.path.join(PROJECT_ROOT, config["paths"]["silver_layer"])
    gold_dir = os.path.join(PROJECT_ROOT, config["paths"]["gold_layer"])
    phi_salt = config["security"]["phi_salt"]
    icd10_regex = config["data_quality"]["enforce_icd10_regex"]

    # 2. Step 1: Generate Synthetic Healthcare Data
    print("\n>>> STEP 1: Generating Synthetic Healthcare Datasets...")
    generate_and_save_dataset(raw_dir)

    # 3. Step 2: Initialize PySpark Session
    print("\n>>> STEP 2: Initializing PySpark Session...")
    spark = get_spark_session(app_name="Optum-Healthcare-Pipeline")

    # 4. Step 3: Bronze Layer Ingestion
    print("\n>>> STEP 3: Ingesting Raw Data into Bronze Tables (Audit Lineage)...")
    ingest_bronze_claims(spark, raw_dir, bronze_dir)
    ingest_bronze_patients(spark, raw_dir, bronze_dir)
    ingest_bronze_reference_data(spark, raw_dir, bronze_dir)

    # 5. Step 4: Silver Layer Processing & De-identification
    print("\n>>> STEP 4: Processing Silver Layer (Cleansing & PHI De-identification)...")
    df_silver_patients = transform_silver_patients(spark, bronze_dir, silver_dir, phi_salt)
    df_silver_claims = transform_silver_claims(spark, bronze_dir, silver_dir, phi_salt, icd10_regex)

    # 6. Step 5: Gold Layer Star Schema & Analytical Marts
    print("\n>>> STEP 5: Building Gold Star Schema & Actuarial Marts...")
    fact_claims, dim_patient, dim_provider, dim_diagnosis = build_gold_star_schema(spark, silver_dir, bronze_dir, gold_dir)
    pmpm_mart = build_pmpm_analytics_mart(spark, gold_dir)
    readmit_mart = build_30day_readmission_mart(spark, gold_dir)
    icd10_mart = build_icd10_cost_drivers_mart(spark, gold_dir)
    provider_mart = build_provider_performance_mart(spark, gold_dir)

    # 7. Step 6: Automated Data Quality Suite
    print("\n>>> STEP 6: Executing Healthcare Data Quality Suite...")
    dq_suite = HealthcareDataQualitySuite()
    
    dq_suite.check_uniqueness(df_silver_claims, "claim_id", "silver_claims")
    dq_suite.check_non_null(df_silver_claims, ["claim_id", "patient_key", "service_date"], "silver_claims")
    valid_claims = df_silver_claims.filter(df_silver_claims.quality_status == "VALID")
    dq_suite.check_financial_integrity(valid_claims, "silver_claims")
    dq_suite.check_phi_zero_leakage(
        df_silver_patients, 
        ["mrn", "ssn", "first_name", "last_name", "address", "date_of_birth"], 
        "silver_patients"
    )

    report = dq_suite.generate_report()
    print(report)

    # 8. Step 7: Display Analytical Sample Results
    print("\n>>> SAMPLE ANALYTICAL RESULTS: Per-Member-Per-Month (PMPM) Cost Trends")
    pmpm_mart.show(5, truncate=False)

    print("\n>>> SAMPLE ANALYTICAL RESULTS: Top Clinical Cost Drivers (ICD-10)")
    icd10_mart.show(5, truncate=False)

    print("\n>>> SAMPLE ANALYTICAL RESULTS: 30-Day Hospital Readmissions by Facility")
    readmit_mart.show(5, truncate=False)

    print("PySpark Pipeline Demonstration Completed Successfully!")


if __name__ == "__main__":
    run_pipeline()
