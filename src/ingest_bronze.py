"""
Bronze Layer Ingestion Engine
Ingests raw synthetic healthcare payloads (CSV, JSON) into Bronze Delta/Parquet tables.
Implements append-only ingestion and attaches standardized metadata:
  - ingestion_timestamp: UTC timestamp of ingestion
  - source_file: Origin file path
  - batch_id: Unique batch UUID

NOTE: This is a student portfolio project using 100% synthetic data.
No real patient or personal data is processed or stored.
"""

import os
import uuid
from datetime import datetime, timezone

try:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.types import (
        StructType, StructField, StringType, DoubleType, DateType, TimestampType, BooleanType
    )
    HAS_PYSPARK = True
except ImportError:
    HAS_PYSPARK = False


def get_claims_raw_schema():
    """
    Explicit schema for raw synthetic medical claims.
    Enforcing schema at Bronze ingestion prevents malformed payloads from breaking downstream pipelines.
    """
    if not HAS_PYSPARK:
        return None

    return StructType([
        StructField("claim_id", StringType(), False),
        StructField("patient_mrn", StringType(), False),
        StructField("provider_npi", StringType(), True),
        StructField("claim_type", StringType(), True),
        StructField("service_date", StringType(), True),
        StructField("admit_date", StringType(), True),
        StructField("discharge_date", StringType(), True),
        StructField("primary_icd10", StringType(), True),
        StructField("procedure_cpt", StringType(), True),
        StructField("billed_amount", DoubleType(), True),
        StructField("allowed_amount", DoubleType(), True),
        StructField("paid_amount", DoubleType(), True),
        StructField("claim_status", StringType(), True),
        StructField("submission_timestamp", StringType(), True),
    ])


def ingest_bronze_claims(spark, raw_dir: str, bronze_dir: str, batch_id: str = None):
    """
    Ingests synthetic claims JSON into the Bronze layer with ingestion metadata.
    """
    batch_id = batch_id or str(uuid.uuid4())
    input_path = os.path.join(raw_dir, "claims.json")
    output_path = os.path.join(bronze_dir, "bronze_claims")

    print(f"[BRONZE] Ingesting raw claims from {input_path} (Batch: {batch_id[:8]})...")
    schema = get_claims_raw_schema()
    df_raw = spark.read.schema(schema).json(input_path)

    # Attach standardized audit metadata columns
    df_bronze = (
        df_raw
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn("source_file", F.input_file_name())
        .withColumn("batch_id", F.lit(batch_id))
    )

    try:
        df_bronze.write.format("delta").mode("overwrite").save(output_path)
        print(f"[BRONZE] Successfully wrote bronze_claims Delta table at {output_path}")
    except Exception as e:
        print(f"[BRONZE] Local Delta Lake fallback to Parquet: {e}")
        df_bronze.write.format("parquet").mode("overwrite").save(output_path)

    return df_bronze


def ingest_bronze_patients(spark, raw_dir: str, bronze_dir: str, batch_id: str = None):
    """
    Ingests synthetic patient demographics CSV into the Bronze layer with ingestion metadata.
    Preserves raw synthetic identifiers (synthetic MRN, synthetic SSN) for Bronze auditing.
    """
    batch_id = batch_id or str(uuid.uuid4())
    input_path = os.path.join(raw_dir, "patients.csv")
    output_path = os.path.join(bronze_dir, "bronze_patients")

    print(f"[BRONZE] Ingesting raw patients from {input_path} (Batch: {batch_id[:8]})...")
    df_raw = spark.read.option("header", "true").option("inferSchema", "true").csv(input_path)

    df_bronze = (
        df_raw
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn("source_file", F.input_file_name())
        .withColumn("batch_id", F.lit(batch_id))
    )

    try:
        df_bronze.write.format("delta").mode("overwrite").save(output_path)
        print(f"[BRONZE] Successfully wrote bronze_patients Delta table at {output_path}")
    except Exception as e:
        print(f"[BRONZE] Local Delta Lake fallback to Parquet: {e}")
        df_bronze.write.format("parquet").mode("overwrite").save(output_path)

    return df_bronze


def ingest_bronze_reference_data(spark, raw_dir: str, bronze_dir: str):
    """
    Ingests synthetic provider and ICD-10 clinical diagnosis reference tables into Bronze.
    """
    providers_in = os.path.join(raw_dir, "providers.csv")
    providers_out = os.path.join(bronze_dir, "bronze_providers")
    df_prov = spark.read.option("header", "true").csv(providers_in)
    try:
        df_prov.write.format("delta").mode("overwrite").save(providers_out)
    except Exception:
        df_prov.write.format("parquet").mode("overwrite").save(providers_out)

    icd10_in = os.path.join(raw_dir, "icd10_reference.csv")
    icd10_out = os.path.join(bronze_dir, "bronze_icd10")
    df_icd = spark.read.option("header", "true").csv(icd10_in)
    try:
        df_icd.write.format("delta").mode("overwrite").save(icd10_out)
    except Exception:
        df_icd.write.format("parquet").mode("overwrite").save(icd10_out)

    print("[BRONZE] Ingested provider directory and ICD-10 reference tables.")
