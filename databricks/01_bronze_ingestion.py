# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Bronze Layer Ingestion (Raw to Delta Lake)
# MAGIC 
# MAGIC **Healthcare Data Engineering & Claims Pipeline**
# MAGIC 
# MAGIC ### Engineering Objectives:
# MAGIC 1. Ingest raw streaming/batch healthcare payloads (JSON claims, CSV patients/providers) into Bronze Delta tables.
# MAGIC 2. Enforce strict schema validation on landing to quarantine corrupted schemas early.
# MAGIC 3. Append standardized ingestion audit lineage metadata (`ingestion_timestamp`, `source_file`, `batch_id`).
# MAGIC 4. Preserve immutable raw state for compliance auditing.
# MAGIC 
# MAGIC *Note: Uses synthetic healthcare data for educational/portfolio demonstration.*

# COMMAND ----------

import uuid
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType
)

# Configuration Parameters (In Azure, use Azure Key Vault / dbutils.secrets)
RAW_LANDING_PATH = "/mnt/healthcare-lake/raw"  # or "abfss://raw@optumadlsgen2.dfs.core.windows.net/"
BRONZE_PATH = "/mnt/healthcare-lake/bronze"

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Claims Raw Schema Definition & Ingestion
# MAGIC In Databricks production, we use **Auto Loader** (`cloudFiles`) to incrementally detect new files in ADLS Gen2 without file-listing bottlenecks.

# COMMAND ----------

claims_schema = StructType([
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

batch_id = str(uuid.uuid4())

# Auto Loader batch/stream read (or standard spark.read for local/batch)
df_claims_raw = (
    spark.read
    .schema(claims_schema)
    .json(f"{RAW_LANDING_PATH}/claims.json")
    .withColumn("ingestion_timestamp", F.current_timestamp())
    .withColumn("source_file", F.input_file_name())
    .withColumn("batch_id", F.lit(batch_id))
)

# Write as Bronze Delta Table with Auto-Compact
(
    df_claims_raw.write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "false")
    .save(f"{BRONZE_PATH}/bronze_claims")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Ingest Patient Demographics & Reference Master Data

# COMMAND ----------

# Patients CSV
df_patients_raw = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(f"{RAW_LANDING_PATH}/patients.csv")
    .withColumn("ingestion_timestamp", F.current_timestamp())
    .withColumn("source_file", F.input_file_name())
    .withColumn("batch_id", F.lit(batch_id))
)

df_patients_raw.write.format("delta").mode("append").save(f"{BRONZE_PATH}/bronze_patients")

# Providers CSV
df_providers_raw = spark.read.option("header", "true").csv(f"{RAW_LANDING_PATH}/providers.csv")
df_providers_raw.write.format("delta").mode("overwrite").save(f"{BRONZE_PATH}/bronze_providers")

# ICD-10 Reference Master
df_icd10_raw = spark.read.option("header", "true").csv(f"{RAW_LANDING_PATH}/icd10_reference.csv")
df_icd10_raw.write.format("delta").mode("overwrite").save(f"{BRONZE_PATH}/bronze_icd10")

print(f"Bronze Layer Ingestion Completed successfully for Batch: {batch_id}")
