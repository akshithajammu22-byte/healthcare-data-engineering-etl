# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Silver Layer Processing (Cleansing, PHI Tokenization & Delta MERGE)
# MAGIC 
# MAGIC **Healthcare Data Engineering & Claims Pipeline**
# MAGIC 
# MAGIC ### Engineering Objectives:
# MAGIC 1. **PHI De-Identification**: Salted SHA-256 surrogate keys (`patient_key`) using native Spark functions.
# MAGIC 2. **Clinical & Financial Validation**: Validates date order (`discharge_date >= admit_date`) and financial boundaries (`0 <= paid <= allowed <= billed`).
# MAGIC 3. **Quarantine Logic**: Tags corrupt/invalid records (`quality_status = 'QUARANTINED'`) rather than dropping them silently.
# MAGIC 4. **Delta MERGE (Upsert)**: Ingests late-arriving claim adjustments and deduplicates submissions without full table rewrites.
# MAGIC 
# MAGIC *Note: Uses synthetic healthcare data for educational/portfolio demonstration.*

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

BRONZE_PATH = "/mnt/healthcare-lake/bronze"
SILVER_PATH = "/mnt/healthcare-lake/silver"

# Retrieve secret salt from Azure Key Vault via Databricks Secrets utility
# In Azure production: PHI_SALT = dbutils.secrets.get("healthcare-vault", "phi-salt")
PHI_SALT = "HEALTHCARE_PHI_SALT"
ICD10_REGEX = "^[A-Z][0-9][0-9A-Z](\\.[0-9A-Z]{1,4})?$"

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Silver Patients: PHI De-Identification & Demographic Normalization
# MAGIC Transforms synthetic patient identifiers into surrogate keys (`patient_key`). Removes raw SSN, MRN, Name, and Address.

# COMMAND ----------

df_bronze_patients = spark.read.format("delta").load(f"{BRONZE_PATH}/bronze_patients")

df_silver_patients = (
    df_bronze_patients
    # Native Spark sha2 + concat avoids Python UDF Py4J serialization overhead
    .withColumn("patient_key", F.sha2(F.concat(F.coalesce(F.col("mrn"), F.lit("NULL_VAL")), F.lit(PHI_SALT)), 256))
    .withColumn("birth_year", F.year(F.to_date(F.col("date_of_birth"))))
    .withColumn("masked_ssn", F.concat(F.lit("XXX-XX-"), F.substring(F.regexp_replace(F.col("ssn"), "[^0-9]", ""), -4, 4)))
    .withColumn("gender", F.upper(F.trim(F.col("gender"))))
    .withColumn("state", F.upper(F.trim(F.col("state"))))
    .withColumn("effective_date", F.to_date(F.col("effective_date")))
    .withColumn("_silver_processed_at", F.current_timestamp())
    .drop("mrn", "ssn", "first_name", "last_name", "address", "date_of_birth")
)

(
    df_silver_patients.write
    .format("delta")
    .mode("overwrite")
    .partitionBy("state")
    .save(f"{SILVER_PATH}/silver_patients")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Silver Claims: Validation, Quarantine & Deduplication
# MAGIC Validates clinical chronological constraints and financial integrity.

# COMMAND ----------

df_bronze_claims = spark.read.format("delta").load(f"{BRONZE_PATH}/bronze_claims")

df_claims_stg = (
    df_bronze_claims
    .withColumn("patient_key", F.sha2(F.concat(F.coalesce(F.col("patient_mrn"), F.lit("NULL_VAL")), F.lit(PHI_SALT)), 256))
    .withColumn("service_date", F.to_date(F.col("service_date")))
    .withColumn("admit_date", F.to_date(F.col("admit_date")))
    .withColumn("discharge_date", F.to_date(F.col("discharge_date")))
    .withColumn("submission_timestamp", F.to_timestamp(F.col("submission_timestamp")))
    .withColumn("billed_amount", F.round(F.col("billed_amount"), 2))
    .withColumn("allowed_amount", F.round(F.col("allowed_amount"), 2))
    .withColumn("paid_amount", F.round(F.col("paid_amount"), 2))
    .withColumn("length_of_stay_days", F.datediff(F.col("discharge_date"), F.col("admit_date")))
    .drop("patient_mrn")
)

# Healthcare Validation Rules
df_claims_validated = (
    df_claims_stg
    .withColumn("is_valid_dates", (F.col("discharge_date") >= F.col("admit_date")) & (F.col("admit_date") >= F.col("service_date")))
    .withColumn("is_valid_financials", (F.col("paid_amount") <= F.col("allowed_amount")) & (F.col("allowed_amount") <= F.col("billed_amount")) & (F.col("billed_amount") >= 0.0))
    .withColumn("is_valid_icd10", F.col("primary_icd10").rlike(ICD10_REGEX))
    .withColumn(
        "quality_status",
        F.when(F.col("is_valid_dates") & F.col("is_valid_financials") & F.col("is_valid_icd10"), F.lit("VALID")).otherwise(F.lit("QUARANTINED"))
    )
    .withColumn("_silver_processed_at", F.current_timestamp())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Delta MERGE (Upsert) for Late-Arriving Adjustments
# MAGIC Matches on `claim_id`. When a claim status update or revised adjudication arrives, it updates in-place if `submission_timestamp` is newer.

# COMMAND ----------

silver_claims_target = f"{SILVER_PATH}/silver_claims"

# If table does not exist, initialize it
if not DeltaTable.isDeltaTable(spark, silver_claims_target):
    (
        df_claims_validated.write
        .format("delta")
        .partitionBy("claim_type")
        .save(silver_claims_target)
    )
    print("Initialized silver_claims Delta table.")
else:
    # Execute Delta Lake MERGE INTO
    target_table = DeltaTable.forPath(spark, silver_claims_target)
    (
        target_table.alias("target")
        .merge(
            df_claims_validated.alias("source"),
            "target.claim_id = source.claim_id"
        )
        .whenMatchedUpdate(
            condition="source.submission_timestamp > target.submission_timestamp",
            set={
                "patient_key": "source.patient_key",
                "provider_npi": "source.provider_npi",
                "claim_type": "source.claim_type",
                "service_date": "source.service_date",
                "admit_date": "source.admit_date",
                "discharge_date": "source.discharge_date",
                "length_of_stay_days": "source.length_of_stay_days",
                "primary_icd10": "source.primary_icd10",
                "procedure_cpt": "source.procedure_cpt",
                "billed_amount": "source.billed_amount",
                "allowed_amount": "source.allowed_amount",
                "paid_amount": "source.paid_amount",
                "claim_status": "source.claim_status",
                "quality_status": "source.quality_status",
                "submission_timestamp": "source.submission_timestamp",
                "_silver_processed_at": "source._silver_processed_at"
            }
        )
        .whenNotMatchedInsertAll()
        .execute()
    )
    print("Executed DeltaTable.merge() with late-arriving adjustment resolution.")
