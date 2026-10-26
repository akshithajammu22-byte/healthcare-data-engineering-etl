"""
Silver Layer Transformation Engine
Transforms raw Bronze healthcare payloads into validated, de-identified, and cleansed Silver tables:
1. De-identifies synthetic PHI using deterministic salted SHA-256 tokenization and generalization.
2. Validates clinical and financial rules (date order sanity, non-negative amounts, ICD-10 regex).
3. Handles duplicates using business key `claim_id` and latest `submission_timestamp`.
4. Implements Delta Lake `DeltaTable.forPath(...).merge(...)` for late-arriving claim adjustments.

NOTE: This is an educational portfolio project using 100% synthetic data.
It demonstrates de-identification patterns and does NOT claim actual regulatory HIPAA compliance.
"""

import os
from datetime import datetime

try:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window
    HAS_PYSPARK = True
except ImportError:
    HAS_PYSPARK = False

try:
    from delta.tables import DeltaTable
    HAS_DELTA = True
except ImportError:
    HAS_DELTA = False

from src.security import (
    mask_phi_column,
    mask_birth_date,
    mask_phone_or_ssn_last4,
    hash_phi_value,
    mask_ssn_value
)


def transform_silver_patients(spark, bronze_dir: str, silver_dir: str, phi_salt: str):
    """
    Cleanses synthetic patient demographics and applies PHI de-identification.
    Generates a deterministic surrogate key `patient_key` for relational linking.
    """
    input_path = os.path.join(bronze_dir, "bronze_patients")
    output_path = os.path.join(silver_dir, "silver_patients")

    print(f"[SILVER] Processing patient demographics from {input_path}...")
    try:
        df_bronze = spark.read.format("delta").load(input_path)
    except Exception:
        df_bronze = spark.read.format("parquet").load(input_path)

    # Apply de-identification & surrogate key generation
    df_silver = (
        df_bronze
        .withColumn("patient_key", mask_phi_column("mrn", phi_salt))
        .withColumn("birth_year", mask_birth_date("date_of_birth"))
        .withColumn("masked_ssn", mask_phone_or_ssn_last4("ssn"))
        .withColumn("gender", F.upper(F.trim(F.col("gender"))))
        .withColumn("state", F.upper(F.trim(F.col("state"))))
        .withColumn("effective_date", F.to_date(F.col("effective_date")))
        .withColumn("_silver_processed_at", F.current_timestamp())
        # Drop synthetic cleartext PHI identifiers to prevent leakage
        .drop("mrn", "ssn", "first_name", "last_name", "address", "date_of_birth")
    )

    try:
        df_silver.write.format("delta").mode("overwrite").save(output_path)
        print(f"[SILVER] Successfully wrote silver_patients Delta table at {output_path}")
    except Exception:
        df_silver.write.format("parquet").mode("overwrite").save(output_path)

    return df_silver


def transform_silver_claims(spark, bronze_dir: str, silver_dir: str, phi_salt: str, icd10_regex: str):
    """
    Cleanses medical claims, validates clinical dates and financial fields,
    and deduplicates claims using Window functions or Delta MERGE.
    """
    input_path = os.path.join(bronze_dir, "bronze_claims")
    output_path = os.path.join(silver_dir, "silver_claims")

    print(f"[SILVER] Processing medical claims from {input_path}...")
    try:
        df_bronze = spark.read.format("delta").load(input_path)
    except Exception:
        df_bronze = spark.read.format("parquet").load(input_path)

    # 1. Tokenize synthetic patient identifier using identical salt to match silver_patients
    df_claims = (
        df_bronze
        .withColumn("patient_key", mask_phi_column("patient_mrn", phi_salt))
        .withColumn("service_date", F.to_date(F.col("service_date")))
        .withColumn("admit_date", F.to_date(F.col("admit_date")))
        .withColumn("discharge_date", F.to_date(F.col("discharge_date")))
        .withColumn("submission_timestamp", F.to_timestamp(F.col("submission_timestamp")))
        .withColumn("billed_amount", F.round(F.col("billed_amount"), 2))
        .withColumn("allowed_amount", F.round(F.col("allowed_amount"), 2))
        .withColumn("paid_amount", F.round(F.col("paid_amount"), 2))
        .drop("patient_mrn")
    )

    # 2. Healthcare Business Quality Checks & Quarantine Status
    df_validated = (
        df_claims
        .withColumn(
            "is_valid_dates",
            (F.col("discharge_date") >= F.col("admit_date")) & (F.col("admit_date") >= F.col("service_date"))
        )
        .withColumn(
            "is_valid_financials",
            (F.col("paid_amount") <= F.col("allowed_amount")) & 
            (F.col("allowed_amount") <= F.col("billed_amount")) &
            (F.col("billed_amount") >= 0.0)
        )
        .withColumn(
            "is_valid_icd10",
            F.col("primary_icd10").rlike(icd10_regex)
        )
        .withColumn(
            "quality_status",
            F.when(
                F.col("is_valid_dates") & F.col("is_valid_financials") & F.col("is_valid_icd10"),
                F.lit("VALID")
            ).otherwise(F.lit("QUARANTINED"))
        )
        .withColumn(
            "length_of_stay_days",
            F.datediff(F.col("discharge_date"), F.col("admit_date"))
        )
    )

    # 3. Deduplication: In case of claim resubmission/duplicate in the batch,
    # retain the record with the latest submission timestamp
    dedup_window = Window.partitionBy("claim_id").orderBy(F.col("submission_timestamp").desc())
    df_deduped = (
        df_validated
        .withColumn("row_num", F.row_number().over(dedup_window))
        .filter(F.col("row_num") == 1)
        .drop("row_num")
        .withColumn("_silver_processed_at", F.current_timestamp())
    )

    # 4. Write or Merge into Silver Claims
    if HAS_DELTA and os.path.exists(output_path):
        try:
            merge_silver_claims_delta(spark, output_path, df_deduped)
            return df_deduped
        except Exception as e:
            print(f"[SILVER] Delta merge fallback to overwrite: {e}")

    try:
        df_deduped.write.format("delta").mode("overwrite").save(output_path)
        print(f"[SILVER] Successfully wrote silver_claims Delta table at {output_path}")
    except Exception:
        df_deduped.write.format("parquet").mode("overwrite").save(output_path)

    return df_deduped


def merge_silver_claims_delta(spark, silver_claims_path: str, incoming_claims_df):
    """
    Demonstrates native PySpark DeltaTable.merge() upsert logic.
    
    Business Logic:
    - Match key: `target.claim_id = source.claim_id`
    - Late-arriving claim update condition: `source.submission_timestamp > target.submission_timestamp`
      When a claim adjustment arrives, the target record is updated in-place without creating a duplicate.
    - When not matched: Inserts the new claim record.
    """
    if not HAS_DELTA:
        print("[SILVER] Delta Lake is not available; skipping DeltaTable.merge().")
        return

    print(f"[SILVER] Executing DeltaTable.merge() on {silver_claims_path}...")
    target_table = DeltaTable.forPath(spark, silver_claims_path)

    (
        target_table.alias("target")
        .merge(
            incoming_claims_df.alias("source"),
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
    print("[SILVER] DeltaTable.merge() completed successfully.")


# -----------------------------------------------------------------------------
# Standalone Reference Implementation (Python/Dictionary)
# Demonstrates late-arriving claim adjustment & deduplication for testing & local demo
# -----------------------------------------------------------------------------
def apply_claims_merge(existing_records: dict, incoming_records: list[dict]) -> tuple[dict, int, int]:
    """
    Pure Python reference implementation of Delta MERGE business logic.
    Takes existing claims dict {claim_id: record} and merges incoming claims list.
    
    Returns:
      (updated_records_dict, inserted_count, updated_count)
    """
    inserted = 0
    updated = 0

    for inc in incoming_records:
        cid = inc["claim_id"]
        if cid not in existing_records:
            existing_records[cid] = inc.copy()
            inserted += 1
        else:
            # Check submission timestamp for late-arriving adjustment
            existing_ts = existing_records[cid].get("submission_timestamp", "")
            incoming_ts = inc.get("submission_timestamp", "")

            if incoming_ts > existing_ts:
                existing_records[cid].update(inc)
                updated += 1
            # If incoming_ts <= existing_ts, ignore (out-of-order duplicate)

    return existing_records, inserted, updated
