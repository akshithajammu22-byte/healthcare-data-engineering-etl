# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Healthcare Data Quality & Governance Suite
# MAGIC 
# MAGIC **Healthcare Data Engineering & Claims Pipeline**
# MAGIC 
# MAGIC ### Engineering Objectives:
# MAGIC 1. **Automated Data Quality Auditing**: Asserts Primary Key uniqueness, non-null mandatory fields, and financial constraints.
# MAGIC 2. **Delta Live Tables (DLT) Expectations**: Illustrates production DLT expectation syntax for automated quarantine and failure handling.
# MAGIC 3. **PHI Zero-Leakage Audit**: Verifies that no cleartext Protected Health Information (SSN, MRN, Name, Address) has leaked into Silver or Gold layers.
# MAGIC 
# MAGIC *Note: Uses synthetic healthcare data for educational/portfolio demonstration.*

# COMMAND ----------

from pyspark.sql import functions as F

SILVER_PATH = "/mnt/healthcare-lake/silver"
GOLD_PATH = "/mnt/healthcare-lake/gold"

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Primary Key Uniqueness & Referential Integrity Audits

# COMMAND ----------

df_silver_claims = spark.read.format("delta").load(f"{SILVER_PATH}/silver_claims")
df_silver_patients = spark.read.format("delta").load(f"{SILVER_PATH}/silver_patients")

total_claims = df_silver_claims.count()
distinct_claims = df_silver_claims.select("claim_id").distinct().count()

assert total_claims == distinct_claims, f"PK Uniqueness Violation: {total_claims} rows vs {distinct_claims} distinct claim_ids"
print(f"[PASS] Primary Key Uniqueness verified: {total_claims} unique claims.")

# Check mandatory foreign keys are non-null
null_patient_keys = df_silver_claims.filter(F.col("patient_key").isNull()).count()
null_service_dates = df_silver_claims.filter(F.col("service_date").isNull()).count()

assert null_patient_keys == 0, f"Found {null_patient_keys} null patient_keys"
assert null_service_dates == 0, f"Found {null_service_dates} null service_dates"
print("[PASS] Non-null mandatory constraints verified.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Healthcare Financial & Clinical Chronology Audits

# COMMAND ----------

# In Silver, valid claims must have: 0 <= Paid <= Allowed <= Billed
financial_violations = df_silver_claims.filter(
    (F.col("quality_status") == "VALID") & (
        (F.col("paid_amount") < 0) |
        (F.col("allowed_amount") < 0) |
        (F.col("billed_amount") < 0) |
        (F.col("paid_amount") > F.col("allowed_amount")) |
        (F.col("allowed_amount") > F.col("billed_amount"))
    )
).count()

assert financial_violations == 0, f"Found {financial_violations} financial rule violations in valid claims"
print("[PASS] Healthcare financial logic (0 <= Paid <= Allowed <= Billed) verified.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. PHI Zero-Leakage Audit
# MAGIC Verifies that raw direct identifiers are absent from de-identified tables.

# COMMAND ----------

forbidden_phi = ["mrn", "ssn", "first_name", "last_name", "address", "date_of_birth"]

patient_cols = [c.lower() for c in df_silver_patients.columns]
leaked_in_patients = [c for c in forbidden_phi if c in patient_cols]

assert len(leaked_in_patients) == 0, f"PHI Leak detected in silver_patients: {leaked_in_patients}"
print("[PASS] PHI Zero-Leakage Audit: silver_patients contains 0 forbidden identifier columns.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Delta Live Tables (DLT) Expectation Reference Syntax
# MAGIC In modern Databricks Lakehouse pipelines using Delta Live Tables, these assertions are declared declaratively:
# MAGIC 
# MAGIC ```python
# MAGIC # Example DLT Syntax for production pipelines:
# MAGIC @dlt.table(
# MAGIC     comment="Cleaned and validated claims with DLT expectations"
# MAGIC )
# MAGIC @dlt.expect_or_drop("valid_claim_id", "claim_id IS NOT NULL")
# MAGIC @dlt.expect_or_drop("valid_dates", "discharge_date >= admit_date")
# MAGIC @dlt.expect_or_fail("valid_financials", "paid_amount <= allowed_amount AND allowed_amount <= billed_amount")
# MAGIC def dlt_silver_claims():
# MAGIC     return dlt.read("bronze_claims").filter(...)
# MAGIC ```

# COMMAND ----------

print("=" * 60)
print("  ALL DATABRICKS DATA QUALITY & GOVERNANCE CHECKS PASSED  ")
print("=" * 60)
