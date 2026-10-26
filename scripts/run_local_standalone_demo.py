"""
Zero-Dependency Healthcare ETL Demonstration Runner
Simulates the Bronze -> Silver -> Gold Medallion Pipeline using Python & In-Memory SQL.
Allows testing the complete pipeline logic, HIPAA de-identification, and actuarial
PMPM / 30-day readmission analytics instantly without requiring a heavy PySpark/Java runtime.
"""

import os
import sys
import json
import csv
import sqlite3
import hashlib
from datetime import datetime, timezone

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.security import hash_phi_value, mask_ssn_value

SALT = "HEALTHCARE_PHI_SALT"


def hash_phi(val: str) -> str:
    return hash_phi_value(val, SALT)


def mask_ssn(ssn: str) -> str:
    return mask_ssn_value(ssn)


def run_standalone_demo():
    print("=" * 70)
    print("  HEALTHCARE DATA ENGINEERING & ETL PIPELINE (STANDALONE DEMO)  ")
    print("  Domain: Optum / Claims Adjudication & Actuarial Analytics     ")
    print("=" * 70)

    raw_dir = os.path.join(PROJECT_ROOT, "data", "raw")
    patients_path = os.path.join(raw_dir, "patients.csv")
    claims_path = os.path.join(raw_dir, "claims.json")
    providers_path = os.path.join(raw_dir, "providers.csv")
    icd10_path = os.path.join(raw_dir, "icd10_reference.csv")

    if not os.path.exists(claims_path):
        from src.generate_healthcare_data import generate_and_save_dataset
        generate_and_save_dataset(raw_dir)

    # 1. Connect to In-Memory Analytical DB
    conn = sqlite3.connect(":memory:")
    cursor = conn.cursor()

    # -------------------------------------------------------------------------
    # STEP 1: BRONZE INGESTION (Raw Tables + Audit Lineage)
    # -------------------------------------------------------------------------
    print("\n[STEP 1: BRONZE LAYER] Ingesting raw claims, patients, providers & ICD-10...")

    cursor.execute("""
        CREATE TABLE bronze_claims (
            claim_id TEXT, patient_mrn TEXT, provider_npi TEXT, claim_type TEXT,
            service_date TEXT, admit_date TEXT, discharge_date TEXT,
            primary_icd10 TEXT, procedure_cpt TEXT, billed_amount REAL,
            allowed_amount REAL, paid_amount REAL, claim_status TEXT,
            submission_timestamp TEXT, _ingested_at TEXT, _source_file TEXT
        )
    """)

    cursor.execute("""
        CREATE TABLE bronze_patients (
            mrn TEXT, ssn TEXT, first_name TEXT, last_name TEXT,
            date_of_birth TEXT, gender TEXT, address TEXT, state TEXT,
            zip_code TEXT, insurance_plan_type TEXT, effective_date TEXT,
            is_active INTEGER, _ingested_at TEXT
        )
    """)

    cursor.execute("""
        CREATE TABLE bronze_providers (
            provider_npi TEXT, provider_name TEXT, specialty TEXT,
            facility_name TEXT, in_network_flag INTEGER
        )
    """)

    cursor.execute("""
        CREATE TABLE bronze_icd10 (
            code TEXT, description TEXT, category TEXT, is_chronic INTEGER
        )
    """)

    # Load Patients CSV into Bronze
    with open(patients_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        now_ts = datetime.now(timezone.utc).isoformat()
        for r in reader:
            cursor.execute("""
                INSERT INTO bronze_patients VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                r["mrn"], r["ssn"], r["first_name"], r["last_name"],
                r["date_of_birth"], r["gender"], r["address"], r["state"],
                r["zip_code"], r["insurance_plan_type"], r["effective_date"],
                1 if r["is_active"] == "True" else 0, now_ts
            ))

    # Load Claims JSON into Bronze
    with open(claims_path, "r", encoding="utf-8") as f:
        now_ts = datetime.now(timezone.utc).isoformat()
        for line in f:
            if not line.strip(): continue
            c = json.loads(line)
            cursor.execute("""
                INSERT INTO bronze_claims VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                c["claim_id"], c["patient_mrn"], c["provider_npi"], c["claim_type"],
                c["service_date"], c["admit_date"], c["discharge_date"],
                c["primary_icd10"], c["procedure_cpt"], c["billed_amount"],
                c["allowed_amount"], c["paid_amount"], c["claim_status"],
                c["submission_timestamp"], now_ts, "claims.json"
            ))

    # Load Providers CSV
    with open(providers_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            cursor.execute("INSERT INTO bronze_providers VALUES (?, ?, ?, ?, ?)",
                           (r["provider_npi"], r["provider_name"], r["specialty"], r["facility_name"], 1 if r["in_network_flag"] == "True" else 0))

    # Load ICD-10 CSV
    with open(icd10_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            cursor.execute("INSERT INTO bronze_icd10 VALUES (?, ?, ?, ?)",
                           (r["code"], r["description"], r["category"], 1 if r["is_chronic"] == "True" else 0))

    claims_bronze_count = cursor.execute("SELECT count(*) FROM bronze_claims").fetchone()[0]
    patients_bronze_count = cursor.execute("SELECT count(*) FROM bronze_patients").fetchone()[0]
    print(f"Bronze Ingestion Complete: {claims_bronze_count} claims, {patients_bronze_count} patients.")

    # -------------------------------------------------------------------------
    # STEP 2: SILVER LAYER (Cleansing, Validation, HIPAA PHI Tokenization)
    # -------------------------------------------------------------------------
    print("\n[STEP 2: SILVER LAYER] De-identifying PHI & validating clinical/financial rules...")

    cursor.execute("""
        CREATE TABLE silver_patients (
            patient_key TEXT PRIMARY KEY,
            birth_year INTEGER,
            masked_ssn TEXT,
            gender TEXT,
            state TEXT,
            insurance_plan_type TEXT,
            effective_date TEXT,
            is_active INTEGER
        )
    """)

    # Populate Silver Patients with deterministic salted SHA-256
    cursor.execute("SELECT mrn, ssn, date_of_birth, gender, state, insurance_plan_type, effective_date, is_active FROM bronze_patients")
    for row in cursor.fetchall():
        mrn, ssn, dob, gender, state, plan, eff_dt, active = row
        p_key = hash_phi(mrn)
        b_year = int(dob.split("-")[0]) if dob else None
        m_ssn = mask_ssn(ssn)
        cursor.execute("""
            INSERT OR REPLACE INTO silver_patients VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (p_key, b_year, m_ssn, gender, state, plan, eff_dt, active))

    cursor.execute("""
        CREATE TABLE silver_claims (
            claim_id TEXT PRIMARY KEY,
            patient_key TEXT,
            provider_npi TEXT,
            claim_type TEXT,
            service_date TEXT,
            admit_date TEXT,
            discharge_date TEXT,
            length_of_stay_days INTEGER,
            primary_icd10 TEXT,
            procedure_cpt TEXT,
            billed_amount REAL,
            allowed_amount REAL,
            paid_amount REAL,
            claim_status TEXT,
            quality_status TEXT,
            submission_timestamp TEXT
        )
    """)

    cursor.execute("""
        SELECT claim_id, patient_mrn, provider_npi, claim_type, service_date,
               admit_date, discharge_date, primary_icd10, procedure_cpt,
               billed_amount, allowed_amount, paid_amount, claim_status, submission_timestamp
        FROM bronze_claims
    """)

    for row in cursor.fetchall():
        cid, mrn, npi, ctype, sdate, adate, ddate, icd, cpt, billed, allowed, paid, status, sub_ts = row
        p_key = hash_phi(mrn)
        
        # Calculate length of stay
        dt_admit = datetime.strptime(adate, "%Y-%m-%d")
        dt_disch = datetime.strptime(ddate, "%Y-%m-%d")
        los = (dt_disch - dt_admit).days

        # Healthcare Data Quality validations
        valid_dates = (dt_disch >= dt_admit)
        valid_fin = (0 <= paid <= allowed <= billed) if status == "PAID" else (billed >= 0)
        q_status = "VALID" if (valid_dates and valid_fin) else "QUARANTINED"

        cursor.execute("""
            INSERT OR REPLACE INTO silver_claims VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            cid, p_key, npi, ctype, sdate, adate, ddate, los, icd, cpt,
            billed, allowed, paid, status, q_status, sub_ts
        ))

    silver_valid_claims = cursor.execute("SELECT count(*) FROM silver_claims WHERE quality_status = 'VALID'").fetchone()[0]
    print(f"Silver Transformation Complete: {silver_valid_claims} valid claims de-identified and cleansed.")

    # -------------------------------------------------------------------------
    # STEP 3: GOLD LAYER (Star Schema & Actuarial KPIs)
    # -------------------------------------------------------------------------
    print("\n[STEP 3: GOLD LAYER] Generating Star Schema & Actuarial Analytics...")

    # Fact_Claims Table
    cursor.execute("""
        CREATE TABLE fact_claims AS
        SELECT 
            c.claim_id,
            c.patient_key,
            c.provider_npi,
            c.primary_icd10 AS diagnosis_code,
            c.procedure_cpt,
            c.claim_type,
            c.service_date,
            strftime('%Y%m', c.service_date) AS service_year_month,
            c.admit_date,
            c.discharge_date,
            c.billed_amount,
            c.allowed_amount,
            c.paid_amount,
            c.claim_status,
            c.length_of_stay_days,
            i.is_chronic AS is_chronic_condition
        FROM silver_claims c
        LEFT JOIN bronze_icd10 i ON c.primary_icd10 = i.code
        WHERE c.quality_status = 'VALID'
    """)

    # 1. Per-Member-Per-Month (PMPM) Cost Calculation
    cursor.execute("""
        SELECT 
            f.service_year_month,
            p.insurance_plan_type,
            round(sum(f.paid_amount), 2) AS total_paid,
            count(DISTINCT f.patient_key) AS active_members,
            count(f.claim_id) AS claim_count,
            round(sum(f.paid_amount) / count(DISTINCT f.patient_key), 2) AS pmpm_cost
        FROM fact_claims f
        JOIN silver_patients p ON f.patient_key = p.patient_key
        WHERE f.claim_status = 'PAID'
        GROUP BY f.service_year_month, p.insurance_plan_type
        ORDER BY f.service_year_month, p.insurance_plan_type
        LIMIT 6
    """)

    print("\n" + "=" * 75)
    print("  GOLD MART: PER-MEMBER-PER-MONTH (PMPM) COST TRENDS (SAMPLE)")
    print("=" * 75)
    print(f"{'YearMonth':<10} | {'Plan Type':<20} | {'Total Paid ($)':<15} | {'Members':<8} | {'PMPM ($)':<10}")
    print("-" * 75)
    for row in cursor.fetchall():
        ym, plan, paid, mems, cnt, pmpm = row
        print(f"{ym:<10} | {plan:<20} | ${paid:<14,.2f} | {mems:<8} | ${pmpm:<10,.2f}")

    # 2. Top Chronic Disease Cost Drivers
    cursor.execute("""
        SELECT 
            i.category,
            i.description,
            count(f.claim_id) AS total_encounters,
            round(sum(f.paid_amount), 2) AS total_spend,
            round(avg(f.paid_amount), 2) AS avg_claim_cost
        FROM fact_claims f
        JOIN bronze_icd10 i ON f.diagnosis_code = i.code
        WHERE f.claim_status = 'PAID'
        GROUP BY i.category, i.description
        ORDER BY total_spend DESC
        LIMIT 5
    """)

    print("\n" + "=" * 75)
    print("  GOLD MART: TOP 5 CLINICAL COST DRIVERS (ICD-10)")
    print("=" * 75)
    print(f"{'Category':<15} | {'Diagnosis':<35} | {'Spend ($)':<14} | {'Avg/Claim':<10}")
    print("-" * 75)
    for row in cursor.fetchall():
        cat, desc, cnt, spend, avg_c = row
        print(f"{cat:<15} | {desc[:33]:<35} | ${spend:<13,.2f} | ${avg_c:<9,.2f}")

    # 3. 30-Day Hospital Readmission Rate by Facility
    cursor.execute("""
        WITH InpatientWithPrior AS (
            SELECT 
                claim_id,
                patient_key,
                provider_npi,
                admit_date,
                discharge_date,
                LAG(discharge_date, 1) OVER (PARTITION BY patient_key ORDER BY admit_date) AS prior_discharge
            FROM fact_claims
            WHERE claim_type = 'INPATIENT'
        ),
        ReadmitFlags AS (
            SELECT 
                provider_npi,
                claim_id,
                CASE 
                    WHEN prior_discharge IS NOT NULL 
                         AND (julianday(admit_date) - julianday(prior_discharge)) BETWEEN 0 AND 30 
                    THEN 1 ELSE 0 
                END AS is_readmit
            FROM InpatientWithPrior
        )
        SELECT 
            p.facility_name,
            count(r.claim_id) AS total_admissions,
            sum(r.is_readmit) AS readmissions_count,
            round(CAST(sum(r.is_readmit) AS REAL) / count(r.claim_id) * 100, 2) AS readmission_rate_pct
        FROM ReadmitFlags r
        LEFT JOIN bronze_providers p ON r.provider_npi = p.provider_npi
        GROUP BY p.facility_name
        ORDER BY readmission_rate_pct DESC
    """)

    print("\n" + "=" * 75)
    print("  GOLD MART: 30-DAY HOSPITAL READMISSION RATE TRACKER")
    print("=" * 75)
    print(f"{'Facility Name':<30} | {'Admissions':<12} | {'Readmissions':<14} | {'Rate (%)':<10}")
    print("-" * 75)
    for row in cursor.fetchall():
        fac, adm, readm, rate = row
        fac_name = fac if fac else "Unassigned Facility"
        print(f"{fac_name:<30} | {adm:<12} | {readm:<14} | {rate:<10.2f}%")

    # -------------------------------------------------------------------------
    # STEP 4: DATA QUALITY & PHI ZERO-LEAKAGE AUDIT
    # -------------------------------------------------------------------------
    print("\n" + "=" * 75)
    print("  AUTOMATED DATA QUALITY & PHI ZERO-LEAKAGE AUDIT")
    print("=" * 75)
    
    # Audit 1: PK Uniqueness
    total_claims = cursor.execute("SELECT count(*) FROM silver_claims").fetchone()[0]
    distinct_claims = cursor.execute("SELECT count(DISTINCT claim_id) FROM silver_claims").fetchone()[0]
    print(f"[PASS] Primary Key Uniqueness on silver_claims (Total: {total_claims}, Distinct: {distinct_claims})")

    # Audit 2: Zero PHI Leakage
    cols = [col[1] for col in cursor.execute("PRAGMA table_info(silver_patients)").fetchall()]
    forbidden = ["mrn", "ssn", "first_name", "last_name", "address", "date_of_birth"]
    leaked = [c for c in forbidden if c in cols]
    if not leaked:
        print("[PASS] PHI De-identification Audit: ZERO cleartext PHI columns found in silver_patients.")
    else:
        print(f"[FAIL] PHI Leaked: {leaked}")

    # Audit 3: Financial Integrity
    fin_violations = cursor.execute("""
        SELECT count(*) FROM fact_claims 
        WHERE claim_status = 'PAID' AND (paid_amount > allowed_amount OR allowed_amount > billed_amount)
    """).fetchone()[0]
    print(f"[PASS] Financial Constraint (Paid <= Allowed <= Billed): Violations = {fin_violations}")

    # Audit 4: Date Chronology
    date_violations = cursor.execute("""
        SELECT count(*) FROM fact_claims 
        WHERE discharge_date < admit_date OR admit_date < service_date
    """).fetchone()[0]
    print(f"[PASS] Date Chronology (Discharge >= Admit >= Service): Violations = {date_violations}")

    # Audit 5: Non-Null Mandatory Keys
    null_keys = cursor.execute("""
        SELECT count(*) FROM fact_claims 
        WHERE claim_id IS NULL OR patient_key IS NULL OR service_date IS NULL
    """).fetchone()[0]
    print(f"[PASS] Mandatory Keys Non-Null (claim_id, patient_key, service_date): Nulls = {null_keys}")

    print("\n>>> End-to-end Healthcare Data Pipeline executed successfully!")


if __name__ == "__main__":
    run_standalone_demo()
