-- ==============================================================================
-- Healthcare Data Engineering & Claims Analytics Pipeline
-- Designed for Azure Databricks & Delta Lake
-- Medallion Architecture: Bronze -> Silver -> Gold
--
-- NOTE: This script demonstrates Lakehouse Spark SQL & Delta Lake operations
-- using 100% synthetic healthcare data. It does NOT claim actual regulatory HIPAA compliance.
-- ==============================================================================

-- ------------------------------------------------------------------------------
-- 1. DATABASE & STORAGE SETUP (Azure ADLS Gen2 Mapping)
-- ------------------------------------------------------------------------------
-- In Azure Databricks, external locations are mounted via Unity Catalog:
-- 'abfss://<container>@<storage-account>.dfs.core.windows.net/<path>'
CREATE DATABASE IF NOT EXISTS optum_healthcare_db
LOCATION 'abfss://healthcare-lake@optumadlsgen2.dfs.core.windows.net/data/';

USE optum_healthcare_db;

-- ------------------------------------------------------------------------------
-- 2. BRONZE LAYER: Raw Ingestion with Schema Enforcement & Ingestion Metadata
-- ------------------------------------------------------------------------------
-- In Databricks production, Auto Loader (cloudFiles) incrementally ingests arriving files
CREATE OR REPLACE TABLE bronze_claims (
    claim_id STRING,
    patient_mrn STRING,
    provider_npi STRING,
    claim_type STRING,
    service_date STRING,
    admit_date STRING,
    discharge_date STRING,
    primary_icd10 STRING,
    procedure_cpt STRING,
    billed_amount DOUBLE,
    allowed_amount DOUBLE,
    paid_amount DOUBLE,
    claim_status STRING,
    submission_timestamp STRING,
    ingestion_timestamp TIMESTAMP,
    source_file STRING,
    batch_id STRING
)
USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

CREATE OR REPLACE TABLE bronze_patients (
    mrn STRING,
    ssn STRING,
    first_name STRING,
    last_name STRING,
    date_of_birth STRING,
    gender STRING,
    address STRING,
    state STRING,
    zip_code STRING,
    insurance_plan_type STRING,
    effective_date STRING,
    is_active BOOLEAN,
    ingestion_timestamp TIMESTAMP,
    source_file STRING,
    batch_id STRING
)
USING DELTA;

-- ------------------------------------------------------------------------------
-- 3. SILVER LAYER: PHI De-Identification, Cleansing & Delta MERGE
-- ------------------------------------------------------------------------------
-- Patients: Apply deterministic salted SHA-256 and remove raw PHI identifiers
CREATE OR REPLACE TABLE silver_patients
USING DELTA
PARTITIONED BY (state)
AS
SELECT 
    sha2(concat(coalesce(mrn, 'NULL_VAL'), 'HEALTHCARE_PHI_SALT'), 256) AS patient_key,
    year(to_date(date_of_birth)) AS birth_year,
    concat('XXX-XX-', substring(regexp_replace(ssn, '[^0-9]', ''), -4, 4)) AS masked_ssn,
    upper(trim(gender)) AS gender,
    upper(trim(state)) AS state,
    insurance_plan_type,
    to_date(effective_date) AS effective_date,
    is_active,
    current_timestamp() AS _silver_processed_at
FROM bronze_patients;

-- Claims: Delta Table declaration with claim_type partitioning
CREATE TABLE IF NOT EXISTS silver_claims (
    claim_id STRING,
    patient_key STRING,
    provider_npi STRING,
    claim_type STRING,
    service_date DATE,
    admit_date DATE,
    discharge_date DATE,
    length_of_stay_days INT,
    primary_icd10 STRING,
    procedure_cpt STRING,
    billed_amount DOUBLE,
    allowed_amount DOUBLE,
    paid_amount DOUBLE,
    claim_status STRING,
    quality_status STRING,
    submission_timestamp TIMESTAMP,
    _silver_processed_at TIMESTAMP
)
USING DELTA
PARTITIONED BY (claim_type);

-- Delta Lake MERGE (Upsert) for late-arriving claim adjustments and resubmissions
-- Matches on business key `claim_id` and updates in-place if incoming timestamp is newer
MERGE INTO silver_claims AS target
USING (
    SELECT 
        claim_id,
        sha2(concat(coalesce(patient_mrn, 'NULL_VAL'), 'HEALTHCARE_PHI_SALT'), 256) AS patient_key,
        provider_npi,
        claim_type,
        to_date(service_date) AS service_date,
        to_date(admit_date) AS admit_date,
        to_date(discharge_date) AS discharge_date,
        datediff(to_date(discharge_date), to_date(admit_date)) AS length_of_stay_days,
        primary_icd10,
        procedure_cpt,
        round(billed_amount, 2) AS billed_amount,
        round(allowed_amount, 2) AS allowed_amount,
        round(paid_amount, 2) AS paid_amount,
        claim_status,
        CASE 
            WHEN to_date(discharge_date) >= to_date(admit_date) 
                 AND to_date(admit_date) >= to_date(service_date)
                 AND paid_amount <= allowed_amount 
                 AND allowed_amount <= billed_amount
                 AND primary_icd10 RLIKE '^[A-Z][0-9][0-9A-Z](\\.[0-9A-Z]{1,4})?$'
            THEN 'VALID'
            ELSE 'QUARANTINED'
        END AS quality_status,
        to_timestamp(submission_timestamp) AS submission_timestamp,
        current_timestamp() AS _silver_processed_at
    FROM bronze_claims
) AS source
ON target.claim_id = source.claim_id
WHEN MATCHED AND source.submission_timestamp > target.submission_timestamp THEN
    UPDATE SET *
WHEN NOT MATCHED THEN
    INSERT *;

-- ------------------------------------------------------------------------------
-- 4. GOLD LAYER: Star Schema & Healthcare Analytical Marts
-- ------------------------------------------------------------------------------

-- Fact Table: Fact_Claims (Uses Databricks Liquid Clustering)
CREATE OR REPLACE TABLE fact_claims
USING DELTA
CLUSTER BY (patient_key, service_date)
AS
SELECT 
    c.claim_id,
    c.patient_key,
    c.provider_npi,
    c.primary_icd10 AS diagnosis_code,
    c.procedure_cpt,
    c.claim_type,
    c.service_date,
    date_format(c.service_date, 'yyyyMM') AS service_year_month,
    c.admit_date,
    c.discharge_date,
    c.length_of_stay_days,
    c.billed_amount,
    c.allowed_amount,
    c.paid_amount,
    c.claim_status
FROM silver_claims c
WHERE c.quality_status = 'VALID';

-- Analytics Mart 1: Per-Member-Per-Month (PMPM) Cost Trends by Insurance Plan
CREATE OR REPLACE TABLE mart_pmpm_cost_analysis
USING DELTA AS
SELECT 
    f.service_year_month,
    p.insurance_plan_type,
    round(sum(f.paid_amount), 2) AS total_claims_paid,
    count(DISTINCT f.patient_key) AS active_utilizing_members,
    count(f.claim_id) AS total_claims_count,
    round(sum(f.paid_amount) / count(DISTINCT f.patient_key), 2) AS pmpm_cost,
    round(sum(f.paid_amount) / count(f.claim_id), 2) AS avg_cost_per_claim
FROM fact_claims f
INNER JOIN silver_patients p ON f.patient_key = p.patient_key
WHERE f.claim_status = 'PAID'
GROUP BY f.service_year_month, p.insurance_plan_type
ORDER BY f.service_year_month, p.insurance_plan_type;

-- Analytics Mart 2: 30-Day Hospital Readmission Rate Tracker by Facility
CREATE OR REPLACE TABLE mart_30day_readmissions
USING DELTA AS
WITH InpatientEncounters AS (
    SELECT 
        claim_id,
        patient_key,
        provider_npi,
        admit_date,
        discharge_date,
        lag(discharge_date, 1) OVER (PARTITION BY patient_key ORDER BY admit_date) AS prior_discharge_date
    FROM fact_claims
    WHERE claim_type = 'INPATIENT'
),
ReadmitFlags AS (
    SELECT 
        provider_npi,
        claim_id,
        CASE 
            WHEN prior_discharge_date IS NOT NULL 
                 AND datediff(admit_date, prior_discharge_date) BETWEEN 0 AND 30 
            THEN 1 ELSE 0 
        END AS is_30day_readmission
    FROM InpatientEncounters
)
SELECT 
    coalesce(pr.facility_name, 'Unassigned Facility') AS facility_name,
    count(r.claim_id) AS total_inpatient_admissions,
    sum(r.is_30day_readmission) AS total_30day_readmissions,
    round((sum(r.is_30day_readmission) / count(r.claim_id)) * 100, 2) AS readmission_rate_pct
FROM ReadmitFlags r
LEFT JOIN bronze_providers pr ON r.provider_npi = pr.provider_npi
GROUP BY pr.facility_name
ORDER BY readmission_rate_pct DESC;

-- ------------------------------------------------------------------------------
-- 5. PERFORMANCE TUNING & COMPACTION
-- ------------------------------------------------------------------------------
-- Z-Order by high-cardinality join key to accelerate file skipping
OPTIMIZE fact_claims
ZORDER BY (patient_key, diagnosis_code);

-- Vacuum obsolete file versions older than 7 days (preserving 168-hour time travel)
VACUUM fact_claims RETAIN 168 HOURS;
