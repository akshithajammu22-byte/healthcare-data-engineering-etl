"""
Synthetic Healthcare Data Generator
Generates realistic simulated healthcare data for claims processing,
including patient demographics, claims, providers, and ICD-10 codes.

NOTE: This is a student portfolio project using 100% synthetic data.
No real patient or personal data is processed or stored.
"""

import os
import json
import csv
import random
from datetime import datetime, timedelta

# Seed for reproducible synthetic datasets
random.seed(42)

ICD10_CATALOG = [
    {"code": "I10", "description": "Essential (primary) hypertension", "category": "Circulatory", "is_chronic": True},
    {"code": "E11.9", "description": "Type 2 diabetes mellitus without complications", "category": "Endocrine", "is_chronic": True},
    {"code": "I50.9", "description": "Heart failure, unspecified", "category": "Circulatory", "is_chronic": True},
    {"code": "J44.9", "description": "Chronic obstructive pulmonary disease, unspecified", "category": "Respiratory", "is_chronic": True},
    {"code": "M54.5", "description": "Low back pain", "category": "Musculoskeletal", "is_chronic": False},
    {"code": "Z00.00", "description": "Encounter for general adult medical examination", "category": "Preventive", "is_chronic": False},
    {"code": "J06.9", "description": "Acute upper respiratory infection, unspecified", "category": "Infectious", "is_chronic": False},
    {"code": "N18.3", "description": "Chronic kidney disease, stage 3", "category": "Genitourinary", "is_chronic": True},
    {"code": "F32.9", "description": "Major depressive disorder, single episode", "category": "Mental Health", "is_chronic": True},
    {"code": "K21.9", "description": "Gastro-esophageal reflux disease without esophagitis", "category": "Digestive", "is_chronic": False},
]

CPT_CODES = ["99213", "99214", "99283", "99285", "80053", "85025", "71045", "93000"]
INSURANCE_PLANS = ["Medicare Advantage", "Commercial PPO", "Commercial HMO", "Medicaid"]
CLAIM_STATUSES = ["PAID", "PAID", "PAID", "DENIED", "IN_PROCESS"]
CLAIM_TYPES = ["INPATIENT", "OUTPATIENT", "EMERGENCY"]
FIRST_NAMES = ["James", "Mary", "Robert", "Patricia", "John", "Jennifer", "Michael", "Linda", "David", "Elizabeth", "William", "Barbara", "Richard", "Susan", "Joseph", "Jessica"]
LAST_NAMES = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson"]
STATES = ["MN", "TX", "CA", "NY", "FL", "IL", "NC", "PA", "OH", "GA"]


def generate_patients(count: int = 300) -> list[dict]:
    patients = []
    base_date = datetime(2025, 1, 1)
    
    for i in range(1, count + 1):
        mrn = f"MRN{i:07d}"
        dob = datetime(1940, 1, 1) + timedelta(days=random.randint(0, 25000))
        patient = {
            "mrn": mrn,
            "ssn": f"{random.randint(100, 999)}-{random.randint(10, 99)}-{random.randint(1000, 9999)}",
            "first_name": random.choice(FIRST_NAMES),
            "last_name": random.choice(LAST_NAMES),
            "date_of_birth": dob.strftime("%Y-%m-%d"),
            "gender": random.choice(["M", "F", "OTHER"]),
            "address": f"{random.randint(100, 9999)} Healthcare Blvd",
            "state": random.choice(STATES),
            "zip_code": f"{random.randint(10000, 99999)}",
            "insurance_plan_type": random.choice(INSURANCE_PLANS),
            "effective_date": (base_date - timedelta(days=random.randint(30, 700))).strftime("%Y-%m-%d"),
            "is_active": True
        }
        patients.append(patient)
    return patients


def generate_providers(count: int = 30) -> list[dict]:
    providers = []
    specialties = ["Cardiology", "Family Medicine", "Internal Medicine", "Oncology", "Orthopedics", "Emergency Medicine"]
    facilities = ["Optum Health Center", "Memorial Hospital", "Mercy Medical Center", "St. Jude Clinic", "Regional General"]
    
    for i in range(1, count + 1):
        npi = f"1{i:09d}"
        providers.append({
            "provider_npi": npi,
            "provider_name": f"Dr. {random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}, MD",
            "specialty": random.choice(specialties),
            "facility_name": random.choice(facilities),
            "in_network_flag": random.choice([True, True, True, False])
        })
    return providers


def generate_claims(patients: list[dict], providers: list[dict], count: int = 1500) -> list[dict]:
    claims = []
    start_date = datetime(2025, 1, 1)
    
    for i in range(1, count + 1):
        claim_id = f"CLM{i:08d}"
        patient = random.choice(patients)
        provider = random.choice(providers)
        diag = random.choice(ICD10_CATALOG)
        claim_type = random.choice(CLAIM_TYPES)
        
        service_dt = start_date + timedelta(days=random.randint(0, 360))
        
        if claim_type == "INPATIENT":
            admit_dt = service_dt
            los = random.randint(1, 10)
            discharge_dt = admit_dt + timedelta(days=los)
            billed = round(random.uniform(5000.0, 45000.0), 2)
        else:
            admit_dt = service_dt
            discharge_dt = service_dt
            billed = round(random.uniform(150.0, 3500.0), 2)
            
        status = random.choice(CLAIM_STATUSES)
        if status == "PAID":
            allowed = round(billed * random.uniform(0.65, 0.85), 2)
            copay = round(random.choice([0, 20, 40, 50, 100]), 2)
            paid = round(max(allowed - copay, 0.0), 2)
        elif status == "DENIED":
            allowed = 0.0
            copay = 0.0
            paid = 0.0
        else: # IN_PROCESS
            allowed = 0.0
            copay = 0.0
            paid = 0.0

        claim = {
            "claim_id": claim_id,
            "patient_mrn": patient["mrn"],
            "provider_npi": provider["provider_npi"],
            "claim_type": claim_type,
            "service_date": service_dt.strftime("%Y-%m-%d"),
            "admit_date": admit_dt.strftime("%Y-%m-%d"),
            "discharge_date": discharge_dt.strftime("%Y-%m-%d"),
            "primary_icd10": diag["code"],
            "procedure_cpt": random.choice(CPT_CODES),
            "billed_amount": billed,
            "allowed_amount": allowed,
            "paid_amount": paid,
            "claim_status": status,
            "submission_timestamp": (discharge_dt + timedelta(days=random.randint(1, 14))).isoformat()
        }
        claims.append(claim)
    return claims


def generate_and_save_dataset(output_dir: str = "data/raw"):
    os.makedirs(output_dir, exist_ok=True)
    
    # 1. Generate & save Patients
    patients = generate_patients(300)
    patients_file = os.path.join(output_dir, "patients.csv")
    with open(patients_file, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(patients[0].keys()))
        writer.writeheader()
        writer.writerows(patients)
    print(f"Generated {len(patients)} patients -> {patients_file}")

    # 2. Generate & save Providers
    providers = generate_providers(30)
    providers_file = os.path.join(output_dir, "providers.csv")
    with open(providers_file, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(providers[0].keys()))
        writer.writeheader()
        writer.writerows(providers)
    print(f"Generated {len(providers)} providers -> {providers_file}")

    # 3. Save ICD-10 Reference Master
    icd10_file = os.path.join(output_dir, "icd10_reference.csv")
    with open(icd10_file, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(ICD10_CATALOG[0].keys()))
        writer.writeheader()
        writer.writerows(ICD10_CATALOG)
    print(f"Saved {len(ICD10_CATALOG)} ICD-10 codes -> {icd10_file}")

    # 4. Generate & save Claims JSON
    claims = generate_claims(patients, providers, 1500)
    claims_file = os.path.join(output_dir, "claims.json")
    with open(claims_file, mode="w", encoding="utf-8") as f:
        for claim in claims:
            f.write(json.dumps(claim) + "\n")
    print(f"Generated {len(claims)} claims -> {claims_file}")


if __name__ == "__main__":
    generate_and_save_dataset()
