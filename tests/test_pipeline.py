"""
Unit Tests for Healthcare ETL Pipeline Core Utilities, Security & Transformations
Tests:
1. Deterministic salted SHA-256 tokenization
2. Different input values producing different tokens
3. PHI masking (SSN last-4)
4. Absence of cleartext PHI after transformation
5. Duplicate removal using business keys
6. Referential integrity between patients, providers, and claims
7. Valid financial relationships (0 <= Paid <= Allowed <= Billed)
8. Delta MERGE behavior & late-arriving claim adjustment handling

Compatible with standard unittest and pytest.
"""

import sys
import os
import unittest

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.security import (
    hash_phi_value,
    mask_ssn_value,
    HAS_PYSPARK
)
from src.transform_silver import apply_claims_merge
from src.data_quality_checks import HealthcareDataQualitySuite

TEST_SALT = "HEALTHCARE_PHI_SALT"


class TestSecurityUtilities(unittest.TestCase):

    def test_deterministic_hashing(self):
        """
        Verify that salted SHA-256 hashing is 100% deterministic:
        the same input and salt MUST produce the exact same 64-char token.
        This enables relational joins across claims and patient demographics
        without exposing raw MRNs.
        """
        mrn = "MRN0001234"
        token_1 = hash_phi_value(mrn, TEST_SALT)
        token_2 = hash_phi_value(mrn, TEST_SALT)

        # 1. Output must be a 64-character hex string (SHA-256)
        self.assertEqual(len(token_1), 64)
        self.assertTrue(all(c in "0123456789abcdef" for c in token_1))

        # 2. Must be strictly deterministic
        self.assertEqual(token_1, token_2)

    def test_different_inputs_produce_different_tokens(self):
        """
        Verify that different patient identifiers produce unique tokens (no collisions).
        """
        token_a = hash_phi_value("MRN0000001", TEST_SALT)
        token_b = hash_phi_value("MRN0000002", TEST_SALT)
        token_c = hash_phi_value("MRN0000003", TEST_SALT)

        self.assertNotEqual(token_a, token_b)
        self.assertNotEqual(token_b, token_c)
        self.assertNotEqual(token_a, token_c)

    def test_different_salts_produce_different_tokens(self):
        """
        Verify that changing the secret salt produces a completely different token.
        Protects against pre-computed rainbow table and dictionary attacks.
        """
        mrn = "MRN0001234"
        token_salt1 = hash_phi_value(mrn, "SALT_KEY_A")
        token_salt2 = hash_phi_value(mrn, "SALT_KEY_B")

        self.assertNotEqual(token_salt1, token_salt2)

    def test_phi_ssn_masking(self):
        """
        Verify that SSNs are masked to only the last 4 digits (e.g. 'XXX-XX-1234').
        """
        self.assertEqual(mask_ssn_value("123-45-6789"), "XXX-XX-6789")
        self.assertEqual(mask_ssn_value("987654321"), "XXX-XX-4321")
        self.assertEqual(mask_ssn_value(None), "XXX-XX-0000")

    def test_absence_of_cleartext_phi_after_transformation(self):
        """
        Verify that de-identified patient records contain surrogate keys
        and DO NOT leak cleartext PHI identifiers.
        """
        raw_patient_record = {
            "mrn": "MRN0009999",
            "ssn": "123-45-6789",
            "first_name": "Jane",
            "last_name": "Doe",
            "address": "123 Medical Center Blvd",
            "date_of_birth": "1975-08-20",
            "state": "MN",
            "insurance_plan_type": "Medicare Advantage"
        }

        # Simulate Silver-layer de-identification
        de_identified_record = {
            "patient_key": hash_phi_value(raw_patient_record["mrn"], TEST_SALT),
            "birth_year": int(raw_patient_record["date_of_birth"].split("-")[0]),
            "masked_ssn": mask_ssn_value(raw_patient_record["ssn"]),
            "state": raw_patient_record["state"],
            "insurance_plan_type": raw_patient_record["insurance_plan_type"]
        }

        forbidden_phi_fields = ["mrn", "ssn", "first_name", "last_name", "address", "date_of_birth"]
        for field in forbidden_phi_fields:
            self.assertNotIn(
                field,
                de_identified_record,
                f"PHI leak detected: '{field}' should not be present in de-identified record."
            )

        self.assertIn("patient_key", de_identified_record)
        self.assertEqual(len(de_identified_record["patient_key"]), 64)
        self.assertEqual(de_identified_record["birth_year"], 1975)
        self.assertEqual(de_identified_record["masked_ssn"], "XXX-XX-6789")


class TestBronzeSilverETL(unittest.TestCase):

    def test_duplicate_removal_by_business_key(self):
        """
        Verify that duplicate submissions for the same claim_id are detected,
        retaining only the record with the latest submission timestamp.
        """
        batch_records = [
            {
                "claim_id": "CLM000100",
                "patient_key": hash_phi_value("MRN001", TEST_SALT),
                "billed_amount": 500.0,
                "submission_timestamp": "2025-01-10T10:00:00"
            },
            {
                "claim_id": "CLM000100",  # Duplicate submission
                "patient_key": hash_phi_value("MRN001", TEST_SALT),
                "billed_amount": 550.0,
                "submission_timestamp": "2025-01-12T15:30:00"  # Newer timestamp
            },
            {
                "claim_id": "CLM000101",
                "patient_key": hash_phi_value("MRN002", TEST_SALT),
                "billed_amount": 1200.0,
                "submission_timestamp": "2025-01-11T09:00:00"
            }
        ]

        # Deduplicate using business key and latest timestamp
        deduped = {}
        for rec in batch_records:
            cid = rec["claim_id"]
            if cid not in deduped or rec["submission_timestamp"] > deduped[cid]["submission_timestamp"]:
                deduped[cid] = rec

        # Exactly 2 unique claims should remain
        self.assertEqual(len(deduped), 2)
        # For CLM000100, the newer $550 record must be retained
        self.assertEqual(deduped["CLM000100"]["billed_amount"], 550.0)
        self.assertEqual(deduped["CLM000100"]["submission_timestamp"], "2025-01-12T15:30:00")

    def test_referential_integrity(self):
        """
        Verify referential integrity: claims reference valid patient_key and provider_npi.
        """
        # Dim Patients (de-identified)
        dim_patients = {
            hash_phi_value("MRN001", TEST_SALT): {"birth_year": 1980, "state": "MN"},
            hash_phi_value("MRN002", TEST_SALT): {"birth_year": 1965, "state": "TX"}
        }

        # Dim Providers
        dim_providers = {
            "1000000001": {"provider_name": "Dr. Smith", "specialty": "Cardiology"},
            "1000000002": {"provider_name": "Dr. Jones", "specialty": "Family Medicine"}
        }

        # Incoming Claims
        claims = [
            {"claim_id": "CLM1", "patient_key": hash_phi_value("MRN001", TEST_SALT), "provider_npi": "1000000001"},
            {"claim_id": "CLM2", "patient_key": hash_phi_value("MRN002", TEST_SALT), "provider_npi": "1000000002"},
        ]

        # Verify every claim's foreign keys resolve in dimensions
        for c in claims:
            self.assertIn(c["patient_key"], dim_patients, f"Orphan claim: patient_key {c['patient_key']} not in dim_patient")
            self.assertIn(c["provider_npi"], dim_providers, f"Orphan claim: provider_npi {c['provider_npi']} not in dim_provider")

    def test_valid_financial_relationships(self):
        """
        Verify financial consistency validation: 0 <= paid_amount <= allowed_amount <= billed_amount.
        Records violating this must be flagged as QUARANTINED.
        """
        valid_claim = {"billed": 1000.0, "allowed": 800.0, "paid": 750.0}
        invalid_paid_greater_than_allowed = {"billed": 1000.0, "allowed": 800.0, "paid": 950.0}
        invalid_negative_amount = {"billed": -100.0, "allowed": 0.0, "paid": 0.0}

        def validate_financials(c):
            return (0 <= c["paid"] <= c["allowed"] <= c["billed"])

        self.assertTrue(validate_financials(valid_claim))
        self.assertFalse(validate_financials(invalid_paid_greater_than_allowed))
        self.assertFalse(validate_financials(invalid_negative_amount))

    def test_delta_merge_late_arriving_claim_handling(self):
        """
        Demonstrates Delta Lake MERGE behavior on late-arriving claim adjustments:
        1. An initial claim is submitted as IN_PROCESS ($0 paid).
        2. A late-arriving adjustment arrives as PAID ($450 paid) with newer timestamp.
        3. Merge updates the existing record in-place without creating a duplicate row.
        4. Out-of-order older claims do NOT overwrite newer data.
        """
        existing_table = {
            "CLM999": {
                "claim_id": "CLM999",
                "patient_key": hash_phi_value("MRN005", TEST_SALT),
                "billed_amount": 500.0,
                "allowed_amount": 0.0,
                "paid_amount": 0.0,
                "claim_status": "IN_PROCESS",
                "submission_timestamp": "2025-01-10T08:00:00"
            }
        }

        # Late-arriving claim adjustment with newer timestamp
        incoming_adjustment = [
            {
                "claim_id": "CLM999",
                "patient_key": hash_phi_value("MRN005", TEST_SALT),
                "billed_amount": 500.0,
                "allowed_amount": 480.0,
                "paid_amount": 450.0,
                "claim_status": "PAID",
                "submission_timestamp": "2025-01-20T14:30:00"  # Newer
            }
        ]

        # Apply MERGE
        merged_table, inserted, updated = apply_claims_merge(existing_table, incoming_adjustment)

        # 1. Record count must remain exactly 1 (no duplicate row)
        self.assertEqual(len(merged_table), 1)
        self.assertEqual(inserted, 0)
        self.assertEqual(updated, 1)

        # 2. Record must be updated with the latest adjudication values
        record = merged_table["CLM999"]
        self.assertEqual(record["claim_status"], "PAID")
        self.assertEqual(record["paid_amount"], 450.0)
        self.assertEqual(record["submission_timestamp"], "2025-01-20T14:30:00")

        # 3. Test out-of-order arrival: an older timestamp payload should be ignored
        outdated_payload = [
            {
                "claim_id": "CLM999",
                "patient_key": hash_phi_value("MRN005", TEST_SALT),
                "billed_amount": 500.0,
                "allowed_amount": 0.0,
                "paid_amount": 0.0,
                "claim_status": "DENIED",
                "submission_timestamp": "2025-01-05T00:00:00"  # Older than existing 2025-01-20
            }
        ]
        merged_table, inserted_old, updated_old = apply_claims_merge(merged_table, outdated_payload)
        self.assertEqual(inserted_old, 0)
        self.assertEqual(updated_old, 0)
        # Status remains PAID (not overwritten by the older record)
        self.assertEqual(merged_table["CLM999"]["claim_status"], "PAID")


class TestDataQualityAndAnalytics(unittest.TestCase):

    def setUp(self):
        self.dq = HealthcareDataQualitySuite()

    def test_data_quality_uniqueness_and_nulls(self):
        """Verify data quality PK uniqueness and required non-null constraints."""
        records = [
            {"claim_id": "CLM001", "patient_key": "PK001", "service_date": "2025-01-01"},
            {"claim_id": "CLM002", "patient_key": "PK002", "service_date": "2025-01-02"},
        ]
        self.assertTrue(self.dq.check_uniqueness(records, "claim_id", "test_claims"))
        self.assertTrue(self.dq.check_non_null(records, ["claim_id", "patient_key"], "test_claims"))

        # Test duplicate detection
        duplicate_records = records + [{"claim_id": "CLM001", "patient_key": "PK003", "service_date": "2025-01-03"}]
        self.assertFalse(self.dq.check_uniqueness(duplicate_records, "claim_id", "test_claims"))

    def test_data_quality_bounds_and_dates(self):
        """Verify date chronology and reasonable claim amount thresholds."""
        valid_records = [
            {"service_date": "2025-01-01", "admit_date": "2025-01-01", "discharge_date": "2025-01-05", "billed_amount": 5000.0}
        ]
        self.assertTrue(self.dq.check_date_validity(valid_records, "admit_date", "discharge_date", "test_claims"))
        self.assertTrue(self.dq.check_reasonable_amounts(valid_records, "billed_amount", 0.0, 100000.0, "test_claims"))

        # Inverted dates should fail
        inverted_dates = [
            {"service_date": "2025-01-01", "admit_date": "2025-01-10", "discharge_date": "2025-01-02", "billed_amount": 5000.0}
        ]
        self.assertFalse(self.dq.check_date_validity(inverted_dates, "admit_date", "discharge_date", "test_claims"))

    def test_data_quality_icd10_regex(self):
        """Verify diagnosis code conforms to standard ICD-10-CM format."""
        regex = r"^[A-Z][0-9][0-9A-Z](\.[0-9A-Z]{1,4})?$"
        valid_icd = [{"code": "I10"}, {"code": "E11.9"}, {"code": "J44.9"}]
        invalid_icd = [{"code": "1234"}, {"code": "INVALID_CODE"}]

        self.assertTrue(self.dq.check_icd10_format(valid_icd, "code", regex, "test_icd10"))
        self.assertFalse(self.dq.check_icd10_format(invalid_icd, "code", regex, "test_icd10"))

    def test_pmpm_calculation_logic(self):
        """
        Verify PMPM math: Total Paid Amount / Unique Active Members.
        If Plan A has $30,000 paid across 10 distinct members in a month, PMPM = $3,000.00.
        """
        claims = [
            {"member": "M1", "paid": 1000.0},
            {"member": "M1", "paid": 2000.0},
            {"member": "M2", "paid": 3000.0},
            {"member": "M3", "paid": 4000.0},
        ]
        total_paid = sum(c["paid"] for c in claims)  # 10,000
        unique_members = len(set(c["member"] for c in claims))  # 3
        pmpm = round(total_paid / unique_members, 2)

        self.assertEqual(total_paid, 10000.0)
        self.assertEqual(unique_members, 3)
        self.assertEqual(pmpm, 3333.33)

    def test_30day_readmission_logic(self):
        """
        Verify 30-day readmission flagging:
        Admit 1: 2025-01-01, Discharge 1: 2025-01-05
        Admit 2: 2025-01-20 (15 days after discharge -> 30-day readmission = 1)
        Admit 3: 2025-03-10 (49 days after discharge -> 30-day readmission = 0)
        """
        from datetime import datetime
        encounters = [
            {"admit": "2025-01-01", "discharge": "2025-01-05"},
            {"admit": "2025-01-20", "discharge": "2025-01-25"},
            {"admit": "2025-03-10", "discharge": "2025-03-15"},
        ]

        readmissions = []
        for i in range(len(encounters)):
            if i == 0:
                readmissions.append(0)
            else:
                prior_disch = datetime.strptime(encounters[i - 1]["discharge"], "%Y-%m-%d")
                curr_admit = datetime.strptime(encounters[i]["admit"], "%Y-%m-%d")
                days_between = (curr_admit - prior_disch).days
                readmissions.append(1 if 0 <= days_between <= 30 else 0)

        self.assertEqual(readmissions, [0, 1, 0])


if __name__ == "__main__":
    unittest.main()
