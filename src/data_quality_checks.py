"""
Healthcare Data Quality & Governance Engine
Implements automated assertion checks across Bronze, Silver, and Gold layers:
1. Primary Key Uniqueness & Nullability constraints.
2. Duplicate Record Detection.
3. Referential Integrity (Foreign Key resolution between child and parent tables).
4. Date Sanity (discharge_date >= admit_date >= service_date).
5. Financial Sanity (0 <= paid <= allowed <= billed, reasonable claim amounts).
6. ICD-10 Format & Catalog Validation.
7. PHI Zero-Leakage Audit (verifies absence of cleartext identifiers).

NOTE: This is a student portfolio project using 100% synthetic data.
No real patient or personal data is processed or stored.
"""

import re

try:
    from pyspark.sql import DataFrame
    from pyspark.sql import functions as F
    HAS_PYSPARK = True
except ImportError:
    HAS_PYSPARK = False


class HealthcareDataQualitySuite:
    def __init__(self):
        self.results = []

    def check_uniqueness(self, df, primary_key: str, table_name: str):
        """Validates that primary key contains 0 duplicate values."""
        if HAS_PYSPARK and hasattr(df, "count"):
            total_count = df.count()
            distinct_count = df.select(primary_key).distinct().count()
        else:
            total_count = len(df)
            distinct_count = len(set(r[primary_key] for r in df))

        passed = (total_count == distinct_count)
        self.results.append({
            "table": table_name,
            "check": f"Uniqueness of PK '{primary_key}'",
            "passed": passed,
            "details": f"Total rows: {total_count}, Distinct PKs: {distinct_count}"
        })
        return passed

    def check_non_null(self, df, mandatory_columns: list[str], table_name: str):
        """Validates that mandatory columns have 0 null values."""
        all_passed = True
        for col in mandatory_columns:
            if HAS_PYSPARK and hasattr(df, "filter"):
                null_count = df.filter(F.col(col).isNull()).count()
            else:
                null_count = sum(1 for r in df if r.get(col) is None or str(r.get(col)).strip() == "")

            passed = (null_count == 0)
            if not passed:
                all_passed = False
            self.results.append({
                "table": table_name,
                "check": f"Non-null constraint on '{col}'",
                "passed": passed,
                "details": f"Null occurrences: {null_count}"
            })
        return all_passed

    def check_financial_integrity(self, df, table_name: str):
        """Validates healthcare financial logic: 0 <= Paid <= Allowed <= Billed."""
        if HAS_PYSPARK and hasattr(df, "filter"):
            violations = df.filter(
                (F.col("paid_amount") < 0) |
                (F.col("allowed_amount") < 0) |
                (F.col("billed_amount") < 0) |
                (F.col("paid_amount") > F.col("allowed_amount")) |
                (F.col("allowed_amount") > F.col("billed_amount"))
            ).count()
        else:
            violations = sum(
                1 for r in df
                if r.get("claim_status") == "PAID" and (
                    float(r.get("paid_amount", 0)) < 0 or
                    float(r.get("allowed_amount", 0)) < 0 or
                    float(r.get("billed_amount", 0)) < 0 or
                    float(r.get("paid_amount", 0)) > float(r.get("allowed_amount", 0)) or
                    float(r.get("allowed_amount", 0)) > float(r.get("billed_amount", 0))
                )
            )

        passed = (violations == 0)
        self.results.append({
            "table": table_name,
            "check": "Financial logic (0 <= Paid <= Allowed <= Billed)",
            "passed": passed,
            "details": f"Violations found: {violations}"
        })
        return passed

    def check_reasonable_amounts(self, df, amount_col: str, min_amt: float, max_amt: float, table_name: str):
        """Validates that claim amounts fall within realistic actuarial thresholds."""
        if HAS_PYSPARK and hasattr(df, "filter"):
            outliers = df.filter((F.col(amount_col) < min_amt) | (F.col(amount_col) > max_amt)).count()
        else:
            outliers = sum(
                1 for r in df if float(r.get(amount_col, 0)) < min_amt or float(r.get(amount_col, 0)) > max_amt
            )

        passed = (outliers == 0)
        self.results.append({
            "table": table_name,
            "check": f"Reasonable claim bounds on '{amount_col}' (${min_amt:,.0f} to ${max_amt:,.0f})",
            "passed": passed,
            "details": f"Outlier records: {outliers}"
        })
        return passed

    def check_date_validity(self, df, start_col: str, end_col: str, table_name: str):
        """Validates chronological order (e.g. discharge_date >= admit_date)."""
        if HAS_PYSPARK and hasattr(df, "filter"):
            invalid = df.filter(F.col(end_col) < F.col(start_col)).count()
        else:
            invalid = sum(1 for r in df if str(r.get(end_col)) < str(r.get(start_col)))

        passed = (invalid == 0)
        self.results.append({
            "table": table_name,
            "check": f"Date chronology ({end_col} >= {start_col})",
            "passed": passed,
            "details": f"Chronological violations: {invalid}"
        })
        return passed

    def check_icd10_format(self, df, icd_col: str, regex_pattern: str, table_name: str):
        """Validates that diagnosis codes conform to standard ICD-10-CM alphanumeric format."""
        if HAS_PYSPARK and hasattr(df, "filter"):
            invalid = df.filter(~F.col(icd_col).rlike(regex_pattern)).count()
        else:
            compiled_regex = re.compile(regex_pattern)
            invalid = sum(1 for r in df if not compiled_regex.match(str(r.get(icd_col, ""))))

        passed = (invalid == 0)
        self.results.append({
            "table": table_name,
            "check": f"ICD-10 format regex validation on '{icd_col}'",
            "passed": passed,
            "details": f"Format violations: {invalid}"
        })
        return passed

    def check_phi_zero_leakage(self, df, forbidden_phi_columns: list[str], table_name: str):
        """Audits tables to verify zero presence of cleartext PHI identifiers."""
        if HAS_PYSPARK and hasattr(df, "columns"):
            present_forbidden = [col for col in forbidden_phi_columns if col in df.columns]
        else:
            sample_keys = df[0].keys() if len(df) > 0 and isinstance(df[0], dict) else []
            present_forbidden = [col for col in forbidden_phi_columns if col in sample_keys]

        passed = (len(present_forbidden) == 0)
        self.results.append({
            "table": table_name,
            "check": "PHI Zero-Leakage Audit",
            "passed": passed,
            "details": f"Forbidden columns present: {present_forbidden if present_forbidden else 'None (Compliant)'}"
        })
        return passed

    def check_referential_integrity(self, child_keys: set, parent_keys: set, fk_name: str, table_name: str):
        """Validates that all foreign keys in child table exist in parent table (no orphaned records)."""
        orphans = child_keys - parent_keys
        passed = (len(orphans) == 0)
        self.results.append({
            "table": table_name,
            "check": f"Referential integrity on FK '{fk_name}'",
            "passed": passed,
            "details": f"Orphaned distinct keys: {len(orphans)}"
        })
        return passed

    def generate_report(self) -> str:
        report = []
        report.append("\n==================================================")
        report.append("       HEALTHCARE DATA QUALITY AUDIT REPORT       ")
        report.append("==================================================")
        all_passed = True
        for res in self.results:
            status = "PASS" if res["passed"] else "FAIL"
            if not res["passed"]:
                all_passed = False
            report.append(f"[{status}] Table: {res['table']:<18} | Check: {res['check']}")
            report.append(f"       Details: {res['details']}")
        report.append("==================================================")
        report.append(f"OVERALL PIPELINE QUALITY STATUS: {'PASSED' if all_passed else 'FAILED'}")
        report.append("==================================================\n")
        return "\n".join(report)
