"""
Synthetic Healthcare Portfolio Project — PHI De-Identification Utility
Demonstrates Protected Health Information (PHI) de-identification and cryptographic
tokenization techniques inspired by healthcare industry standards.

NOTE: This is a student portfolio project using 100% synthetic data.
It demonstrates de-identification patterns and does NOT claim actual regulatory HIPAA compliance.

Key Engineering Highlights:
1. Deterministic Tokenization: Salted SHA-256 hashing allows linking patient records across
   claims and demographic datasets without exposing raw identifiers (MRN, SSN).
2. Native Spark SQL Execution: Uses native Spark functions (sha2 + concat) rather than Python
   UDFs to enable Catalyst whole-stage code generation and avoid Py4J serialization overhead.
"""

import hashlib

try:
    from pyspark.sql import Column
    from pyspark.sql import functions as F
    HAS_PYSPARK = True
except ImportError:
    HAS_PYSPARK = False


# -----------------------------------------------------------------------------
# PySpark Native Expressions (Catalyst Optimized)
# -----------------------------------------------------------------------------
def mask_phi_column(col_name: str, salt: str):
    """
    Creates a deterministic, salted SHA-256 cryptographic surrogate key
    for sensitive PHI identifiers (e.g. MRN, SSN, Patient Name).
    
    Using native Spark functions ensures Catalyst optimizer pushdown
    and Tungsten memory execution.
    """
    if not HAS_PYSPARK:
        raise RuntimeError("PySpark is required for mask_phi_column.")
    return F.sha2(F.concat(F.coalesce(F.col(col_name), F.lit("NULL_VAL")), F.lit(salt)), 256)


def mask_birth_date(dob_col: str):
    """
    Generalizes Date of Birth to Year of Birth to protect individual age specifics
    while preserving age-band demographic modeling capabilities.
    """
    if not HAS_PYSPARK:
        raise RuntimeError("PySpark is required for mask_birth_date.")
    return F.year(F.to_date(F.col(dob_col)))


def mask_phone_or_ssn_last4(col_name: str):
    """
    Masks identifier keeping only the last 4 digits (e.g., 'XXX-XX-1234').
    """
    if not HAS_PYSPARK:
        raise RuntimeError("PySpark is required for mask_phone_or_ssn_last4.")
    cleaned = F.regexp_replace(F.col(col_name), "[^0-9]", "")
    return F.concat(F.lit("XXX-XX-"), F.substring(cleaned, -4, 4))


# -----------------------------------------------------------------------------
# Pure Python Reference Functions (Used for validation, tests & standalone mode)
# -----------------------------------------------------------------------------
def hash_phi_value(value: str, salt: str) -> str:
    """
    Deterministic salted SHA-256 hashing for individual string values.
    Returns a 64-character hexadecimal digest.
    """
    if value is None:
        value = "NULL_VAL"
    combined = f"{value}{salt}".encode("utf-8")
    return hashlib.sha256(combined).hexdigest()


def mask_ssn_value(ssn: str) -> str:
    """
    Masks a string SSN keeping only the last 4 digits (e.g. 'XXX-XX-1234').
    """
    if not ssn:
        return "XXX-XX-0000"
    cleaned = "".join(ch for ch in str(ssn) if ch.isdigit())
    last4 = cleaned[-4:] if len(cleaned) >= 4 else "0000"
    return f"XXX-XX-{last4}"
