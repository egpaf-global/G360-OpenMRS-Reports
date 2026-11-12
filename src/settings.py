"""
Application configuration.

All application-wide configuration should be defined here.
"""


# ============================================================
# Databricks Unity Catalog
# ============================================================

CATALOG = "programsdev"
SCHEMA = "malawi"

TARGET_SCHEMA = f"{CATALOG}.{SCHEMA}"


# ============================================================
# Defaulter thresholds (days after ARV run-out date)
# Malawi team agreement: round 56 -> 60 (MoH) and 28 -> 30 (PEPFAR).
# A patient is a defaulter when run-out date + N days < end_date.
# ============================================================

DEFAULTER_DAYS_MOH = 60
DEFAULTER_DAYS_PEPFAR = 30
DEFAULT_DEFAULTER_DAYS = DEFAULTER_DAYS_MOH
ALLOWED_DEFAULTER_DAYS = (DEFAULTER_DAYS_PEPFAR, DEFAULTER_DAYS_MOH)


def validate_defaulter_days(defaulter_days: int) -> int:
    """Ensure the defaulter threshold is one of the agreed values (30 PEPFAR / 60 MoH)."""
    if defaulter_days not in ALLOWED_DEFAULTER_DAYS:
        raise ValueError(
            f"defaulter_days must be one of {ALLOWED_DEFAULTER_DAYS} "
            f"(PEPFAR={DEFAULTER_DAYS_PEPFAR}, MoH={DEFAULTER_DAYS_MOH}); got {defaulter_days}"
        )
    return int(defaulter_days)



# ============================================================
# Transaction Tables
# ============================================================

TRANSACTION_TABLES = [
    {
        "table": "patient",
        "primary_key": "patient_id"
    },
    {
        "table": "encounter",
        "primary_key": "encounter_id"
    },
    {
        "table": "patient_program",
        "primary_key": "patient_program_id"
    },
    {
        "table": "order",
        "primary_key": "order_id"
    },
    {
        "table": "drug_order",
        "primary_key": "order_id"
    },
    {
        "table": "observation",
        "primary_key": "obs_id"
    }
]

