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

