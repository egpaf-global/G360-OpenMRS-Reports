"""
Patient ARV regimen (all joins on patient_id + site_id).

Regimen at a date = regimen of the LATEST ARV dispensation on or before that date:
  1. ARV orders from pharmacy.arv_orders ('drug order' type, ARV via arv_drug,
     quantity > 0, HIV program patients).
  2. Take every ARV drug dispensed on the latest dispensing date <= date.
  3. Regimen lookup = programsdev.malawi.regimen (one row per regimen_name x
     drug_inventory_id, voided = 0).
  4. A regimen_name is VALID when
       - the number of distinct ARV drug_inventory_id dispensed on that date
         = num_of_drug_combination, and
       - every one of those drugs is listed for that regimen_name.
     Otherwise regimen_name is NULL (regimen_status = 'No matching regimen').

  latest_regimen(site_id, end_date[, patient_id])  -> DataFrame, one row per patient
  patient_regimen(patient_id, site_id, date)       -> regimen_name (str) or None
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from src import settings
from src.analytics.pharmacy import arv_orders

spark = SparkSession.builder.getOrCreate()


def _regimen_lookup() -> DataFrame:
    """Active regimen lookup: regimen_name, num_of_drug_combination, drug_inventory_id."""
    return (
        spark.read.table(f"{settings.TARGET_SCHEMA}.regimen")
        .filter(F.coalesce(F.col("voided"), F.lit(0)) == 0)
        .select("regimen_name", "num_of_drug_combination", "drug_inventory_id")
        .distinct()
    )


def latest_regimen(site_id: int, end_date: str, patient_id: int = None) -> DataFrame:
    """
    One row per patient with ARVs dispensed at the site on or before end_date.

    Columns: site_id, patient_id, last_dispensed_date, arv_drugs, n_drugs,
             regimen_name, regimen_status:
               'Valid regimen'
               'No matching regimen'
               'Multiple regimens match' (names joined with '/')
    """
    orders = arv_orders(end_date, site_id)
    if patient_id is not None:
        orders = orders.filter(F.col("patient_id") == patient_id)
    orders = orders.withColumn("dispensed_date", F.to_date("start_date"))

    # Latest dispensing date per patient (<= end_date already applied by arv_orders)
    last_date = orders.groupBy("site_id", "patient_id").agg(
        F.max("dispensed_date").alias("dispensed_date")
    )

    drug = spark.read.table(f"{settings.TARGET_SCHEMA}.drug").select(
        F.col("drug_id").alias("drug_inventory_id"),
        F.col("name").alias("drug_name"),
    )
    lookup = _regimen_lookup()

    # Distinct ARV drugs on the latest dispensing date
    last_drugs = (
        orders.join(last_date, ["site_id", "patient_id", "dispensed_date"], "inner")
        .select("site_id", "patient_id", "dispensed_date", "drug_inventory_id")
        .distinct()
    )

    per_patient = (
        last_drugs.join(drug, "drug_inventory_id", "left")
        .groupBy("site_id", "patient_id")
        .agg(
            F.max("dispensed_date").alias("last_dispensed_date"),
            F.array_sort(F.collect_set("drug_name")).alias("arv_drugs"),
            F.countDistinct("drug_inventory_id").alias("n_drugs"),
        )
    )

    # Candidate regimens: how many of the patient's drugs each regimen lists
    candidates = (
        last_drugs.join(lookup, "drug_inventory_id", "inner")
        .groupBy("site_id", "patient_id", "regimen_name", "num_of_drug_combination")
        .agg(F.countDistinct("drug_inventory_id").alias("n_matched"))
    )

    # Valid: all dispensed drugs belong to the regimen AND count = num_of_drug_combination
    valid = (
        candidates.join(per_patient.select("site_id", "patient_id", "n_drugs"), ["site_id", "patient_id"])
        .filter(
            (F.col("n_matched") == F.col("n_drugs"))
            & (F.col("num_of_drug_combination") == F.col("n_drugs"))
        )
        .groupBy("site_id", "patient_id")
        .agg(F.array_sort(F.collect_set("regimen_name")).alias("regimens"))
    )

    return (
        per_patient.join(valid, ["site_id", "patient_id"], "left")
        .select(
            "site_id",
            "patient_id",
            "last_dispensed_date",
            "arv_drugs",
            "n_drugs",
            F.array_join("regimens", "/").alias("regimen_name"),
            F.when(F.col("regimens").isNull(), "No matching regimen")
            .when(F.size("regimens") > 1, "Multiple regimens match")
            .otherwise("Valid regimen")
            .alias("regimen_status"),
        )
    )


def patient_regimen(patient_id: int, site_id: int, date: str):
    """
    regimen_name of the patient's latest ARV dispensation on or before date
    (e.g. '13A', '5A'), or None if no ARVs dispensed / no valid regimen.
    """
    row = latest_regimen(site_id, date, patient_id).select("regimen_name").first()
    return row["regimen_name"] if row else None


# Example (only when run directly, not on import)
if __name__ == "__main__":
    display(latest_regimen(574, "2026-06-30"))
