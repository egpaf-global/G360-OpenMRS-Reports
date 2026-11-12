"""
ART start dates per patient at a site (all joins on patient_id + site_id).

  clinic_start_date   - date the patient FIRST received ARVs at this site, regardless
                        of whether they started ARVs somewhere else before
                        (pharmacy.arv_orders: ARV drug via arv_drug, quantity > 0).
  earliest_start_date - date the patient first started ART anywhere:
    1. 'Ever received ART?' = Yes on HIV CLINIC REGISTRATION and the 'ART start date'
       observation (concept 2516) has a value_datetime -> that value_datetime
       (entered manually at registration; usually a transfer in).
    2. 'Ever received ART?' = No -> first ARV dispensing at the site on or after the
       first HIV staging ('Reason for ART eligibility', concept 7563) date.
    3. Otherwise (not asked / Unknown / Yes without a date / No without staging):
       the registration 'ART start date' if present, else clinic_start_date.
    earliest_start_date is always <= clinic_start_date: a later candidate (e.g. staged
    only after the first ARVs at the site) is capped to clinic_start_date and flagged
    in earliest_start_source.

  ever_received_art(site_id[, patient_id])               -> Yes / No / Unknown / Other
  registration_art_start_date(site_id[, patient_id])     -> 'ART start date' value_datetime
  first_staging_date(site_id[, patient_id])              -> first 'Reason for ART eligibility' date
  patient_start_date(site_id[, patient_id, end_date])    -> clinic_start_date (first ARVs at site)
  earliest_start_date(site_id[, patient_id, end_date])   -> all of the above, one row per patient
"""

from datetime import date

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src import settings
from src.analytics.pharmacy import arv_orders

spark = SparkSession.builder.getOrCreate()

ART_START_DATE_CONCEPT_ID = 2516  # 'Date antiretrovirals started' (FULLY_SPECIFIED) / 'ART start date'
HIV_CLINIC_REGISTRATION = "HIV CLINIC REGISTRATION"  # encounter the ART start date is captured on
# 'ART start date' is entered manually at registration when 'Ever received ART?' = Yes
# (patient started ARVs before / elsewhere); 'No' = new initiation at this site.
EVER_RECEIVED_ART_CONCEPT_ID = 7754  # 'Ever received ART?'
YES_CONCEPT_ID = 1065
NO_CONCEPT_ID = 1066
UNKNOWN_CONCEPT_ID = 1067  # sometimes stored only as value_text '1067'
REASON_FOR_ART_CONCEPT_ID = 7563  # 'Reason for ART eligibility' (HIV STAGING)
HIV_STAGING = "HIV STAGING"


def _encounters(site_id: int, encounter_type_name: str) -> DataFrame:
    """Encounters of one type at a site (encounter_id, site_id, person_id)."""
    enc_type = spark.read.table(f"{settings.TARGET_SCHEMA}.encounter_type").filter(
        F.upper(F.col("name")) == encounter_type_name
    )
    return (
        spark.read.table(f"{settings.TARGET_SCHEMA}.encounter")
        .filter(F.col("site_id") == site_id)
        .join(enc_type, F.col("encounter_type") == F.col("encounter_type_id"), "inner")
        .select("encounter_id", "site_id", F.col("patient_id").alias("person_id"))
    )


def _registrations(site_id: int) -> DataFrame:
    """HIV CLINIC REGISTRATION encounters at a site (encounter_id, site_id, person_id)."""
    return _encounters(site_id, HIV_CLINIC_REGISTRATION)


def ever_received_art(site_id: int, patient_id: int = None) -> DataFrame:
    """
    First recorded 'Ever received ART?' answer per patient on an HIV CLINIC
    REGISTRATION encounter: 'Yes' / 'No' / 'Unknown' / 'Other'. The answer is
    value_coded, or the concept id stored in value_text when value_coded is empty.
    Patients never asked (common in older registrations) are not returned.
    """
    obs = spark.read.table(f"{settings.TARGET_SCHEMA}.observation").filter(
        (F.col("site_id") == site_id) & (F.col("concept_id") == EVER_RECEIVED_ART_CONCEPT_ID)
    )
    if patient_id is not None:
        obs = obs.filter(F.col("person_id") == patient_id)
    obs = obs.join(_registrations(site_id), ["encounter_id", "site_id", "person_id"], "inner")
    answer = F.coalesce(F.col("value_coded"), F.expr("try_cast(trim(value_text) AS BIGINT)"))

    w = Window.partitionBy("site_id", "person_id").orderBy(
        F.to_timestamp("obs_datetime"), "obs_id"
    )
    return (
        obs.withColumn("_rn", F.row_number().over(w))
        .filter("_rn = 1")
        .select(
            "site_id",
            F.col("person_id").alias("patient_id"),
            F.when(answer == YES_CONCEPT_ID, "Yes")
            .when(answer == NO_CONCEPT_ID, "No")
            .when(answer == UNKNOWN_CONCEPT_ID, "Unknown")
            .otherwise("Other")
            .alias("ever_received_art"),
        )
    )


def registration_art_start_date(site_id: int, patient_id: int = None) -> DataFrame:
    """
    First recorded 'ART start date' observation per patient on an HIV CLINIC
    REGISTRATION encounter (ordered by obs_datetime, obs_id).

    Columns: site_id, patient_id, registration_art_start_date (DATE(value_datetime), may be NULL)
    """
    obs = spark.read.table(f"{settings.TARGET_SCHEMA}.observation").filter(
        (F.col("site_id") == site_id)
        & (F.col("concept_id") == ART_START_DATE_CONCEPT_ID)
        & (F.col("encounter_id") > 0)
    )
    if patient_id is not None:
        obs = obs.filter(F.col("person_id") == patient_id)

    obs = obs.join(_registrations(site_id), ["encounter_id", "site_id", "person_id"], "inner")

    w = Window.partitionBy("site_id", "person_id").orderBy(
        F.to_timestamp("obs_datetime"), "obs_id"
    )
    return (
        obs.withColumn("_rn", F.row_number().over(w))
        .filter("_rn = 1")
        .select(
            "site_id",
            F.col("person_id").alias("patient_id"),
            F.to_date("value_datetime").alias("registration_art_start_date"),
        )
    )


def first_staging_date(site_id: int, patient_id: int = None) -> DataFrame:
    """First 'Reason for ART eligibility' date per patient on an HIV STAGING encounter."""
    obs = spark.read.table(f"{settings.TARGET_SCHEMA}.observation").filter(
        (F.col("site_id") == site_id) & (F.col("concept_id") == REASON_FOR_ART_CONCEPT_ID)
    )
    if patient_id is not None:
        obs = obs.filter(F.col("person_id") == patient_id)
    return (
        obs.join(_encounters(site_id, HIV_STAGING), ["encounter_id", "site_id", "person_id"], "inner")
        .groupBy("site_id", F.col("person_id").alias("patient_id"))
        .agg(F.min(F.to_date("obs_datetime")).alias("first_staging_date"))
    )


def _arv_dispensing_dates(site_id: int, patient_id: int = None, end_date: str = None) -> DataFrame:
    """Distinct ARV dispensing dates per patient at the site (up to end_date; default today)."""
    orders = arv_orders(end_date or date.today().isoformat(), site_id)
    if patient_id is not None:
        orders = orders.filter(F.col("patient_id") == patient_id)
    return orders.select(
        "site_id", "patient_id", F.to_date("start_date").alias("dispensed_date")
    ).distinct()


def patient_start_date(site_id: int, patient_id: int = None, end_date: str = None) -> DataFrame:
    """clinic_start_date: first ARV dispensing date per patient at the site."""
    return (
        _arv_dispensing_dates(site_id, patient_id, end_date)
        .groupBy("site_id", "patient_id")
        .agg(F.min("dispensed_date").alias("clinic_start_date"))
    )


def earliest_start_date(site_id: int, patient_id: int = None, end_date: str = None) -> DataFrame:
    """
    One row per patient who received ARVs at the site (up to end_date; default today).

    Columns: site_id, patient_id, ever_received_art, registration_art_start_date,
             first_staging_date, first_arv_after_staging, clinic_start_date,
             earliest_start_date, earliest_start_source:
               'ART start date (Ever received ART = Yes)'
               'First ARVs on/after HIV staging (Ever received ART = No)'
               'ART start date (registration)'      - not asked / Unknown, date recorded
               'First ARVs at site'                  - nothing else available
             with ' - capped to first ARVs at site' appended when the candidate was later
             than clinic_start_date.
    """
    dispensed = _arv_dispensing_dates(site_id, patient_id, end_date)
    clinic = dispensed.groupBy("site_id", "patient_id").agg(
        F.min("dispensed_date").alias("clinic_start_date")
    )
    staging = first_staging_date(site_id, patient_id)
    after_staging = (
        dispensed.join(staging, ["site_id", "patient_id"], "inner")
        .filter(F.col("dispensed_date") >= F.col("first_staging_date"))
        .groupBy("site_id", "patient_id")
        .agg(F.min("dispensed_date").alias("first_arv_after_staging"))
    )

    ever = F.col("ever_received_art")
    reg_date = F.col("registration_art_start_date")
    after = F.col("first_arv_after_staging")
    candidate_source = (
        F.when((ever == "Yes") & reg_date.isNotNull(), "ART start date (Ever received ART = Yes)")
        .when((ever == "No") & after.isNotNull(), "First ARVs on/after HIV staging (Ever received ART = No)")
        .when(reg_date.isNotNull() & ((ever != "No") | ever.isNull()), "ART start date (registration)")
        .otherwise("First ARVs at site")
    )
    candidate = (
        F.when((ever == "Yes") & reg_date.isNotNull(), reg_date)
        .when((ever == "No") & after.isNotNull(), after)
        .when(reg_date.isNotNull() & ((ever != "No") | ever.isNull()), reg_date)
        .otherwise(F.col("clinic_start_date"))
    )
    capped = F.col("_candidate") > F.col("clinic_start_date")

    return (
        clinic.join(ever_received_art(site_id, patient_id), ["site_id", "patient_id"], "left")
        .join(registration_art_start_date(site_id, patient_id), ["site_id", "patient_id"], "left")
        .join(staging, ["site_id", "patient_id"], "left")
        .join(after_staging, ["site_id", "patient_id"], "left")
        .withColumns({"_candidate": candidate, "_source": candidate_source})
        .withColumns(
            {
                "earliest_start_date": F.least("_candidate", "clinic_start_date"),
                "earliest_start_source": F.when(
                    capped, F.concat("_source", F.lit(" - capped to first ARVs at site"))
                ).otherwise(F.col("_source")),
            }
        )
        .select(
            "site_id", "patient_id", "ever_received_art", "registration_art_start_date",
            "first_staging_date", "first_arv_after_staging", "clinic_start_date",
            "earliest_start_date", "earliest_start_source",
        )
    )
