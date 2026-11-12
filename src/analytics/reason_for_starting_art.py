"""
Reason for starting ART (ART eligibility) per patient, from the HIV STAGING encounter.

Observations used (observation.concept_id -> answer in observation.value_coded):
  7563 'Reason for ART eligibility'   - one answer per patient (e.g. WHO stage, CD4, Option B+)
  2743 'Who stages criteria present'  - can have several answers per patient (staging conditions)

All joins are on patient_id + site_id (patient_id is only unique within a site).

  get_reason_for_art(site_id)                     -> all patients at a site, one row per observation
  get_reason_for_art(site_id, patient_id=1234)    -> one patient's records
  reason_for_art_summary(site_id[, patient_id])   -> one row per patient: reason for STARTING ART

Start dates come from src.functions.art_start_date:
  clinic_start_date       - first ARV dispensing at this site (regardless of earlier ART elsewhere)
  earliest_art_start_date - Ever received ART = Yes: 'ART start date' value_datetime on
                            HIV CLINIC REGISTRATION; = No: first ARVs on/after HIV staging;
                            always <= clinic_start_date

Patients can be staged several times, so the reason for starting is the latest
'Reason for ART eligibility' recorded ON earliest_art_start_date; if none that day,
the latest one recorded BEFORE it; if the patient was only staged AFTER it, the
earliest staging after it.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from src import settings
from src.functions.art_start_date import earliest_start_date

spark = SparkSession.builder.getOrCreate()

REASON_FOR_ART_CONCEPT_ID = 7563   # 'Reason for ART eligibility'
WHO_STAGE_CRITERIA_CONCEPT_ID = 2743  # 'Who stages criteria present'
HIV_STAGING_ENCOUNTER = "HIV STAGING"
ART_PROGRAM_ID = 1                 # 'HIV PROGRAM'


def _table(name: str) -> str:
    return f"{settings.TARGET_SCHEMA}.{name}"


def concept_names() -> DataFrame:
    """One English name per concept: locale_preferred first, then FULLY_SPECIFIED, then lowest id."""
    w = Window.partitionBy("concept_id").orderBy(
        F.col("locale_preferred").desc(),
        F.when(F.col("concept_name_type") == "FULLY_SPECIFIED", 0).otherwise(1),
        "concept_name_id",
    )
    return (
        spark.read.table(_table("concept_name"))
        .filter((F.col("locale") == "en") & (F.col("voided") == 0))
        .withColumn("_rn", F.row_number().over(w))
        .filter("_rn = 1")
        .select("concept_id", "name")
    )


def get_reason_for_art(site_id: int, patient_id: int = None) -> DataFrame:
    """
    Reason-for-ART observations for ART (program 1) patients at a site,
    one row per observation. Pass patient_id for a single patient.

    Columns: site_id, patient_id, encounter_id, obs_datetime, concept_id,
             question, value_coded, answer
    """
    if site_id is None:
        raise ValueError("site_id is required (patient_id is only unique within a site)")

    names = concept_names()

    obs = spark.read.table(_table("observation")).filter(
        (F.col("site_id") == site_id)
        & F.col("concept_id").isin(REASON_FOR_ART_CONCEPT_ID, WHO_STAGE_CRITERIA_CONCEPT_ID)
    )
    if patient_id is not None:
        obs = obs.filter(F.col("person_id") == patient_id)
    obs = obs.select(
        "site_id", F.col("person_id").alias("patient_id"), "encounter_id",
        "obs_datetime", "concept_id", "value_coded",
    )

    staging_type = spark.read.table(_table("encounter_type")).filter(
        F.upper(F.col("name")) == HIV_STAGING_ENCOUNTER
    )
    staging_encounters = (
        spark.read.table(_table("encounter"))
        .filter(F.col("site_id") == site_id)
        .join(staging_type, F.col("encounter_type") == F.col("encounter_type_id"), "inner")
        .select("encounter_id", "patient_id", "site_id")
    )

    art_patients = (
        spark.read.table(_table("patient_program"))
        .filter((F.col("site_id") == site_id) & (F.col("program_id") == ART_PROGRAM_ID))
        .select("patient_id", "site_id")
        .distinct()
    )

    return (
        obs.join(staging_encounters, ["encounter_id", "patient_id", "site_id"], "inner")
        .join(art_patients, ["patient_id", "site_id"], "inner")
        .join(names.withColumnRenamed("name", "question"), "concept_id", "left")
        .join(
            names.select(F.col("concept_id").alias("value_coded"), F.col("name").alias("answer")),
            "value_coded",
            "left",
        )
        .select(
            "site_id", "patient_id", "encounter_id", "obs_datetime",
            "concept_id", "question", "value_coded", "answer",
        )
    )


def reason_for_art_summary(
    site_id: int, patient_id: int = None, end_date: str = None
) -> DataFrame:
    """
    One row per patient who has received ARVs at the site (up to end_date; default today).
    Columns:
      site_id, patient_id
      ever_received_art_before    - 'Ever received ART?' at registration: Yes / No / Unknown /
                                    Other / NULL (not asked)
      earliest_art_start_date     - first ART anywhere (see src.functions.art_start_date);
                                    always <= clinic_start_date
      clinic_start_date           - first ARV dispensing at this site
      art_date_check              - earliest_art_start_date vs clinic_start_date:
                                    'Started before first ARVs at site (likely transfer in)' /
                                    'Same date' /
                                    'New initiation at site (Ever received ART = No)' /
                                    'Missing ART start date (Ever received ART = Yes)' /
                                    'Later date capped to first ARVs at site' (ART start date or
                                    HIV staging recorded after the first ARVs at the site)
      reason_for_art              - 'Reason for ART eligibility', chosen relative to
                                    earliest_art_start_date:
                                      1. latest one on that date
                                      2. else latest one before it
                                      3. else earliest one after it
                                    (NULL only if the patient was never staged)
      reason_date                 - obs_datetime of that answer
      days_from_clinic_start_date - reason date minus clinic_start_date (negative = before,
                                    0 = same day)
      who_stage_criteria          - 'Who stages criteria present' answers from the same
                                    staging encounter as the reason (sorted array)
    """
    # Patients who received ARVs at the site; reason is chosen relative to earliest_start_date
    starts = earliest_start_date(site_id, patient_id, end_date).withColumn(
        "arv_start_date", F.col("earliest_start_date")
    )
    obs = get_reason_for_art(site_id, patient_id).withColumn(
        "obs_date", F.to_date("obs_datetime")
    )

    # Priority 0: on/before ARV start -> latest wins (same-day records are the
    #             latest allowed, so they win automatically).
    # Priority 1: after ARV start (only if none on/before) -> earliest wins.
    on_or_before = F.col("obs_date") <= F.col("arv_start_date")
    obs_ts = F.to_timestamp("obs_datetime")
    w = Window.partitionBy("site_id", "patient_id").orderBy(
        F.when(on_or_before, 0).otherwise(1),
        F.when(on_or_before, obs_ts).desc_nulls_last(),
        F.when(~on_or_before, obs_ts).asc_nulls_last(),
        F.col("encounter_id").desc(),
    )
    reason = (
        obs.filter(F.col("concept_id") == REASON_FOR_ART_CONCEPT_ID)
        .join(starts, ["site_id", "patient_id"], "inner")
        .withColumn("_rn", F.row_number().over(w))
        .filter("_rn = 1")
        .select(
            "site_id",
            "patient_id",
            F.col("answer").alias("reason_for_art"),
            F.col("obs_datetime").alias("reason_date"),
            F.datediff("obs_date", "clinic_start_date").alias("days_from_clinic_start_date"),
            F.col("encounter_id").alias("reason_encounter_id"),
        )
    )

    criteria = (
        obs.filter(
            (F.col("concept_id") == WHO_STAGE_CRITERIA_CONCEPT_ID) & F.col("answer").isNotNull()
        )
        .groupBy("site_id", "patient_id", F.col("encounter_id").alias("reason_encounter_id"))
        .agg(F.array_sort(F.collect_set("answer")).alias("who_stage_criteria"))
    )

    # 'ART start date' is entered manually at registration when 'Ever received ART?' = Yes;
    # 'No' means a new initiation at this site.
    ever = F.col("ever_received_art")
    art_date_check = (
        F.when(F.col("earliest_start_source").endswith("capped to first ARVs at site"),
               "Later date capped to first ARVs at site")
        .when((ever == "Yes") & F.col("registration_art_start_date").isNull(),
              "Missing ART start date (Ever received ART = Yes)")
        .when(F.col("earliest_start_date") < F.col("clinic_start_date"),
              "Started before first ARVs at site (likely transfer in)")
        .when(ever == "No", "New initiation at site (Ever received ART = No)")
        .otherwise("Same date")
    )

    return (
        starts.join(reason, ["site_id", "patient_id"], "left")
        .join(criteria, ["site_id", "patient_id", "reason_encounter_id"], "left")
        .select(
            "site_id",
            "patient_id",
            F.col("ever_received_art").alias("ever_received_art_before"),
            F.col("earliest_start_date").alias("earliest_art_start_date"),
            "clinic_start_date",
            art_date_check.alias("art_date_check"),
            "reason_for_art",
            "reason_date",
            "days_from_clinic_start_date",
            "who_stage_criteria",
        )
    )


# Example (only when run directly, not on import)
if __name__ == "__main__":
    # All patients at a site (one row per patient)
    display(reason_for_art_summary(site_id=865))
    # Single patient, all observations
    # display(get_reason_for_art(site_id=865, patient_id=1234))
