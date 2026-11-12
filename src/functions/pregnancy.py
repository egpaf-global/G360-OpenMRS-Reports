"""
Pregnant / breastfeeding status (Spark port of the EMR pregnant_breastfeeding?
logic), scoped by patient_id + site_id.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from src import settings

spark = SparkSession.builder.getOrCreate()

HIV_CLINIC_CONSULTATION = "HIV CLINIC CONSULTATION"
PREGNANT_CONCEPT_ID = 6131       # 'Is patient pregnant?'
BREASTFEEDING_CONCEPT_ID = 7965  # 'Is patient breast feeding?'
YES_CONCEPT_ID = 1065            # 'Yes' (standard OpenMRS id; no concept_name row in this copy)

FEMALE_PREGNANT = "FP"
FEMALE_BREASTFEEDING = "FBF"
FEMALE_NOT_PREGNANT = "FNP"


def add_pregnancy_status(
    df: DataFrame,
    site_id: int,
    visit_date_col: str = "drug_dispensed_date",
    gender_col: str = "gender",
    female_value: str = "Female",
) -> DataFrame:
    """
    Adds pregnancy_status for females: FP (pregnant), FBF (breastfeeding), FNP (neither).
    Non-female rows get NULL.

    df must have patient_id, site_id, gender_col and visit_date_col
    (the EMR's passed_date; default = latest ARV dispensing date).

    Logic (per patient_id + site_id):
      1. Latest HIV CLINIC CONSULTATION encounter on the visit date.
      2. 'Is patient pregnant?' / 'Is patient breast feeding?' obs on that
         encounter and date.
      3. Flag only when the answer is Yes (value_coded = 1065).
         (The EMR Ruby uses `=` (assignment), which would flag any answer;
         Yes-only was validated against the site e-Mastercard export.)
    Pregnant takes precedence when both are Yes.
    """
    visits = df.filter(F.col(gender_col) == female_value).select(
        "patient_id", "site_id", F.to_date(visit_date_col).alias("visit_date")
    ).distinct()

    enc_type = spark.read.table(f"{settings.TARGET_SCHEMA}.encounter_type").filter(
        F.col("name") == HIV_CLINIC_CONSULTATION
    )
    consultations = (
        spark.read.table(f"{settings.TARGET_SCHEMA}.encounter")
        .filter(F.col("site_id") == site_id)
        .join(enc_type, F.col("encounter_type") == F.col("encounter_type_id"), "inner")
        .select(
            "patient_id", "site_id", "encounter_id", "encounter_datetime",
            F.to_date("encounter_datetime").alias("visit_date"),
        )
    )
    latest_consult = (
        visits.join(consultations, ["patient_id", "site_id", "visit_date"], "inner")
        .groupBy("patient_id", "site_id", "visit_date")
        .agg(F.max_by("encounter_id", "encounter_datetime").alias("encounter_id"))
    )

    obs = (
        spark.read.table(f"{settings.TARGET_SCHEMA}.observation")
        .filter(
            (F.col("site_id") == site_id)
            & F.col("concept_id").isin(PREGNANT_CONCEPT_ID, BREASTFEEDING_CONCEPT_ID)
        )
        .select(
            F.col("person_id").alias("patient_id"), "site_id", "encounter_id",
            "concept_id", "value_coded", F.to_date("obs_datetime").alias("visit_date"),
        )
    )
    is_yes = F.col("value_coded") == YES_CONCEPT_ID
    flags = (
        latest_consult.join(obs, ["patient_id", "site_id", "encounter_id", "visit_date"], "inner")
        .groupBy("patient_id", "site_id")
        .agg(
            F.max(F.when((F.col("concept_id") == PREGNANT_CONCEPT_ID) & is_yes, 1).otherwise(0)).alias("is_pregnant"),
            F.max(F.when((F.col("concept_id") == BREASTFEEDING_CONCEPT_ID) & is_yes, 1).otherwise(0)).alias("is_breastfeeding"),
        )
    )

    return (
        df.join(flags, ["patient_id", "site_id"], "left")
        .withColumn(
            "pregnancy_status",
            F.when(F.col(gender_col) != female_value, F.lit(None).cast("string"))
            .when(F.col("is_pregnant") == 1, FEMALE_PREGNANT)
            .when(F.col("is_breastfeeding") == 1, FEMALE_BREASTFEEDING)
            .otherwise(FEMALE_NOT_PREGNANT),
        )
        .drop("is_pregnant", "is_breastfeeding")
    )
