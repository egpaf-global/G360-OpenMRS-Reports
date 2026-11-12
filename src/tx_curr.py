"""
TX_CURR: patients Alive and On Treatment as of end_date, with age groups.

defaulter_days: 60 = MoH (default), 30 = PEPFAR (see settings.DEFAULTER_DAYS_*).
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from src import settings
from src.analytics.outcomes import get_alive_and_on_treatment
# Shared helpers (re-exported here so existing tx_curr.add_age_group etc. still work)
from src.functions.age_group import AGE_GROUPS, AGE_GROUP_ORDER, add_age_group
from src.functions.pregnancy import add_pregnancy_status

spark = SparkSession.builder.getOrCreate()

ALIVE_STATUS = "Alive and On Treatment"
# Age groups / genders always shown in the report grid (zeros filled in)
REPORT_AGE_GROUPS = AGE_GROUP_ORDER[:-1]
REPORT_GENDERS = ["Female", "Male"]
GENDER_ORDER = REPORT_GENDERS + ["Unknown"]

# MMD (multi-month dispensing) buckets, based on days of ARVs DISPENSED at the
# latest pickup = dispensed_quantity / daily_dose (pills brought back excluded).
MMD_LT3_MAX_DAYS = 90     # < 90 days        -> "<3 months"
MMD_6PLUS_MIN_DAYS = 180  # 90-179 days      -> "3-5 months"; >= 180 -> ">=6 months"
MMD_COLUMNS = {
    "<3 months": "# of clients on <3 months of ARVs",
    "3-5 months": "# of clients on 3 - 5 months of ARVs",
    ">=6 months": "# of clients on >= 6 months of ARVs",
}

# Summary rows appended to the report (pregnant takes precedence over breastfeeding)
PB_SUMMARY_ROWS = ["Male", "FP", "FNP", "FBF"]  # All Male, Female Pregnant / Not Pregnant / Breastfeeding

tables = {
    item["table"]: f"{settings.TARGET_SCHEMA}.{item['table']}"
    for item in settings.TRANSACTION_TABLES
}


def patient_program(site_id: int = None):
    pp = spark.read.table(tables["patient_program"])
    """returns patients within selected site_id"""
    if site_id:
        return pp.filter(F.col("site_id") == site_id)
    
    return pp


def patients(site_id: int = None):
    """Returns distinct patients enrolled in program_id = 1 

    with selected demography attributes.
    """
    p = spark.read.table(tables["patient"])
    pp = patient_program(site_id).filter(F.col("program_id") == 1)

    return (
        p.join(pp, ["patient_id", "site_id"], "inner")
        .dropDuplicates(["patient_id", "site_id"])
        .select("birthdate", "birthdate_estimated", "gender", "site_id")
    )


def validate_period(start_date: str, end_date: str) -> tuple:
    """Validates a reporting period (YYYY-MM-DD strings, start_date <= end_date)."""
    from datetime import date

    try:
        start, end = date.fromisoformat(start_date), date.fromisoformat(end_date)
    except (TypeError, ValueError):
        raise ValueError(
            f"start_date and end_date must be YYYY-MM-DD; got {start_date!r}, {end_date!r}"
        )
    if start > end:
        raise ValueError(f"start_date {start_date} is after end_date {end_date}")
    return start.isoformat(), end.isoformat()


def first_art_enrolment(site_id: int) -> DataFrame:
    """First ART (program_id = 1) enrolment date per patient at a site."""
    return (
        patient_program(site_id)
        .filter(F.col("program_id") == 1)
        .groupBy("patient_id", "site_id")
        .agg(F.min(F.to_date("date_enrolled")).alias("first_enrolled_date"))
    )


def tx_curr(
    start_date: str,
    end_date: str,
    site_id: int,
    defaulter_days: int = settings.DEFAULT_DEFAULTER_DAYS,
    enrolled_in_period: bool = False,
) -> DataFrame:
    """
    Patient-level TX_CURR for the reporting period start_date..end_date.

    enrolled_in_period=False (default): all patients Alive and On Treatment at
        the end of the reporting period (end_date), regardless of when they
        enrolled. This is the TX_CURR / TX_CURR MMD definition.
    enrolled_in_period=True: optional cohort view - only patients whose FIRST ART
        enrolment (patient_program.date_enrolled, program 1) falls between
        start_date and end_date (inclusive) and who are Alive at end_date.

    Status, age and MMD are evaluated as of end_date.
    defaulter_days: 60 = MoH (default), 30 = PEPFAR.
    """
    start_date, end_date = validate_period(start_date, end_date)
    defaulter_days = settings.validate_defaulter_days(defaulter_days)

    alive = get_alive_and_on_treatment(end_date, site_id, defaulter_days).filter(
        F.col("patient_status") == ALIVE_STATUS
    )

    alive = alive.join(first_art_enrolment(site_id), ["patient_id", "site_id"], "left")
    if enrolled_in_period:
        alive = alive.filter(
            F.col("first_enrolled_date").between(
                F.to_date(F.lit(start_date)), F.to_date(F.lit(end_date))
            )
        )

    p = (
        spark.read.table(tables["patient"])
        .filter(F.col("site_id") == site_id)
        .select("patient_id", "site_id", "gender", "birthdate", "birthdate_estimated")
        .dropDuplicates(["patient_id", "site_id"])
        # Normalise gender: data has both M/F and Male/Female
        .withColumn(
            "gender",
            F.when(F.upper(F.trim("gender")).isin("M", "MALE"), "Male")
            .when(F.upper(F.trim("gender")).isin("F", "FEMALE"), "Female")
            .otherwise("Unknown"),
        )
    )

    df = alive.join(p, ["patient_id", "site_id"], "left")
    df = add_age_group(df, end_date)
    df = add_pregnancy_status(df, site_id)

    # MMD: days of ARVs dispensed at latest pickup (excludes pills brought back)
    days_dispensed = F.floor(F.col("dispensed_quantity") / F.col("daily_dose"))
    return df.withColumns(
        {
            "days_dispensed": days_dispensed,
            "mmd_category": F.when(days_dispensed.isNull(), "Unknown")
            .when(days_dispensed < MMD_LT3_MAX_DAYS, "<3 months")
            .when(days_dispensed < MMD_6PLUS_MIN_DAYS, "3-5 months")
            .otherwise(">=6 months"),
            "defaulter_days": F.lit(defaulter_days),
            "start_date": F.lit(start_date),
            "end_date": F.lit(end_date),
            "enrolled_in_period": F.lit(enrolled_in_period),
        }
    )


def tx_curr_by_age_group(
    start_date: str,
    end_date: str,
    site_id: int,
    defaulter_days: int = settings.DEFAULT_DEFAULTER_DAYS,
    enrolled_in_period: bool = False,
) -> DataFrame:
    """
    TX_CURR report by gender and age_group with MMD columns for the period
    start_date..end_date (status evaluated as of end_date).
    enrolled_in_period: see tx_curr() (default False = all patients alive on ART at end_date).

    Every standard age group is listed for Female and Male (zeros filled in);
    Unknown age/gender rows are appended only if they have patients.
    Summary rows (Age group 'All') follow, as in the EMR export:
    All Male, All FP (pregnant), All FNP (not pregnant/breastfeeding), All FBF (breastfeeding).
    FP + FNP + FBF = all Female rows.
    tx_curr = sum of the three MMD columns (plus any patients with unknown MMD).
    """
    patients_df = tx_curr(start_date, end_date, site_id, defaulter_days, enrolled_in_period)
    aggs = [
        F.countDistinct("patient_id").alias("tx_curr"),
        *[
            F.countDistinct(
                F.when(F.col("mmd_category") == cat, F.col("patient_id"))
            ).alias(col)
            for cat, col in MMD_COLUMNS.items()
        ],
    ]
    counts = patients_df.groupBy("age_group", "gender").agg(*aggs)

    # Summary rows: All Male + Female by pregnancy status (zeros filled in)
    summary = (
        patients_df.withColumn(
            "summary_group",
            F.when(F.col("gender") == "Male", "Male").otherwise(F.col("pregnancy_status")),
        )
        .filter(F.col("summary_group").isNotNull())
        .groupBy(F.col("summary_group").alias("gender"))
        .agg(*aggs)
    )
    summary = (
        spark.createDataFrame([(g,) for g in PB_SUMMARY_ROWS], "gender string")
        .join(summary, "gender", "left")
        .fillna(0, subset=["tx_curr", *MMD_COLUMNS.values()])
        .withColumns({"age_group": F.lit("All"), "_g": F.lit(len(GENDER_ORDER))})
    )
    summary_order = F.create_map(
        *[x for i, g in enumerate(PB_SUMMARY_ROWS) for x in (F.lit(g), F.lit(i))]
    )
    summary = summary.withColumn("_a", summary_order[F.col("gender")])

    grid = spark.createDataFrame(
        [(a, g) for g in REPORT_GENDERS for a in REPORT_AGE_GROUPS],
        "age_group string, gender string",
    )
    # Full grid with zeros + any non-standard (Unknown) rows that have patients
    report = grid.join(counts, ["age_group", "gender"], "full").fillna(
        0, subset=["tx_curr", *MMD_COLUMNS.values()]
    )

    age_order = F.create_map(
        *[x for i, g in enumerate(AGE_GROUP_ORDER) for x in (F.lit(g), F.lit(i))]
    )
    gender_order = F.create_map(
        *[x for i, g in enumerate(GENDER_ORDER) for x in (F.lit(g), F.lit(i))]
    )
    report = report.withColumns(
        {
            "_g": gender_order[F.col("gender")],
            "_a": age_order[F.col("age_group")],
        }
    ).unionByName(summary)

    return (
        report.withColumns(
            {
                "site_id": F.lit(site_id),
                "start_date": F.lit(start_date),
                "end_date": F.lit(end_date),
            }
        )
        .orderBy("_g", "_a")
        .select(
            "site_id",
            "start_date",
            "end_date",
            F.col("age_group").alias("Age group"),
            F.col("gender").alias("Gender"),
            *MMD_COLUMNS.values(),
            F.col("tx_curr").alias("TX_CURR"),
        )
    )


# Example (only when run directly, not on import)
if __name__ == "__main__":
    #display(tx_curr_by_age_group("2000-01-01", "2025-03-31", site_id=865))  # MoH, 60 days (default)
    display(tx_curr_by_age_group("2000-01-01", "2025-03-31", site_id=865, defaulter_days=settings.DEFAULTER_DAYS_PEPFAR))  # PEPFAR, 30 days
    #display(tx_curr_by_age_group("2025-01-01", "2025-03-31", site_id=865, enrolled_in_period=False))  # all patients on ART at end_date