"""
Age group helpers (Malawi EMR / PEPFAR age bands).

age_in_months = timestampdiff(month, birthdate, as_of_date)
age_in_years  = timestampdiff(year,  birthdate, as_of_date)
  <1 year       : age_in_months 0-11
  1-4 ... 85-89 : age_in_years in 5-year bands
  90 plus years : age_in_years >= 90
  Unknown       : missing birthdate or birthdate after as_of_date
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# Ordered (label, min_years, max_years); "<1 year" is handled by age_in_months.
AGE_GROUPS = [
    ("1-4 years", 1, 4),
    ("5-9 years", 5, 9),
    ("10-14 years", 10, 14),
    ("15-19 years", 15, 19),
    ("20-24 years", 20, 24),
    ("25-29 years", 25, 29),
    ("30-34 years", 30, 34),
    ("35-39 years", 35, 39),
    ("40-44 years", 40, 44),
    ("45-49 years", 45, 49),
    ("50-54 years", 50, 54),
    ("55-59 years", 55, 59),
    ("60-64 years", 60, 64),
    ("65-69 years", 65, 69),
    ("70-74 years", 70, 74),
    ("75-79 years", 75, 79),
    ("80-84 years", 80, 84),
    ("85-89 years", 85, 89),
]
AGE_GROUP_ORDER = (
    ["<1 year"] + [g[0] for g in AGE_GROUPS] + ["90 plus years", "Unknown"]
)


def add_age_group(df: DataFrame, as_of_date: str, birthdate_col: str = "birthdate") -> DataFrame:
    """
    Adds age_in_months, age_in_years and age_group, with age calculated as of
    as_of_date (YYYY-MM-DD, typically the reporting end_date).
    """
    months = F.expr(f"timestampdiff(MONTH, to_date({birthdate_col}), to_date('{as_of_date}'))")
    years = F.expr(f"timestampdiff(YEAR, to_date({birthdate_col}), to_date('{as_of_date}'))")

    df = df.withColumns({"age_in_months": months, "age_in_years": years})

    age_group = F.when(
        (F.col("age_in_months") >= 0) & (F.col("age_in_months") <= 11), "<1 year"
    )
    for label, lo, hi in AGE_GROUPS:
        age_group = age_group.when(
            (F.col("age_in_years") >= lo) & (F.col("age_in_years") <= hi), label
        )
    age_group = age_group.when(F.col("age_in_years") >= 90, "90 plus years").otherwise("Unknown")

    return df.withColumn("age_group", age_group)
