from pyspark.sql import functions as F
from pyspark.sql import Window
from src import settings


def current_pepfar_defaulter(
    patient_id: int, end_date: str, site_id: int
) -> int:
    """Calculates PEPFAR defaulter status (1 = Defaulter, 0 = Active) for a specific patient."""
    
    # 1. Read base tables
    orders_df = spark.table(f"{settings.TARGET_SCHEMA}.order")  # Ensure correct table name ('orders' vs 'order')
    drug_order_df = spark.table(f"{settings.TARGET_SCHEMA}.drug_order")
    arv_drug_df = spark.table(f"{settings.TARGET_SCHEMA}.arv_drug")

    # 2. Filter base orders down to target patient, site, and cutoff end_date
    filtered_orders = orders_df.filter(
        (F.col("patient_id") == patient_id)
        & (F.col("site_id") == site_id)
        & (F.col("start_date") <= end_date)
    )

    # 3. Join with drug_order and arv_drug
    dispensing_df = (
        drug_order_df.filter(
            (F.col("site_id") == site_id) & (F.col("quantity") > 0)
        )
        .join(filtered_orders, "order_id")
        .join(
            arv_drug_df,
            drug_order_df.drug_inventory_id == arv_drug_df.drug_id,
        )
    )

    # FIX 1: Correct empty DataFrame check
    if not dispensing_df.head(1):
        return 0

    # FIX 2: Wrap equivalent_daily_dose in F.first() inside .agg()
    aggregated_dispense = dispensing_df.groupBy(
        "patient_id",
        "drug_inventory_id",
        F.date_format("start_date", "yyyy-MM-dd").alias("start_date_day"),
    ).agg(
        F.max("start_date").alias("start_date"),
        F.sum("quantity").alias("total_quantity"),
        F.when(
            (F.first("equivalent_daily_dose").isNull())
            | (F.first("equivalent_daily_dose") <= 0),
            1.0,
        )
        .otherwise(F.first("equivalent_daily_dose"))
        .alias("daily_dose"),
    )

    # 5. Filter for ONLY the latest visit date
    latest_visit_window = Window.partitionBy("patient_id")
    latest_visit_df = aggregated_dispense.withColumn(
        "max_obs_datetime", F.max("start_date").over(latest_visit_window)
    ).filter(
        F.to_date("start_date") == F.to_date("max_obs_datetime")
    )

    # FIX 3: Cast days_supply to integer for date_add
    evaluated_df = latest_visit_df.withColumn(
        "days_supply", (F.col("total_quantity") / F.col("daily_dose")).cast("int")
    ).withColumn(
        "calculated_expiry",
        F.expr("date_add(date_sub(start_date, 1), days_supply)"),
    )

    # 7. Get minimum expiry date across drugs for that visit
    patient_summary = evaluated_df.groupBy("patient_id").agg(
        F.min("calculated_expiry").alias("min_expiry_date")
    )

    # 8. Check 30-day defaulter threshold
    result_df = patient_summary.withColumn(
        "is_defaulter",
        F.when(
            F.datediff(F.lit(end_date), F.col("min_expiry_date")) >= 30, 1
        ).otherwise(0),
    )

    res = result_df.collect()
    return res[0]["is_defaulter"] if res else 0


# Execution Example
flag = current_pepfar_defaulter(patient_id=1234, end_date="2026-08-01", site_id=865)
print(f"PEPFAR Defaulter Status: {flag}")