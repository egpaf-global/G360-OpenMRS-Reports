"""
Pharmacy utilities.

Contains reusable pharmacy calculations for HIV indicators.
"""

from pyspark.sql import SparkSession, DataFrame

from src import settings

spark = SparkSession.builder.getOrCreate()

tables = {
    item["table"]: f"{settings.TARGET_SCHEMA}.{item['table']}"
    for item in settings.TRANSACTION_TABLES
}


def arv_orders(end_date: str, site_id: int) -> DataFrame:
    """
    Returns every ARV order up to the reporting date.

    One record per drug order.
    """

    order_table = tables["order"]
    drug_order_table = tables["drug_order"]
    order_type_table = f"{settings.TARGET_SCHEMA}.order_type"
    arv_drug_table = f"{settings.TARGET_SCHEMA}.arv_drug"
    patient_program_table = tables["patient_program"]

    query = f"""
    SELECT
        o.patient_id,
        o.site_id,
        o.order_id,
        do.drug_inventory_id,
        o.start_date,
        CAST(do.quantity AS DOUBLE) AS quantity,
        COALESCE(
            NULLIF(do.equivalent_daily_dose, 0),
            1
        ) AS daily_dose
    FROM {order_table} o
    INNER JOIN {drug_order_table} do
        ON o.order_id = do.order_id
       AND o.site_id = do.site_id
    INNER JOIN {arv_drug_table} ad
        ON do.drug_inventory_id = ad.drug_id
    INNER JOIN {order_type_table} ot
        ON o.order_type_id = ot.order_type_id
    INNER JOIN (
        SELECT DISTINCT
            patient_id,
            site_id
        FROM {patient_program_table}
        WHERE program_id = 1
    ) pp
        ON pp.patient_id = o.patient_id
       AND pp.site_id = o.site_id
    WHERE
        o.site_id = {site_id}
        AND LOWER(ot.name) = 'drug order'
        AND do.quantity > 0
        AND DATE(o.start_date) <= DATE('{end_date}')
    ORDER BY
        patient_id,
        start_date,
        order_id
    """

    return spark.sql(query)


def pill_count(end_date: str, site_id: int) -> DataFrame:
    """
    Spark equivalent of OpenMRS drug_pill_count().

    Returns one row per:
        patient
        drug
        visit_date
    """

    observation_table = tables["observation"]
    drug_order_table = tables["drug_order"]
    encounter_table = tables["encounter"]
    drug_table = f"{settings.TARGET_SCHEMA}.drug"
    encounter_type_table = f"{settings.TARGET_SCHEMA}.encounter_type"

    query = f"""
    WITH order_remaining AS (
        SELECT
            ob.person_id AS patient_id,
            do.drug_inventory_id,
            ob.site_id,
            DATE(ob.obs_datetime) AS visit_date,
            SUM(
                COALESCE(
                    ob.value_numeric,
                    0
                )
            ) AS numeric_remaining,
            SUM(
                CASE
                    WHEN ob.value_text RLIKE '^[0-9]+(\\.[0-9]+)?$'
                    THEN CAST(
                        ob.value_text AS DOUBLE
                    )
                    ELSE 0
                END
            ) AS text_remaining
        FROM {observation_table} ob
        INNER JOIN {drug_order_table} do
            ON ob.order_id = do.order_id
           AND ob.site_id = do.site_id
        INNER JOIN {tables["order"]} o
            ON do.order_id = o.order_id
           AND do.site_id = o.site_id
        WHERE
            ob.site_id = {site_id}
            AND ob.person_id = o.patient_id
            AND ob.concept_id = 2540
            AND DATE(ob.obs_datetime) <= DATE('{end_date}')
        GROUP BY
            ob.person_id,
            do.drug_inventory_id,
            ob.site_id,
            DATE(ob.obs_datetime)
    ),

    drug_remaining AS (
        SELECT
            ob.person_id AS patient_id,
            d.drug_id AS drug_inventory_id,
            ob.site_id,
            DATE(ob.obs_datetime) AS visit_date,
            SUM(
                COALESCE(
                    ob.value_numeric,
                    0
                )
            ) AS drug_remaining
        FROM {observation_table} ob
        INNER JOIN {drug_table} d
            ON ob.concept_id = d.concept_id
        WHERE
            ob.site_id = {site_id}
            AND DATE(ob.obs_datetime) <= DATE('{end_date}')
        GROUP BY
            ob.person_id,
            d.drug_id,
            ob.site_id,
            DATE(ob.obs_datetime)
    ),

    transfer_in AS (
        SELECT
            ob.person_id AS patient_id,
            ob.value_drug AS drug_inventory_id,
            ob.site_id,
            DATE(ob.obs_datetime) AS visit_date,
            SUM(
                COALESCE(
                    ob.value_numeric,
                    0
                )
            ) AS transfer_remaining
        FROM {observation_table} ob
        INNER JOIN {encounter_table} e
            ON ob.encounter_id = e.encounter_id
           AND ob.site_id = e.site_id
        INNER JOIN {encounter_type_table} et
            ON e.encounter_type = et.encounter_type_id
        WHERE
            ob.site_id = {site_id}
            AND et.name = 'HIV CLINIC CONSULTATION'
            AND ob.concept_id = 2540
            AND DATE(ob.obs_datetime) <= DATE('{end_date}')
        GROUP BY
            ob.person_id,
            ob.value_drug,
            ob.site_id,
            DATE(ob.obs_datetime)
    )

    SELECT
        o.patient_id,
        o.drug_inventory_id,
        o.site_id,
        o.visit_date,
        COALESCE(o.numeric_remaining, 0) AS numeric_remaining,
        COALESCE(o.text_remaining, 0) AS text_remaining,
        COALESCE(d.drug_remaining, 0) AS drug_remaining,
        COALESCE(t.transfer_remaining, 0) AS transfer_remaining,
        (
            COALESCE(o.numeric_remaining, 0) +
            COALESCE(o.text_remaining, 0) +
            COALESCE(d.drug_remaining, 0) +
            COALESCE(t.transfer_remaining, 0)
        ) AS pill_count
    FROM order_remaining o
    LEFT JOIN drug_remaining d
        ON o.patient_id = d.patient_id
       AND o.drug_inventory_id = d.drug_inventory_id
       AND o.site_id = d.site_id
       AND o.visit_date = d.visit_date
    LEFT JOIN transfer_in t
        ON o.patient_id = t.patient_id
       AND o.drug_inventory_id = t.drug_inventory_id
       AND o.site_id = t.site_id
       AND o.visit_date = t.visit_date
    ORDER BY
        patient_id,
        visit_date,
        drug_inventory_id
    """

    return spark.sql(query)


def arv_dispensing(end_date: str, site_id: int):
    # 1. Execute Python functions to get DataFrames
    orders_df = arv_orders(end_date, site_id)
    pill_df = pill_count(end_date, site_id)

    # 2. Register DataFrames as temporary views
    orders_df.createOrReplaceTempView("tmp_arv_orders")
    pill_df.createOrReplaceTempView("tmp_pill_count")

    # 3. Query the temporary views directly
    query = """
    SELECT
        o.patient_id,
        o.site_id,
        o.order_id,
        o.drug_inventory_id,
        o.start_date,
        o.quantity,
        p.pill_count,
        o.daily_dose,
        (o.quantity + COALESCE(p.pill_count, 0)) AS effective_quantity,
        ((o.quantity + COALESCE(p.pill_count, 0)) / o.daily_dose) AS days_of_supply,
        DATE_ADD(
            DATE_SUB(DATE(o.start_date), 1),
            CAST(((o.quantity + COALESCE(p.pill_count, 0)) / o.daily_dose) AS INT)
        ) AS expiry_date
    FROM tmp_arv_orders o
    LEFT JOIN tmp_pill_count p
        ON o.patient_id = p.patient_id
       AND o.site_id = p.site_id
       AND o.drug_inventory_id = p.drug_inventory_id
       AND DATE(o.start_date) = p.visit_date
    """
    return spark.sql(query)


display(arv_dispensing("2026-06-30", 870))