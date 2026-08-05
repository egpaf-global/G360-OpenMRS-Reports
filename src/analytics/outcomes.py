from pyspark.sql import SparkSession, DataFrame
from src import settings

spark = SparkSession.builder.getOrCreate()

tables = {
    item["table"]: f"{settings.TARGET_SCHEMA}.{item['table']}"
    for item in settings.TRANSACTION_TABLES
}


def patient_adverse_outcome(
    visit_date: str,
    site_id: int = 870
) -> DataFrame:

    patient_program_table = tables["patient_program"]
    patient_state_table = f"{settings.TARGET_SCHEMA}.patient_state"
    concept_name_table = f"{settings.TARGET_SCHEMA}.concept_name"
    program_workflow_state_table = (
        f"{settings.TARGET_SCHEMA}.program_workflow_state"
    )
    program_workflow_table = (
        f"{settings.TARGET_SCHEMA}.program_workflow"
    )

    query = f"""
    WITH ranked_patient_states AS (
        SELECT
            pp.patient_id,
            pp.program_id,
            ps.patient_state_id,
            ps.state,
            cn.name AS state_name,
            ps.start_date,
            ps.end_date,
            pws.concept_id,
            ROW_NUMBER() OVER (
                PARTITION BY pp.patient_id
                ORDER BY
                    ps.start_date DESC,
                    ps.date_created DESC,
                    ps.patient_state_id DESC
            ) AS state_rank
        FROM {patient_program_table} pp
        INNER JOIN {patient_state_table} ps
            ON pp.patient_program_id = ps.patient_program_id
        INNER JOIN {program_workflow_state_table} pws
            ON ps.state = pws.program_workflow_state_id
        INNER JOIN {program_workflow_table} pw
            ON pws.program_workflow_id = pw.program_workflow_id
        INNER JOIN {concept_name_table} cn 
            ON cn.concept_id = pws.concept_id
        WHERE
            pp.program_id = 1
            AND pp.site_id = {site_id}
            AND DATE(ps.start_date) <= DATE('{visit_date}')
            AND ps.state <> 7
    )
    SELECT *
    FROM ranked_patient_states
    WHERE state_rank = 1
    ORDER BY
        start_date DESC,
        patient_id
    """

    return spark.sql(query)


















def alive_and_on_treatment(
    end_date: str,
    site_id: int = 870
) -> DataFrame:
    """
    Patients considered Alive and On Treatment.

    Logic
    -----
    1. Find all ARV drug orders.
    2. Add pill balances from the previous visit.
    3. Find each patient's latest dispensing date.
    4. Consider ALL ARVs dispensed on that date.
    5. Calculate the minimum days of supply.
    6. Add 30-day grace period.
    """

    order_table = tables["order"]
    drug_order_table = tables["drug_order"]
    observation_table = tables["observation"]
    patient_program_table = tables["patient_program"]
    patient_table = tables["patient"]

    order_type_table = f"{settings.TARGET_SCHEMA}.order_type"
    arv_drug_table = f"{settings.TARGET_SCHEMA}.arv_drug"

    query = f"""

    ------------------------------------------------------------------
    -- Pill balances
    ------------------------------------------------------------------

    WITH pill_remaining AS (

        SELECT

            order_id,

            site_id,

            SUM(COALESCE(value_numeric,0)) AS pill_remaining

        FROM {observation_table}

        WHERE concept_id = 2540

        GROUP BY

            order_id,

            site_id

    ),

    ------------------------------------------------------------------
    -- All ARV Orders
    ------------------------------------------------------------------

    arv_orders AS (

        SELECT

            o.patient_id,

            o.site_id,

            o.order_id,

            o.start_date,

            (
                CAST(do.quantity AS DOUBLE)
                +
                COALESCE(pr.pill_remaining,0)
            ) AS quantity,

            COALESCE(
                NULLIF(do.equivalent_daily_dose,0),
                1
            ) AS equivalent_daily_dose

        FROM {order_table} o

        INNER JOIN {order_type_table} ot
            ON o.order_type_id = ot.order_type_id

        INNER JOIN {drug_order_table} do
            ON o.order_id = do.order_id
           AND o.site_id = do.site_id

        INNER JOIN {arv_drug_table} ad
            ON do.drug_inventory_id = ad.drug_id

        INNER JOIN {patient_program_table} pp
            ON o.patient_id = pp.patient_id
           AND o.site_id = pp.site_id

        LEFT JOIN pill_remaining pr
            ON o.order_id = pr.order_id
           AND o.site_id = pr.site_id

        WHERE

            pp.program_id = 1

            AND o.site_id = {site_id}

            AND LOWER(ot.name) = 'drug order'

            AND do.quantity > 0

            AND DATE(o.start_date) <= DATE('{end_date}')

    ),

    ------------------------------------------------------------------
    -- Latest dispensing visit
    ------------------------------------------------------------------

    latest_visit AS (

        SELECT

            patient_id,

            site_id,

            MAX(start_date) AS last_dispensed_date

        FROM arv_orders

        GROUP BY

            patient_id,

            site_id

    ),

    ------------------------------------------------------------------
    -- All ARVs dispensed during latest visit
    ------------------------------------------------------------------

    latest_arvs AS (

        SELECT

            ao.patient_id,

            ao.site_id,

            ao.order_id,

            ao.start_date,

            ao.quantity,

            ao.equivalent_daily_dose,

            ao.quantity / ao.equivalent_daily_dose
                AS days_of_supply

        FROM arv_orders ao

        INNER JOIN latest_visit lv

            ON ao.patient_id = lv.patient_id

           AND ao.site_id = lv.site_id

           AND ao.start_date = lv.last_dispensed_date

    ),

    ------------------------------------------------------------------
    -- Minimum supply among ARVs on latest visit
    ------------------------------------------------------------------

    patient_supply AS (

        SELECT

            patient_id,

            site_id,

            MAX(order_id) AS last_order_id,

            MAX(start_date) AS last_dispensed_date,

            MIN(days_of_supply) AS days_of_supply

        FROM latest_arvs

        GROUP BY

            patient_id,

            site_id

    )

    ------------------------------------------------------------------
    -- Final Result
    ------------------------------------------------------------------

    SELECT

        p.patient_id,

        p.birthdate,

        p.birthdate_estimated,

        p.gender,

        p.site_id,

        ps.last_order_id,

        ps.last_dispensed_date,

        ROUND(ps.days_of_supply,2) AS days_of_supply,

        DATE_ADD(

            ps.last_dispensed_date,

            CAST(ps.days_of_supply + 30 AS INT)

        ) AS outcome_date,

        'Alive and On Treatment' AS patient_status

    FROM patient_supply ps

    INNER JOIN {patient_table} p

        ON ps.patient_id = p.patient_id

       AND ps.site_id = p.site_id

    WHERE

        DATE_ADD(

            ps.last_dispensed_date,

            CAST(ps.days_of_supply + 30 AS INT)

        ) >= DATE('{end_date}')

    ORDER BY

        p.patient_id

    """

    return spark.sql(query)


display(alive_and_on_treatment("2026-06-30", site_id=870))