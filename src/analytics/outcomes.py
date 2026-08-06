from pyspark.sql import SparkSession, DataFrame
from src import settings
from src.analytics.pharmacy import arv_dispensing
from src.analytics.art_clients_ever_registered import clients_ever_registered_at_art_clinic

spark = SparkSession.builder.getOrCreate()

tables = {
    item["table"]: f"{settings.TARGET_SCHEMA}.{item['table']}"
    for item in settings.TRANSACTION_TABLES
}


def patient_adverse_outcome(visit_date: str, site_id: int = 870):
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
            pp.site_id,
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

def get_alive_and_on_treatment(end_date: str, site_id: int = 870) -> DataFrame:
    # 1. Fetch intermediate DataFrames
    ever_registered_df = clients_ever_registered_at_art_clinic(end_date, site_id)
    adverse_outcome_df = patient_adverse_outcome(end_date, site_id)
    arv_dispensing_df = arv_dispensing(end_date, site_id)
    
    # 2. Register as Temporary Views
    ever_registered_df.createOrReplaceTempView("tmp_art_ever_registered")
    adverse_outcome_df.createOrReplaceTempView("tmp_patient_adverse_outcome")
    arv_dispensing_df.createOrReplaceTempView("tmp_arv_dispensing_df")

    # 3. Query using Windowing and CTE encapsulation for status filtering
    query = f"""
    WITH latest_patient_dispensing AS (
        SELECT
            patient_id,
            site_id,
            quantity AS dispensed_quantity,
            pill_count,
            daily_dose,
            start_date,
            expiry_date,
            ROW_NUMBER() OVER (
                PARTITION BY patient_id, site_id
                ORDER BY
                    start_date DESC,
                    expiry_date ASC,
                    order_id DESC
            ) AS rn
        FROM tmp_arv_dispensing_df
    ),
    patient_outcomes AS (
        SELECT
            reg.patient_id,
            reg.site_id,
            pill.dispensed_quantity,
            pill.pill_count,
            pill.daily_dose,
            pill.start_date AS drug_dispensed_date,
            pill.expiry_date,
            CASE
                -- 1. Preserve explicit adverse outcome state if present
                WHEN adv.state_name IS NOT NULL THEN adv.state_name
                
                -- 2. If no adverse outcome, check defaulting window (> 60 days past expiry)
                WHEN pill.expiry_date IS NOT NULL 
                     AND DATE_ADD(pill.expiry_date, 60) < DATE('{end_date}') 
                THEN 'Defaulter'
                
                -- 3. Active on treatment
                ELSE 'Alive and On Treatment'
            END AS patient_status
        FROM tmp_art_ever_registered reg
        LEFT JOIN tmp_patient_adverse_outcome adv
            ON reg.patient_id = adv.patient_id
           AND reg.site_id = adv.site_id
        LEFT JOIN latest_patient_dispensing pill
            ON reg.patient_id = pill.patient_id
           AND reg.site_id = pill.site_id
           AND pill.rn = 1
    )
    SELECT
        patient_id,
        site_id,
        dispensed_quantity,
        pill_count,
        daily_dose,
        drug_dispensed_date,
        expiry_date,
        patient_status
    FROM patient_outcomes
    WHERE patient_status = 'Defaulter' --- 'Alive and On Treatment'
    """
    
    return spark.sql(query)

display(get_alive_and_on_treatment("2026-06-30", site_id=870))