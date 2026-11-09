from pyspark.sql import SparkSession, DataFrame
from src import settings

spark = SparkSession.builder.getOrCreate()

tables = {
    item["table"]: f"{settings.TARGET_SCHEMA}.{item['table']}"
    for item in settings.TRANSACTION_TABLES
}

def clients_ever_registered_at_art_clinic(end_date: str, site_id: int = 870):
    patient_table = tables["patient"]
    patient_program_table = tables["patient_program"]
    order_table = tables["order"]
    drug_order_table = tables["drug_order"]
    order_type_table = f"{settings.TARGET_SCHEMA}.order_type"
    drug_table = f"{settings.TARGET_SCHEMA}.drug"
    arv_drug_table = f"{settings.TARGET_SCHEMA}.arv_drug"

    query = f"""
        SELECT DISTINCT
            p.patient_id,
            p.birthdate,
            p.birthdate_estimated,
            p.gender,
            p.site_id
        FROM {patient_table} p
        INNER JOIN {patient_program_table} pp
            ON p.patient_id = pp.patient_id
           AND p.site_id = pp.site_id
           AND pp.program_id = 1
        -- Restrict to patients with at least one ARV drug order (ARVs identified via arv_drug)
        INNER JOIN {order_table} o
            ON p.patient_id = o.patient_id
           AND p.site_id = o.site_id
           AND DATE(o.start_date) <= DATE('{end_date}')
        INNER JOIN {order_type_table} ot
            ON o.order_type_id = ot.order_type_id
           AND LOWER(ot.name) = 'drug order'
        INNER JOIN {drug_order_table} do
            ON o.order_id = do.order_id
           AND o.site_id = do.site_id
        INNER JOIN {drug_table} d
            ON do.drug_inventory_id = d.drug_id
        INNER JOIN {arv_drug_table} ad
            ON d.drug_id = ad.drug_id
        WHERE p.site_id = {site_id}
        AND do.quantity > 0
        ORDER BY
            p.site_id,
            p.patient_id
    """
    
    # Return DataFrame directly (do not call .show() inside the function)
    return spark.sql(query)

# Execute and view full interactive result in Databricks
display(clients_ever_registered_at_art_clinic("2026-06-30", site_id=870))