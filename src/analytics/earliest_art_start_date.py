"""
HIV analytics.

Contains HIV-related calculations.
"""

from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src import settings

spark = SparkSession.builder.getOrCreate()

# ============================================================
# Table References
# ============================================================

tables = {
    item["table"]: f"{settings.TARGET_SCHEMA}.{item['table']}"
    for item in settings.TRANSACTION_TABLES
}


# ============================================================
# ART Earliest Start Date
# ============================================================

def hiv_reg_earliest_start_date(end_date: str) -> DataFrame:
    obs_table = tables["observation"]
    enc_table = tables["encounter"]
    concept_name_table = f"{settings.TARGET_SCHEMA}.concept_name"

    query = f"""
        SELECT 
            DISTINCT o.person_id, 
            cn.name AS concept_name,
            o.obs_datetime, 
            o.value_datetime,e.encounter_type
        FROM {obs_table} o
        INNER JOIN {enc_table} e 
            ON o.encounter_id = e.encounter_id 
           AND o.site_id = e.site_id 
        INNER JOIN {concept_name_table} cn 
            ON o.concept_id = cn.concept_id 
        WHERE o.site_id = 870 
          AND o.concept_id = 2516
          AND o.value_datetime <= '{end_date}'
          AND cn.concept_name_type = 'FULLY_SPECIFIED'
        LIMIT 10000
    """

    return spark.sql(query)

def concept_names():
    concept_name_table = f"{settings.TARGET_SCHEMA}.concept_name"

    return spark.sql(f"""
        WITH numbered_concepts AS (
            SELECT 
                *,
                ROW_NUMBER() OVER (ORDER BY concept_id, concept_name_id) AS row_num
            FROM {concept_name_table}
        )
        SELECT * EXCEPT (row_num)
        FROM numbered_concepts
        WHERE row_num BETWEEN 0 AND 10000;
    """)

def obs_table():
    obs_table = f"{settings.TARGET_SCHEMA}.order_type"

    return spark.sql(f"""
        SELECT * FROM {obs_table}
        LIMIT 10000;
    """)

def arv_drug_earliest_start_date(end_date: str):
    order_table = f"{settings.TARGET_SCHEMA}.order"
    order_type_table = f"{settings.TARGET_SCHEMA}.order_type"
    drug_table = f"{settings.TARGET_SCHEMA}.drug"
    drug_order_table = f"{settings.TARGET_SCHEMA}.drug_order"
    arv_drug_table = f"{settings.TARGET_SCHEMA}.arv_drug"

    query = f"""
        SELECT
            o.patient_id,
            MIN(o.start_date) AS first_obs_datetime
        FROM {order_table} o
        INNER JOIN {order_type_table} ot
            ON o.order_type_id = ot.order_type_id
        INNER JOIN {drug_order_table} drug_o
            ON o.order_id = drug_o.order_id
        INNER JOIN {drug_table} d
            ON drug_o.drug_inventory_id = d.drug_id
        INNER JOIN {arv_drug_table} ad
            ON d.drug_id = ad.drug_id
        WHERE ot.name = 'Drug Order'
          AND CAST(drug_o.quantity AS INT) > 0
          AND DATE(o.start_date) <= '{end_date}'
        GROUP BY o.patient_id
        ORDER BY o.patient_id 
        LIMIT 10000
    """

    return spark.sql(query)
    
def earliest_start_dateX(end_date: str) -> DataFrame:
    art_df = hiv_reg_earliest_start_date(end_date)
    arv_df = arv_drug_earliest_start_date(end_date)

    art_df.createOrReplaceTempView("art_earliest")
    arv_df.createOrReplaceTempView("arv_earliest")

    query = """
        SELECT 
            arv.patient_id,
            arv.first_obs_datetime
        FROM arv_earliest arv
        WHERE arv.patient_id NOT IN (
            SELECT DISTINCT person_id 
            FROM art_earliest 
            WHERE person_id IS NOT NULL
        )
    """

    return spark.sql(query)

def earliest_start_date(end_date: str) -> DataFrame:
    """
    Returns all patients. If a patient exists in art_earliest_start_date, 
    their ART record is returned and they are excluded from arv_drug_earliest.
    """
    art_df = (
        hiv_reg_earliest_start_date(end_date)
        .withColumnRenamed("person_id", "patient_id")
        .withColumnRenamed("value_datetime", "earliest_start_date")
        .withColumn("source", F.lit("ART_OBSERVATION"))
        .select("patient_id", "earliest_start_date", "source")
    )

    arv_df = (
        arv_drug_earliest_start_date(end_date)
        .withColumnRenamed("first_obs_datetime", "earliest_start_date")
        .withColumn("source", F.lit("ARV_DRUG_ORDER"))
        .select("patient_id", "earliest_start_date", "source")
    )

    # 1. Get ARV patients that DO NOT exist in ART
    arv_only_df = arv_df.join(art_df, "patient_id", "left_anti")

    # 2. Combine all ART patients with remaining non-ART ARV patients
    return art_df.unionByName(arv_only_df)


display(earliest_start_date("2026-06-30"))
#display(obs_table())

