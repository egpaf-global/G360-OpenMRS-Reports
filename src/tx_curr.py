from pyspark.sql import functions as F
from src import settings

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


# Execute and display
display(patients(870))