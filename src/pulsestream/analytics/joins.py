from pyspark.sql import DataFrame
from pyspark.sql.functions import broadcast

def join_vitals_patients(vitals_df: DataFrame, patients_df: DataFrame) -> DataFrame:
    return vitals_df.join(broadcast(patients_df), on="id_patient",how='inner')