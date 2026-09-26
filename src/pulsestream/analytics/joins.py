from pyspark.sql import DataFrame
from pyspark.sql.functions import broadcast

"""Jointures entre les DataFrames du projet.
"""

def join_vitals_patients(vitals_df: DataFrame, patients_df: DataFrame) -> DataFrame:
    """Joint les vitals avec les informations patient.
    
    Args:
        vitals_df: DataFrame des signes vitaux.
        patients_df: DataFrame des patients (sera broadcasté).

    Returns:
        DataFrame joint avec les colonnes des deux sources.
    """
    return vitals_df.join(broadcast(patients_df), on="id_patient",how='inner')