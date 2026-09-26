from pyspark.sql import SparkSession, DataFrame
from pulsestream.transformations.schemas import PATIENTS_SCHEMA, VITALS_SCHEMA

"""Lecture des fichiers sources (CSV et JSON) avec schémas explicites.
"""

def read_patients_csv(spark: SparkSession, path: str) -> DataFrame: 
    """Lit le CSV des patients avec le schéma PATIENTS_SCHEMA.

    Args:
        spark: SparkSession active.
        path: Chemin du fichier CSV.

    Returns:
        DataFrame Spark contenant les patients.
    """ 
    return spark.read.csv(path, header=True, schema=PATIENTS_SCHEMA)

def read_patients_json(spark: SparkSession, path: str) -> DataFrame: 
    """Lit le JSON des patients avec le schéma PATIENTS_SCHEMA.

    Args:
        spark: SparkSession active.
        path: Chemin du fichier JSON.

    Returns:
        DataFrame Spark contenant les patients.
    """
    return spark.read.option("multiline",True).schema(PATIENTS_SCHEMA).json(path)

def read_vitals_csv(spark: SparkSession, path: str) -> DataFrame: 
    """Lit le CSV des signes vitaux avec le schéma VITALS_SCHEMA.

    Args:
        spark: SparkSession active.
        path: Chemin du fichier CSV.

    Returns:
        DataFrame Spark contenant les signes vitaux.
    """
    return spark.read.csv(path, header=True, schema=VITALS_SCHEMA)

def read_vitals_json(spark: SparkSession, path: str) -> DataFrame:
    """Lit le JSON des signes vitaux avec le schéma VITALS_SCHEMA.

    Args:
        spark: SparkSession active.
        path: Chemin du fichier JSON.

    Returns:
        DataFrame Spark contenant les signes vitaux.
    """
    return spark.read.option("multiline",True).schema(VITALS_SCHEMA).json(path)