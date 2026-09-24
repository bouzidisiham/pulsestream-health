from pyspark.sql import SparkSession, DataFrame
from pulsestream.transformations.schemas import PATIENTS_SCHEMA, VITALS_SCHEMA


def read_patients_csv(spark: SparkSession, path: str) -> DataFrame:  
    return spark.read.csv(path, header=True, schema=PATIENTS_SCHEMA)

def read_patients_json(spark: SparkSession, path: str) -> DataFrame: 
    return spark.read.option("multiline",True).schema(PATIENTS_SCHEMA).json(path)

def read_vitals_csv(spark: SparkSession, path: str) -> DataFrame: 
    return spark.read.csv(path, header=True, schema=VITALS_SCHEMA)

def read_vitals_json(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.option("multiline",True).schema(VITALS_SCHEMA).json(path)