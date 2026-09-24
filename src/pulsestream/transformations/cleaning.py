from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def drop_duplicates(df: DataFrame, subset: list[str]) -> DataFrame: 
    return df.dropDuplicates(subset)

def trim_strings_patients(df: DataFrame) -> DataFrame: 
    return (
        df.withColumn("prenom", F.trim(F.col("prenom"))) 
            .withColumn("nom", F.trim(F.col("nom"))) 
            .withColumn("ville", F.trim(F.col("ville")))
    )

def validate_sexe_patients(df: DataFrame) -> DataFrame: 
    return df.filter(F.col("sexe").isin(["F","M"]))

def handle_nulls_patients(df: DataFrame) -> DataFrame:
    return df.dropna(subset=["id_patient","nom","prenom","sexe"]).fillna({"antecedents": "aucun", "allergies": "aucune"})

def enrich_patients(df: DataFrame) -> DataFrame: 
    df = df.withColumn("imc", F.round(F.col("poids_kg") / ((F.col("taille_cm") / 100) ** 2), 1))
    ordre = [
        "id_patient", "prenom", "nom", "date_naissance", "sexe",
        "groupe_sanguin", "ville", "taille_cm", "poids_kg", "imc",
        "antecedents", "allergies"
    ]
    return df.select(*ordre)

def handle_nulls_vitals(df: DataFrame) -> DataFrame:
    return df.dropna(subset=["id_patient","horodatage"])

def validate_tension_vitals(df: DataFrame) -> DataFrame: 
    return df.filter(F.col("tension_systolique") > F.col("tension_diastolique"))

def add_anomaly_flag(df: DataFrame) -> DataFrame: 
    anomalie = (
        (F.col("frequence_cardiaque") > 100) |
        (F.col("frequence_cardiaque") < 50) |
        (F.col("tension_systolique") > 140) |
        (F.col("tension_systolique") < 90) |
        (F.col("temperature") > 38.0) |
        (F.col("temperature") < 36.0) |
        (F.col("saturation_oxygene") < 92)
    )
    return df.withColumn("est_anomalie", anomalie)

def check_referential_integrity(vitals_df : DataFrame, patients_df : DataFrame) -> DataFrame: 
    return vitals_df.join(patients_df, "id_patient", "left_anti")