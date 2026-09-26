from pyspark.sql import DataFrame
from pyspark.sql import functions as F

"""Nettoyage et enrichissement des DataFrames Spark.

Chaque fonction prend un DataFrame et en retourne un nouveau (Spark est
immutable), ce qui permet de les chaîner dans un pipeline.
"""

def drop_duplicates(df: DataFrame, subset: list[str]) -> DataFrame: 
    """Supprime les doublons selon les colonnes données.

    Args:
        df: DataFrame d'entrée.
        subset: Liste des colonnes qui définissent l'unicité.

    Returns:
        DataFrame sans doublons sur les colonnes spécifiées.
    """
    return df.dropDuplicates(subset)

def trim_strings_patients(df: DataFrame) -> DataFrame:
    """Nettoie les espaces en début et fin des chaînes de caractères.

    Args:
        df: DataFrame des patients.

    Returns:
        DataFrame avec les chaînes nettoyées.
    """
    return (
        df.withColumn("prenom", F.trim(F.col("prenom"))) 
            .withColumn("nom", F.trim(F.col("nom"))) 
            .withColumn("ville", F.trim(F.col("ville")))
    )

def validate_sexe_patients(df: DataFrame) -> DataFrame:
    """Garde uniquement les lignes avec un sexe valide (F ou M).

    Args:
        df: DataFrame des patients.

    Returns:
        DataFrame filtré sur les sexes valides.
    """
    return df.filter(F.col("sexe").isin(["F","M"]))

def handle_nulls_patients(df: DataFrame) -> DataFrame:
    """Gère les valeurs manquantes du DataFrame patients.

    Args:
        df: DataFrame des patients.

    Returns:
        DataFrame nettoyé.
    """
    return df.dropna(subset=["id_patient","nom","prenom","sexe"]).fillna({"antecedents": "aucun", "allergies": "aucune"})

def enrich_patients(df: DataFrame) -> DataFrame:
    """Calcule l'IMC et le place juste après poids_kg.

    Args:
        df: DataFrame des patients.

    Returns:
        DataFrame avec la colonne imc ajoutée et les colonnes réordonnées.
    """
    df = df.withColumn("imc", F.round(F.col("poids_kg") / ((F.col("taille_cm") / 100) ** 2), 1))
    ordre = [
        "id_patient", "prenom", "nom", "date_naissance", "sexe",
        "groupe_sanguin", "ville", "taille_cm", "poids_kg", "imc",
        "antecedents", "allergies"
    ]
    return df.select(*ordre)

def handle_nulls_vitals(df: DataFrame) -> DataFrame:
    """Supprime les mesures sans id_patient ou sans horodatage.

    Args:
        df: DataFrame des signes vitaux.

    Returns:
        DataFrame filtré.
    """
    return df.dropna(subset=["id_patient","horodatage"])

def validate_tension_vitals(df: DataFrame) -> DataFrame:
    """Garde uniquement les mesures où systolique > diastolique.

    Args:
        df: DataFrame des signes vitaux.

    Returns:
        DataFrame filtré sur les tensions cohérentes.
    """ 
    return df.filter(F.col("tension_systolique") > F.col("tension_diastolique"))

def add_anomaly_flag(df: DataFrame) -> DataFrame:
    """Ajoute une colonne booléenne est_anomalie.

    Args:
        df: DataFrame des signes vitaux.

    Returns:
        DataFrame avec la colonne est_anomalie (booléenne).
    """
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