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

def validate_id_patient(df: DataFrame) -> DataFrame:
    """Garde uniquement les patients dont l'id_patient respecte le format PAT-XXXX.
    Args:
        df: DataFrame des patients.
    
    Returns:
        DataFrame filtré sur les IDs valides.
    """
    pattern = r"^PAT-\d{4}$"
    return df.filter(F.col("id_patient").rlike(pattern))
    

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

def validate_groupe_sanguin(df: DataFrame) -> DataFrame:
    """Filtrer les lignes dont groupe_sanguin n’est pas dans la liste officielle.
    
    Args:
        df: DataFrame des patients.

    Returns:
        DataFrame filtré.
    """
    return df.filter(F.col("groupe_sanguin").isin(["A+", "A-", "B+", "B-", "AB+", "AB-", "O+", "O-"]))

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
    """Ajoute une colonne booléenne est_anomalie et ajoute un flag pour chaque anomalie

    Args:
        df: DataFrame des signes vitaux.

    Returns:
        DataFrame avec la colonne est_anomalie (booléenne).
    """
    est_tachycardie = (
        (F.col("frequence_cardiaque") > 100) 
    )
    est_bradycardie = (
        (F.col("frequence_cardiaque") < 50) 
    )
    est_hypertension = (
        (F.col("tension_systolique") > 140) |
        (F.col("tension_diastolique") > 90)
    )

    est_hypotension = (
        (F.col("tension_systolique") < 90) |
        (F.col("tension_diastolique") < 60)
    )
    est_fievre = (
        (F.col("temperature") > 38.0)
    )
    est_hypothermie = (
        (F.col("temperature") < 36.0) 
    )
    est_desaturation = (
        (F.col("saturation_oxygene") < 92)     
    )

    return (df.withColumn("est_tachycardie", est_tachycardie)
            .withColumn("est_bradycardie",est_bradycardie)
            .withColumn("est_hypertension", est_hypertension)
            .withColumn("est_hypotension", est_hypotension)
            .withColumn("est_fievre", est_fievre)
            .withColumn("est_hypothermie", est_hypothermie)
            .withColumn("est_desaturation", est_desaturation)
            .withColumn("est_anomalie", est_tachycardie | est_bradycardie | est_hypertension | est_hypotension | est_fievre | est_hypothermie | est_desaturation))

def check_orphan_vitals(vitals_df : DataFrame, patients_df : DataFrame) -> DataFrame: 
    """Retourne les mesures (vitals) dont l'id_patient n'existe pas dans la table des patients. Utilise une jointure left_anti : le résultat
    (intégrité référentielle)

    Args:
        vitals_df (DataFrame): DataFrame des mesures de signes vitaux.
        patients_df (DataFrame): DataFrame des patients.

    Returns:
        DataFrame: Sous-ensemble de ``vitals_df`` contenant uniquement
        les lignes dont l'``id_patient`` est absent de ``patients_df``.
        Un DataFrame vide signifie que l'intégrité est respectée.
    """
    return vitals_df.join(patients_df, "id_patient", "left_anti")