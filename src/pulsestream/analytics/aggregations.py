from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from functools import reduce

ANOMALY_TYPES = ["tachycardie", "bradycardie", "hypertension", "hypotension", "fievre", "hypothermie", "desaturation"]

def compter_anomalies_par_patient(df_vitals: DataFrame) -> DataFrame:
    """Calcule un profil d'anomalies par patient.

    Args:
        df_vitals: DataFrame de mesures (sortie de ``add_anomaly_flag``),
            avec les colonnes ``est_<type>`` booléennes.

    Returns:
        DataFrame [id_patient, nb_mesures, nb_<type>..., nb_anomalies, taux_anomalies],
        retourne un profil pour tout les patients, trié par taux décroissant d anomalies.
    """


    aggs = [
        F.sum(F.when(F.col(f"est_{t}"), 1).otherwise(0)).alias(f"nb_{t}")
        for t in ANOMALY_TYPES
    ]

    somme = reduce(
        lambda a, b: a + b,
        [F.col(f"nb_{t}") for t in ANOMALY_TYPES]
    )

    df = (
        df_vitals.groupBy("id_patient")
        .agg(F.count("*").alias("nb_mesures"),*aggs) 
        .withColumn("nb_anomalies",somme) 
        .withColumn("taux_anomalies", F.round((F.col("nb_anomalies") / F.col("nb_mesures")) * 100, 2))
    )


    ordre = ["id_patient", "nb_mesures"] + [f"nb_{t}" for t in ANOMALY_TYPES] + ["nb_anomalies", "taux_anomalies"]

    return df.select(*ordre).orderBy(F.col("taux_anomalies").desc())
