"""Tests unitaires pour le module aggregations."""
from pulsestream.analytics.aggregations import compter_anomalies_par_patient
from pulsestream.transformations.cleaning import add_anomaly_flag
from pyspark.sql import Row
from chispa import assert_df_equality

def test_compter_anomalies_par_patient(spark):
    """compter_anomalies_par_patient doit calculer un profil d'anomalies par patient.
    """
    df = spark.createDataFrame([
        Row(id_patient="P1", frequence_cardiaque=115, tension_systolique=120, tension_diastolique=80, temperature=37.0, saturation_oxygene=98),
        Row(id_patient="P2", frequence_cardiaque=40, tension_systolique=120, tension_diastolique=80, temperature=37.0, saturation_oxygene=98),
        Row(id_patient="P3", frequence_cardiaque=75, tension_systolique=120, tension_diastolique=54, temperature=37.0, saturation_oxygene=98),
        Row(id_patient="P4", frequence_cardiaque=75, tension_systolique=159, tension_diastolique=80, temperature=37.0, saturation_oxygene=98),
        Row(id_patient="P5", frequence_cardiaque=75, tension_systolique=120, tension_diastolique=80, temperature=34.0, saturation_oxygene=98),
        Row(id_patient="P6", frequence_cardiaque=75, tension_systolique=120, tension_diastolique=80, temperature=39.0, saturation_oxygene=98),
        Row(id_patient="P7", frequence_cardiaque=75, tension_systolique=120, tension_diastolique=80, temperature=37.0, saturation_oxygene=91),
        Row(id_patient="P8", frequence_cardiaque=75, tension_systolique=120, tension_diastolique=80, temperature=37.0, saturation_oxygene=98)
    ])

    df_exp = spark.createDataFrame([
        Row(id_patient="P1", nb_mesures=1, nb_tachycardie=1, nb_bradycardie=0, nb_hypertension=0, nb_hypotension=0, nb_fievre=0, nb_hypothermie=0, nb_desaturation=0, nb_anomalies=1, taux_anomalies=100.0),
        Row(id_patient="P2", nb_mesures=1, nb_tachycardie=0, nb_bradycardie=1, nb_hypertension=0, nb_hypotension=0, nb_fievre=0, nb_hypothermie=0, nb_desaturation=0, nb_anomalies=1, taux_anomalies=100.0),
        Row(id_patient="P3", nb_mesures=1, nb_tachycardie=0, nb_bradycardie=0, nb_hypertension=0, nb_hypotension=1, nb_fievre=0, nb_hypothermie=0, nb_desaturation=0, nb_anomalies=1, taux_anomalies=100.0),
        Row(id_patient="P4", nb_mesures=1, nb_tachycardie=0, nb_bradycardie=0, nb_hypertension=1, nb_hypotension=0, nb_fievre=0, nb_hypothermie=0, nb_desaturation=0, nb_anomalies=1, taux_anomalies=100.0),
        Row(id_patient="P5", nb_mesures=1, nb_tachycardie=0, nb_bradycardie=0, nb_hypertension=0, nb_hypotension=0, nb_fievre=0, nb_hypothermie=1, nb_desaturation=0, nb_anomalies=1, taux_anomalies=100.0),
        Row(id_patient="P6", nb_mesures=1, nb_tachycardie=0, nb_bradycardie=0, nb_hypertension=0, nb_hypotension=0, nb_fievre=1, nb_hypothermie=0, nb_desaturation=0, nb_anomalies=1, taux_anomalies=100.0),
        Row(id_patient="P7", nb_mesures=1, nb_tachycardie=0, nb_bradycardie=0, nb_hypertension=0, nb_hypotension=0, nb_fievre=0, nb_hypothermie=0, nb_desaturation=1, nb_anomalies=1, taux_anomalies=100.0),
        Row(id_patient="P8", nb_mesures=1, nb_tachycardie=0, nb_bradycardie=0, nb_hypertension=0, nb_hypotension=0, nb_fievre=0, nb_hypothermie=0, nb_desaturation=0, nb_anomalies=0, taux_anomalies=0.0),
    ])

    df_flag = add_anomaly_flag(df)

    df_anomalies = compter_anomalies_par_patient(df_flag)

    assert_df_equality(df_anomalies, df_exp, ignore_row_order=True, ignore_nullable=True)