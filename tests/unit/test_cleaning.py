"""Tests unitaires pour le module cleaning."""
from pyspark.sql import Row 
from pyspark.sql.functions import col, to_timestamp
from pulsestream.transformations.cleaning import drop_duplicates, validate_id_patient, validate_sexe_patients, trim_strings_patients, \
      handle_nulls_patients, enrich_patients, handle_nulls_vitals, validate_tension_vitals, validate_groupe_sanguin , add_anomaly_flag, \
      check_orphan_vitals
from chispa import assert_df_equality



def test_drop_duplicates(spark): 
    """drop_duplicate doit supprimer les doublons selon les colonnes données.
    """
    df =  spark.createDataFrame([
        Row(id_patient="P1"),
        Row(id_patient="P2"),
        Row(id_patient="P3"),   
        Row(id_patient="P3"), 
    ])
    df_exp =  spark.createDataFrame([
        Row(id_patient="P1"),
        Row(id_patient="P2"),
        Row(id_patient="P3") 
    ])
    

    resultat = drop_duplicates(df,subset=["id_patient"])

    assert_df_equality(resultat, df_exp, ignore_row_order=True)


def test_validate_id_patient(spark):
    """validate_id_patien doit garder uniquement les patients dont l'id_patient respecte le format PAT-XXXX.
    """
    df = spark.createDataFrame([
        Row(id_patient="PAT-0012"),
        Row(id_patient="PAT-0111"),
        Row(id_patient="PAT-0"),
        Row(id_patient="P4"),
    ])

    df_exp = spark.createDataFrame([
        Row(id_patient="PAT-0012"),
        Row(id_patient="PAT-0111"),
    ])
 
 
    resultat = validate_id_patient(df)
    
    assert_df_equality(resultat, df_exp, ignore_row_order=True)


def test_trim_strings_patients(spark):
    """ trim_string_patients doit nettoyer les espaces en début et fin des chaînes de caractères (nom, prenom, ville).
    """

    df = spark.createDataFrame([
        Row(id_patient="P1", nom=" Bouzidi", prenom=" Siham ", ville=" Paris" ),
        Row(id_patient="P2", nom="Fafa", prenom="LALA ", ville="      Marseille" )
    ])

    df_exp = spark.createDataFrame([
        Row(id_patient="P1", nom="Bouzidi",prenom="Siham", ville="Paris"),
        Row(id_patient="P2", nom="Fafa", prenom="LALA", ville="Marseille" )
    ])

    resultat = trim_strings_patients(df)
    assert_df_equality(resultat, df_exp, ignore_row_order=True)



def test_validate_sexe(spark):
    """validate_sexe_patients ne doit garder que les sexes F et M."""

    df = spark.createDataFrame([
        Row(id_patient="P1", sexe="F"),
        Row(id_patient="P2", sexe="M"),
        Row(id_patient="P3", sexe="X"),   
        Row(id_patient="P4", sexe="femme"),  
    ])

    df_exp = spark.createDataFrame([
        Row(id_patient="P1", sexe="F"),
        Row(id_patient="P2", sexe="M"), 
    ])
 
 
    resultat = validate_sexe_patients(df)
    
    assert_df_equality(resultat, df_exp, ignore_row_order=True)


def test_handle_nulls_patients(spark):
    """handle_nulls_patient doit gérer les valeurs manquantes du DataFrame patients.
    """

    df = spark.createDataFrame([
        Row(id_patient="P1", nom="Siham", prenom="Bouzidi", sexe="F", antecedents=None, allergies=None),
        Row(id_patient="P2", nom="Siham", prenom="Bouzidi", sexe="F", antecedents="diabète", allergies="penicilline"),
        Row(id_patient="P1", nom=None, prenom="Bouzidi", sexe="F",  antecedents=None, allergies=None),
        Row(id_patient="P1", nom="Siham", prenom=None, sexe="F",  antecedents=None, allergies=None),
        Row(id_patient="P1", nom="Siham", prenom="Bouzidi", sexe=None,  antecedents=None, allergies=None)
    ])

    df_exp = spark.createDataFrame([
        Row(id_patient="P1", nom="Siham", prenom="Bouzidi", sexe="F", antecedents="aucun", allergies="aucune"),
        Row(id_patient="P2", nom="Siham", prenom="Bouzidi", sexe="F", antecedents="diabète", allergies="penicilline")
    ])

    resultat = handle_nulls_patients(df)

    assert_df_equality(resultat, df_exp, ignore_row_order=True, ignore_nullable=True)




def test_enrich_patients(spark):
    """enrich_patients doit calculer l'IMC et le place juste après poids_kg.
    """
    input_data = [
        ("P001", "Alice", "Dupont", "1990-01-01", "F",
         "A+", "Paris", 165, 60.0, "Aucun", "Aucune"),
        ("P002", "Bob", "Martin", "1985-06-15", "M",
         "O-", "Lyon", 180, 90.0, "Asthme", "Pollen"),
    ]
    input_cols = [
        "id_patient", "prenom", "nom", "date_naissance", "sexe",
        "groupe_sanguin", "ville", "taille_cm", "poids_kg",
        "antecedents", "allergies",
    ]
    df_input = spark.createDataFrame(input_data, input_cols)

    expected_data = [
        ("P001", "Alice", "Dupont", "1990-01-01", "F",
         "A+", "Paris", 165, 60.0, 22.0, "Aucun", "Aucune"),
        ("P002", "Bob", "Martin", "1985-06-15", "M",
         "O-", "Lyon", 180, 90.0, 27.8, "Asthme", "Pollen"),
    ]
    expected_cols = [
        "id_patient", "prenom", "nom", "date_naissance", "sexe",
        "groupe_sanguin", "ville", "taille_cm", "poids_kg", "imc",
        "antecedents", "allergies",
    ]
    df_expected = spark.createDataFrame(expected_data, expected_cols)

    df_result = enrich_patients(df_input)

    assert_df_equality(df_result, df_expected, ignore_row_order=True)
    assert df_result.columns == expected_cols  

def test_validate_groupe_sanguin(spark):
    """validate_groupe_sanguin doit filtrer les lignes dont groupe_sanguin n’est pas dans la liste officielle.
    """

    df = spark.createDataFrame([
        Row(id_patient="P1", groupe_sanguin="O+"),
        Row(id_patient="P2", groupe_sanguin="A+"),
        Row(id_patient="P3", groupe_sanguin="O+"),
        Row(id_patient="P4", groupe_sanguin="K"),
    ])

    df_exp = spark.createDataFrame([
        Row(id_patient="P1", groupe_sanguin="O+"),
        Row(id_patient="P2", groupe_sanguin="A+"),
        Row(id_patient="P3", groupe_sanguin="O+"),
    ])


    resultat = validate_groupe_sanguin(df)

    assert_df_equality(resultat, df_exp, ignore_row_order=True)




def test_handle_nulls_vitals(spark):
    """handle_nulls_vitals doit supprimer les mesures sans id_patient ou sans horodatage.
    """
    df = spark.createDataFrame([
        Row(id_patient="P1", horodatage="2026-09-27T13:28:27.615848"),
        Row(id_patient="P2", horodatage=None),
        Row(id_patient="P3", horodatage="2026-09-27T13:28:27.615848"),
        Row(id_patient="P4", horodatage=None),
    ])
    df = df.withColumn("horodatage", to_timestamp(col("horodatage")))


    df_exp = spark.createDataFrame([
        Row(id_patient="P1", horodatage="2026-09-27T13:28:27.615848"),
        Row(id_patient="P3", horodatage="2026-09-27T13:28:27.615848"),
    ])
    df_exp = df_exp.withColumn("horodatage", to_timestamp(col("horodatage")))


    resultat = handle_nulls_vitals(df)

    assert_df_equality(resultat, df_exp, ignore_row_order=True)
    

def test_validate_tension_vitals(spark):
    """validate_tension_vitals doit garder uniquement les mesures où systolique > diastolique.
    """ 
    df = spark.createDataFrame([
        Row(id_patient="P1",tension_systolique=110, tension_diastolique=109),
        Row(id_patient="P2",tension_systolique=111, tension_diastolique=110),
        Row(id_patient="P3",tension_systolique=112, tension_diastolique=120),
        Row(id_patient="P4",tension_systolique=134, tension_diastolique=140),
    ])

    df_exp = spark.createDataFrame([
        Row(id_patient="P1",tension_systolique=110, tension_diastolique=109),
        Row(id_patient="P2",tension_systolique=111, tension_diastolique=110),
    ])

    resultat = validate_tension_vitals(df)

    assert_df_equality(resultat, df_exp, ignore_row_order=True)



def test_add_anomaly_flag(spark):
    """add_anomaly_flag doit ajouter une colonne booléenne est_anomalie et ajoute un flag pour chaque anomalie
    """
    df = spark.createDataFrame([
        Row(id_patient="P1", frequence_cardiaque=101, tension_systolique=120, tension_diastolique=80, temperature=37.0, saturation_oxygene=98),
        Row(id_patient="P2", frequence_cardiaque=40, tension_systolique=120, tension_diastolique=80, temperature=37.0, saturation_oxygene=98),
        Row(id_patient="P3", frequence_cardiaque=75, tension_systolique=120, tension_diastolique=54, temperature=37.0, saturation_oxygene=98),
        Row(id_patient="P4", frequence_cardiaque=75, tension_systolique=159, tension_diastolique=80, temperature=37.0, saturation_oxygene=98),
        Row(id_patient="P5", frequence_cardiaque=75, tension_systolique=120, tension_diastolique=80, temperature=34.0, saturation_oxygene=98),
        Row(id_patient="P6", frequence_cardiaque=75, tension_systolique=120, tension_diastolique=80, temperature=39.0, saturation_oxygene=98),
        Row(id_patient="P7", frequence_cardiaque=75, tension_systolique=120, tension_diastolique=80, temperature=37.0, saturation_oxygene=91),
        Row(id_patient="P8", frequence_cardiaque=75, tension_systolique=120, tension_diastolique=80, temperature=37.0, saturation_oxygene=98)
    ])

    resultat = add_anomaly_flag(df)


    assert resultat.filter(col("est_anomalie")).count() == 7


    assert resultat.filter(col("id_patient") == "P1").first()["est_tachycardie"] is True
    assert resultat.filter(col("id_patient") == "P2").first()["est_bradycardie"] is True
    assert resultat.filter(col("id_patient") == "P3").first()["est_hypotension"] is True
    assert resultat.filter(col("id_patient") == "P4").first()["est_hypertension"] is True
    assert resultat.filter(col("id_patient") == "P5").first()["est_hypothermie"] is True
    assert resultat.filter(col("id_patient") == "P6").first()["est_fievre"] is True
    assert resultat.filter(col("id_patient") == "P7").first()["est_desaturation"] is True

    assert resultat.filter(col("id_patient") == "P8").first()["est_anomalie"] is False

def test_check_orphan_vitals(spark): 
    """doit retourner les mesures (vitals) dont l'id_patient n'existe pas dans la table des patients. Utilise une jointure left_anti : le résultat
    (intégrité référentielle)
    """
    vitals = spark.createDataFrame([
        Row(id_patient="P1"),
        Row(id_patient="P2"),
        Row(id_patient="P3"),
        Row(id_patient="P4"),
    ])
    patients1 = spark.createDataFrame([
        Row(id_patient="P1"),
        Row(id_patient="P2"),
        Row(id_patient="P3"),
        Row(id_patient="P4"),
    ])

    patients2 = spark.createDataFrame([
        Row(id_patient="P1"),
        Row(id_patient="P2"),
        Row(id_patient="P3"),
    ])

    df1_exp = spark.createDataFrame([], "id_patient STRING")

    df2_exp = spark.createDataFrame([
        Row(id_patient="P4"),
    ])

    resultat1 = check_orphan_vitals(vitals,patients1)
    resultat2 = check_orphan_vitals(vitals,patients2)

    assert_df_equality(resultat1, df1_exp, ignore_row_order=True) 

    assert_df_equality(resultat2, df2_exp, ignore_row_order=True)



    



