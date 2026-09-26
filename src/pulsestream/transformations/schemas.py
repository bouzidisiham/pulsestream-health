"""Schémas DDL des DataFrames Spark utilisés dans le projet.
"""

PATIENTS_SCHEMA = "id_patient STRING NOT NULL, prenom STRING, nom STRING, date_naissance DATE, sexe STRING, groupe_sanguin STRING, ville STRING, taille_cm INT, poids_kg DOUBLE, antecedents STRING, allergies STRING"
VITALS_SCHEMA = "id_patient STRING NOT NULL, horodatage TIMESTAMP, frequence_cardiaque INT, tension_systolique INT, tension_diastolique INT, temperature DOUBLE, saturation_oxygene INT"