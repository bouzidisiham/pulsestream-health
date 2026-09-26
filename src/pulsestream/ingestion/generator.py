from faker import Faker
import random
from datetime import datetime, timedelta, date

fake = Faker("fr_FR")

# Reproductibilité : on fixe les seeds 
Faker.seed(42)
random.seed(42)


# Décalaration des constances
SEXE = ["F","M"]
ANTECEDENTS = ['diabete','hypertension', 'asthme', 'insuffisance_cardiaque', 'hypercholesterolemie', 'arthrose', 'depression', 'hypothyroidie']
ALLERGIES = ['penicilline', 'arachides', 'latex', 'aspirine', 'iode', 'pollen', 'acariens', 'lactose', 'gluten', 'sulfamides']
COLONNES = [
    "id_patient", "prenom", "nom", "date_naissance", "sexe",
    "groupe_sanguin", "ville", "taille_cm", "poids_kg",
    "antecedents", "allergies",
]
GS = ['A+','A-','B+','B-','AB+','AB-','O+','O-']
IMC_CATEGORIES = [
    (18.0, 18.5),   # insuffisance pondérale
    (18.5, 25.0),   # normal
    (25.0, 30.0),   # surpoids
    (30.0, 40.0),   # obésité
]
PROBA_IMC = [0.05, 0.50, 0.30, 0.15]

VILLES = [
    "Paris", "Marseille", "Lyon", "Toulouse", "Nice",
    "Nantes", "Montpellier", "Strasbourg", "Bordeaux", "Lille",
    "Rennes", "Reims", "Saint-Étienne", "Toulon", "Le Havre",
    "Grenoble", "Dijon", "Angers", "Nîmes", "Villeurbanne",
]

def generate_patients(n: int) -> list[dict]:
    """
    Génère n patients synthétiques avec des identités et profils cohérents.
    Args:
        n: Nombre de patients à générer.

    Returns:
        Une liste de dictionnaires, un par patient.
    """
    patients = []

    for i in range(n):

        id_patient = f"PAT-{i+1:04d}"

        sexe = random.choice(SEXE)

        if sexe == 'F' : 
            prenom = fake.first_name_female()
        else : 
            prenom = fake.first_name_male()

        nom = fake.last_name()

        date_naissance = fake.date_of_birth(minimum_age=18, maximum_age=90).isoformat()

        groupe_sanguin = random.choice(GS)

        ville = random.choice(VILLES)

        taille_cm = random.randint(150, 195)
        taille_m = taille_cm / 100
        categorie = random.choices(IMC_CATEGORIES, weights=PROBA_IMC, k=1)[0] 
        imc = random.uniform(categorie[0], categorie[1])
        poids_kg = round(imc * taille_m ** 2, 1)

        proba_antecedents = random.random()
        antecedents = ""
        if proba_antecedents < 0.1 :
            nb = random.choice([2, 3])
        elif proba_antecedents < 0.3 :
            nb = 1
        else : 
            nb = 0

        antecedents_list = random.sample(ANTECEDENTS, nb)
        if imc >= 30:
            antecedents_list.append("obesite")

        antecedents = ";".join(antecedents_list)

        allergies = ""
        if random.random() < 0.1 : 
            allergies = random.choice(ALLERGIES)
        
        patients.append({
            "id_patient" : id_patient, "prenom" : prenom, "nom" :nom, "date_naissance" : date_naissance, "sexe" : sexe,
            "groupe_sanguin" : groupe_sanguin, "ville" : ville, "taille_cm" : taille_cm, "poids_kg" : poids_kg,
            "antecedents" : antecedents, "allergies" : allergies,
        })
    
    return patients




def generate_vitals(patient: dict, start: datetime, n_mesures: int) -> list[dict]:
    """
    Génère les signes vitaux d'un patient sur une période donnée.
    
    Args:
        patient: Dictionnaire de type patient.
        start: Date et heure de la première mesure.
        n_mesures: Nombre de mesures à générer.

    Returns:
        Une liste de dictionnaires, un par mesure.
    """
    id_patient = patient["id_patient"] 
    vitals = []

    date_naissance = date.fromisoformat(patient["date_naissance"])
    age = (date.today() - date_naissance).days // 365
    
    taille_m = patient["taille_cm"] / 100
    imc = patient["poids_kg"] / (taille_m ** 2)

    if patient["sexe"] == "F":
        baseline_fc = random.gauss(82, 10)
    else:
        baseline_fc = random.gauss(76, 10)


    baseline_sys = random.gauss(115, 8)       
    baseline_temp = random.gauss(36.8, 0.3)
    baseline_spo2 = random.gauss(97.5, 1.5)


    if age < 40:
        facteur = 1.0
    elif age < 60:
        facteur = 1.5
    elif age < 75:
        facteur = 2.5
    else:
        facteur = 3.4

    for i in range(n_mesures) :
         
        jitter = random.randint(-10,10)
        horodatage = (start + timedelta(hours=2*i) + timedelta(minutes=jitter)).isoformat()

        fc = random.gauss(baseline_fc,2)
        fc = max(50,min(110,fc))
        fc = int(round(fc))

        sys = random.gauss(baseline_sys,3)
        sys = max(90,min(140,sys))
        sys = int(round(sys))

        dia = sys - random.randint(30,50)
        dia = max(60,min(90,dia))
        dia = int(round(dia))

        temp = random.gauss(baseline_temp,0.2)
        temp = max(36.5,min(38.0,temp))
        temp = round(temp, 1)

        spo2 = random.gauss(baseline_spo2,1)
        spo2 = max(92,min(100,spo2))
        spo2 = int(round(spo2))

        
        if random.random() < (0.05 * facteur) :

            if imc < 30 : 
                weights = [1, 1, 1]
            else : 
                weights = [1, 2, 1]

            anomalie = random.choices([1, 2, 3], weights=weights)[0]
            
            if anomalie == 1 :
                temp = random.uniform(38.5, 40.0)
                temp = round(temp, 1)
            elif anomalie == 2 :
                fc = random.uniform(110, 140)
                fc = int(round(fc))
            else :
                spo2 = random.uniform(85, 92)
                spo2 = int(round(spo2))

        vitals.append({'id_patient' : id_patient, 'horodatage' : horodatage, 'frequence_cardiaque' : fc, 'tension_systolique' : sys, 'tension_diastolique' : dia, 'temperature' : temp, 'saturation_oxygene' : spo2})

    return vitals


def generate_all_data(n: int, start: datetime, n_mesures: int) -> tuple[list[dict], list[dict]] :
    """Orchestre la génération complète des données du projet.
    Args:
        n: Nombre de patients à générer.
        start: Date et heure de début des mesures.
        n_mesures: Nombre de mesures par patient.

    Returns:
        Un tuple (patients, vitals) contenant les deux listes de dicts.
    """
    
    patients = generate_patients(n)
    vitals = []

    for patient in patients :
        
        vitals_i = generate_vitals(patient,start,n_mesures)

        vitals.extend(vitals_i)

    return (patients , vitals)
        
    


