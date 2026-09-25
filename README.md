# PulseStream Health
Un pipeline de surveillance santé en temps réel avec PySpark (batch + streaming)

## Description : 
- **Quoi ?** Un pipeline de surveillance des signes vitaux.
- **Comment ?** Génération de données synthétiques avec Faker, traitement avec PySpark (batch et Structured Streaming), détection d'anomalies.
- **Pour qui ?** Projet portfolio démontrant les compétences d'un Data Engineer (ETL, streaming, tests, CI/CD).

## Stack :
- Python 3.13
- PySpark 4.0.4
- Java 21 (JDK)
- Streamlit (interface interactive)
- Parquet
- pytest + chispa
- GitHub Actions (à venir)
- Faker

## Architecture : 
> ⚠️ Diagramme à venir

## Structure du projet

```
pulsestream-health/
├── src/pulsestream/ # Code source réutilisable
│ ├── ingestion/ # Génération et écriture des données
│ ├── transformations/ # Nettoyage, enrichissement
│ ├── analytics/ # Jointures, agrégations
│ ├── streaming/ # Structured Streaming
│ └── utils/ # Helpers (Spark, config)
├── app/ # Interface Streamlit
├── tests/ # Tests unitaires
├── scripts/ # Scripts CLI
├── data/ # Données (bronze / silver / gold)
├── notebooks/ # Exploration
└── docs/ # Documentation        
```

## Installation : 
```bash
git clone ...
cd pulsestream-health
python -m venv .venv
.venv\Scripts\activate  # Windows
pip install -e ".[dev]"
```

## Utilisation : 
- Générer les données :  `python scripts/generate_data.py --n-patients 200 --days 7`
- Lancer le batch : *(à venir)*
- Lancer le streaming : *(à venir)*

### Interface Streamlit

(à venir)

Une interface interactive permettra d'explorer les résultats du pipeline :

- **Vue d'ensemble** : KPIs (patients, mesures, anomalies)
- **Patients** : exploration par sexe, âge, IMC, antécédents
- **Signes vitaux** : séries temporelles et distributions
- **Anomalies** : détection et analyse des cas critiques

Commande prévue :

```bash
streamlit run app/app.py
```

## Tests : *(à venir)*

## Roadmap : 
- [x] Structure du projet
- [x] Dépendances (pyproject.toml)
- [x] .gitignore
- [x] Générateur de données synthétiques
- [x] Lecture Spark avec schémas DDL
- [x] Module de nettoyage (9 fonctions)
- [x] Jointures (broadcast)
- [ ] Agrégations (groupBy, agg)
- [ ] Window functions
- [ ] SQL + createOrReplaceTempView
- [ ] I/O Parquet
- [ ] Détection d'anomalies avancée
- [ ] Structured Streaming
- [ ] Interface Streamlit
- [ ] Tests unitaires (pytest + chispa)
- [ ] CI/CD GitHub Actions
- [ ] Diagramme d'architecture
- [ ] Documentation finale

## Auteur

**Siham Bouzidi**
- GitHub : [@bouzidisiham](https://github.com/bouzidisiham)
- LinkedIn : [Siham Bouzidi](https://linkedin.com/in/siham-bouzidi-b87704174/)
- Email : bouzidisiham1997@gmail.com