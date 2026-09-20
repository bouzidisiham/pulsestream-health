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
- Parquet
- pytest + chispa
- GitHub Actions (à venir)
- Faker

## Architecture : 
> ⚠️ Diagramme à venir

## Structure du projet

```
pulsestream-health/
├── src/pulsestream/    # Code source réutilisable
├── tests/              # Tests unitaires
├── scripts/            # Scripts CLI
├── data/               # Données (bronze/silver/gold)
├── notebooks/          # Exploration
└── docs/               # Documentation
```

## Installation : 
```bash
git clone ...
cd pulsestream-health
python -m venv .venv
.venv\Scripts\activate  # Windows
pip install -r requirements.txt
```

## Utilisation : 
- Générer les données : *(à venir)*
- Lancer le batch : *(à venir)*
- Lancer le streaming : *(à venir)*

## Tests : *(à venir)*

## Roadmap : 
- [x] Structure du projet
- [x] Dépendances (requirements)
- [x] .gitignore
- [ ] Générateur de données synthétiques
- [ ] Pipeline batch (bronze → silver → gold)
- [ ] Détection d'anomalies
- [ ] Structured Streaming
- [ ] Tests unitaires (pytest + chispa)
- [ ] CI/CD GitHub Actions
- [ ] Diagramme d'architecture
- [ ] Documentation finale

## Auteur

**Siham Bouzidi**
- GitHub : [@bouzidisiham](https://github.com/bouzidisiham)
- LinkedIn : [Siham Bouzidi](https://linkedin.com/in/siham-bouzidi-b87704174/)
- Email : bouzidisiham1997@gmail.com