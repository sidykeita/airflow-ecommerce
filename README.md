# Pipeline E-commerce industrialise (Airflow + Jenkins + MongoDB + Docker)

Projet de Data Engineering : automatisation d'un pipeline d'analyse des ventes
e-commerce, de l'ingestion du fichier CSV jusqu'au stockage des indicateurs
metier dans MongoDB, orchestre par Apache Airflow et deploye via Jenkins.

## Contexte

Une entreprise e-commerce traite aujourd'hui ses ventes manuellement a partir
de fichiers CSV exportes quotidiennement. Ce projet remplace ce traitement
manuel par un pipeline automatise, fiable et tracable.

## Dataset

[Online Retail Dataset (UCI)](https://archive.ics.uci.edu/dataset/352/online+retail),
retraite pour respecter le schema minimal demande :
`Date, Produit, Quantite, Prix, Montant, Region, Client`.

## Architecture

```
Git repo -> Jenkins -> Apache Airflow (LocalExecutor) -> MongoDB
```

- **Jenkins** : checkout, tests, validation du DAG, deploiement, declenchement
  du DAG via l'API REST Airflow, verification MongoDB.
- **Airflow** : detection du fichier, controle qualite, calcul des indicateurs,
  stockage MongoDB.
- **MongoDB** : base `ecommerce_analytics`, collection `sales_metrics`.

## Structure du projet

```
airflow-ecommerce-project/
├── dags/                  # DAG Airflow (ecommerce_sales_pipeline.py)
├── tests/                 # Tests pytest
├── data/                  # Dataset source + fichiers d'erreurs generes
├── scripts/               # Scripts utilitaires (prepa dataset, check MongoDB)
├── docker/                # Dockerfiles (Airflow, Jenkins)
├── rapport/captures/      # Captures d'ecran pour le rapport
├── Jenkinsfile
├── docker-compose.yml
└── requirements.txt
```

## Lancer le projet

_Section completee a l'etape Docker Compose du projet (a venir)._

## Indicateurs calcules

Nombre de commandes, nombre de clients, chiffre d'affaires total, panier
moyen, top 10 produits, CA par categorie, CA par region, evolution mensuelle,
lignes valides / rejetees.

## Regles de gestion

- Montant negatif -> ligne rejetee
- Quantite nulle ou negative -> ligne invalide
- Fichier vide -> arret propre du workflow
- Lignes incorrectes -> isolees dans `data/errors.csv`
- Statut de traitement historise : `success`, `failed`, `partial`
