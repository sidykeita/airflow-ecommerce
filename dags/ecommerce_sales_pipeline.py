"""
DAG : ecommerce_sales_pipeline

Pipeline d'analyse des ventes e-commerce : detection du fichier source,
controle qualite, calcul des indicateurs metier, analyse par categorie,
generation d'un rapport et stockage dans MongoDB.

Les chemins de fichiers et l'URI MongoDB sont lus depuis des variables
d'environnement pour rester portables entre l'execution locale et Docker.
"""

import os
import json
import logging
from datetime import datetime

import pandas as pd
from pymongo import MongoClient

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.trigger_rule import TriggerRule

from scripts.pipeline_logic import filter_valid_rows, compute_kpis, analyze_category as analyze_category_logic

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
DATA_DIR = os.environ.get("PIPELINE_DATA_DIR", "/opt/airflow/data")
INPUT_FILE = os.path.join(DATA_DIR, "dataset.csv")
CLEAN_FILE = os.path.join(DATA_DIR, "dataset_clean.csv")
ERROR_FILE = os.path.join(DATA_DIR, "errors.csv")

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://mongodb:27017/")
MONGO_DB = "ecommerce_analytics"
MONGO_COLLECTION = "sales_metrics"

# Categories connues a l'avance (voir scripts/prepare_dataset.py) : on les
# fige ici en dur pour generer les taches sans avoir a lire le fichier de
# donnees au moment ou Airflow *parse* le DAG (le fichier peut ne pas encore
# exister a ce moment-la).
CATEGORIES = [
    "Deco", "Cuisine", "Bijoux_Accessoires",
    "Papeterie", "Jouets_Loisirs", "Textile", "Autre",
]

default_args = {
    "owner": "sidi",
    "retries": 1,
}

dag = DAG(
    dag_id="ecommerce_sales_pipeline",
    description="Pipeline d'analyse des ventes e-commerce (Olist/Online Retail)",
    default_args=default_args,
    schedule_interval=None,   # declenchement manuel / via Jenkins uniquement
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["ecommerce", "jenkins", "mongodb"],
)


# ---------------------------------------------------------------------------
# Exigence 6.2 - point 2 : verifier l'existence du fichier
# ---------------------------------------------------------------------------
def check_file_exists(**context):
    exists = os.path.isfile(INPUT_FILE)
    context["ti"].xcom_push(key="file_exists", value=exists)
    logger.info("Fichier %s existe : %s", INPUT_FILE, exists)


# ---------------------------------------------------------------------------
# Exigence 6.2 - point 3 : verifier que le fichier n'est pas vide
# ---------------------------------------------------------------------------
def check_file_not_empty(**context):
    ti = context["ti"]
    file_exists = ti.xcom_pull(task_ids="check_file_exists", key="file_exists")

    if not file_exists:
        ti.xcom_push(key="is_empty", value=True)
        ti.xcom_push(key="row_count", value=0)
        return

    df = pd.read_csv(INPUT_FILE)
    row_count = len(df)
    ti.xcom_push(key="is_empty", value=row_count == 0)
    ti.xcom_push(key="row_count", value=row_count)
    logger.info("Nombre de lignes lues : %s", row_count)


# ---------------------------------------------------------------------------
# Exigence 6.2 - point 4 : controler la qualite des donnees
# Regles de gestion (section 8 du sujet) :
#   - identifiant de commande manquant -> ligne invalide
#   - montant negatif -> ligne rejetee
#   - quantite nulle ou negative -> ligne invalide
# ---------------------------------------------------------------------------
def check_data_quality(**context):
    ti = context["ti"]
    is_empty = ti.xcom_pull(task_ids="check_file_not_empty", key="is_empty")

    if is_empty:
        ti.xcom_push(key="valid_rows", value=0)
        ti.xcom_push(key="invalid_rows", value=0)
        return

    df = pd.read_csv(INPUT_FILE)

    valid_df, invalid_df = filter_valid_rows(df)

    valid_df.to_csv(CLEAN_FILE, index=False, encoding="utf-8")
    invalid_df.to_csv(ERROR_FILE, index=False, encoding="utf-8")

    ti.xcom_push(key="valid_rows", value=len(valid_df))
    ti.xcom_push(key="invalid_rows", value=len(invalid_df))
    logger.info("Lignes valides : %s / invalides : %s", len(valid_df), len(invalid_df))


# ---------------------------------------------------------------------------
# Exigence 6.2 - point 5 : BranchPythonOperator
# ---------------------------------------------------------------------------
def decide_execution_path(**context):
    ti = context["ti"]
    is_empty = ti.xcom_pull(task_ids="check_file_not_empty", key="is_empty")
    valid_rows = ti.xcom_pull(task_ids="check_data_quality", key="valid_rows")

    if is_empty or not valid_rows:
        return "stop_pipeline"
    return "load_data"


def stop_pipeline(**context):
    context["ti"].xcom_push(key="final_status", value="failed")
    logger.warning("Pipeline arrete proprement : fichier vide ou aucune ligne valide.")


# ---------------------------------------------------------------------------
# Exigence 6.2 - point 6 : charger les donnees
# ---------------------------------------------------------------------------
def load_data(**context):
    df = pd.read_csv(CLEAN_FILE)
    context["ti"].xcom_push(key="loaded_rows", value=len(df))
    logger.info("Donnees chargees : %s lignes", len(df))


# ---------------------------------------------------------------------------
# Exigence 6.2 - point 7 + section 7 : calcul des indicateurs metier globaux
# ---------------------------------------------------------------------------
def compute_global_kpis(**context):
    df = pd.read_csv(CLEAN_FILE)

    global_metrics = compute_kpis(df)

    top_products = (
        df.groupby("Produit")
        .agg(sales=("Quantite", "sum"), revenue=("Montant", "sum"))
        .sort_values("revenue", ascending=False)
        .head(10)
        .reset_index()
        .to_dict("records")
    )

    region_metrics = (
        df.groupby("Region")
        .agg(orders=("ID_Commande", "nunique"), revenue=("Montant", "sum"))
        .sort_values("revenue", ascending=False)
        .reset_index()
        .to_dict("records")
    )

    monthly = df.copy()
    monthly["month"] = pd.to_datetime(monthly["Date"]).dt.to_period("M").astype(str)
    monthly_evolution = (
        monthly.groupby("month")
        .agg(revenue=("Montant", "sum"), orders=("ID_Commande", "nunique"))
        .reset_index()
        .to_dict("records")
    )

    ti = context["ti"]
    ti.xcom_push(key="global_metrics", value=global_metrics)
    ti.xcom_push(key="top_products", value=top_products)
    ti.xcom_push(key="region_metrics", value=region_metrics)
    ti.xcom_push(key="monthly_evolution", value=monthly_evolution)
    logger.info("KPI globaux calcules : %s", global_metrics)


# ---------------------------------------------------------------------------
# Exigence 6.2 - point 9 : taches dynamiques d'analyse par categorie
# ---------------------------------------------------------------------------
def analyze_category(category: str, **context):
    df = pd.read_csv(CLEAN_FILE)
    result = analyze_category_logic(df, category)
    context["ti"].xcom_push(key=f"category_{category}", value=result)
    logger.info("Categorie %s : %s", category, result)


# ---------------------------------------------------------------------------
# Exigence 6.2 - point 11 : rapport final (agrege tout, gere failed/partial)
# ---------------------------------------------------------------------------
def generate_report(**context):
    ti = context["ti"]

    final_status_forced = ti.xcom_pull(task_ids="stop_pipeline", key="final_status")
    if final_status_forced == "failed":
        report = {
            "execution_date": context["ds"],
            "dag_id": dag.dag_id,
            "dataset": "online_retail",
            "source_file": os.path.basename(INPUT_FILE),
            "status": "failed",
            "global_metrics": {},
            "top_products": [],
            "region_metrics": [],
            "category_metrics": [],
            "quality": {"valid_rows": 0, "invalid_rows": 0, "error_file": "errors.csv"},
        }
        ti.xcom_push(key="report", value=report)
        return

    global_metrics = ti.xcom_pull(task_ids="compute_global_kpis", key="global_metrics")
    top_products = ti.xcom_pull(task_ids="compute_global_kpis", key="top_products")
    region_metrics = ti.xcom_pull(task_ids="compute_global_kpis", key="region_metrics")
    valid_rows = ti.xcom_pull(task_ids="check_data_quality", key="valid_rows") or 0
    invalid_rows = ti.xcom_pull(task_ids="check_data_quality", key="invalid_rows") or 0

    category_metrics = []
    failed_categories = 0
    for category in CATEGORIES:
        result = ti.xcom_pull(task_ids=f"analyze_category_{category}", key=f"category_{category}")
        if result is None:
            failed_categories += 1
        else:
            category_metrics.append(result)

    status = "success" if failed_categories == 0 else "partial"

    report = {
        "execution_date": context["ds"],
        "dag_id": dag.dag_id,
        "dataset": "online_retail",
        "source_file": os.path.basename(INPUT_FILE),
        "status": status,
        "global_metrics": global_metrics,
        "top_products": top_products,
        "region_metrics": region_metrics,
        "category_metrics": category_metrics,
        "quality": {
            "valid_rows": int(valid_rows),
            "invalid_rows": int(invalid_rows),
            "error_file": "errors.csv",
        },
    }

    ti.xcom_push(key="report", value=report)
    logger.info("Rapport final : statut=%s", status)


# ---------------------------------------------------------------------------
# Exigence 6.2 - point 12 : stockage MongoDB
# ---------------------------------------------------------------------------
def store_mongodb(**context):
    report = context["ti"].xcom_pull(task_ids="generate_report", key="report")

    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    try:
        db = client[MONGO_DB]
        collection = db[MONGO_COLLECTION]
        collection.insert_one(dict(report))
        logger.info("Document insere dans %s.%s", MONGO_DB, MONGO_COLLECTION)
    finally:
        client.close()


# ---------------------------------------------------------------------------
# Definition des taches
# ---------------------------------------------------------------------------

# Exigence 6.2 - point 1 : FileSensor
wait_for_file = FileSensor(
    task_id="wait_for_file",
    filepath=INPUT_FILE,
    fs_conn_id="fs_default",
    poke_interval=10,
    timeout=60,
    mode="poke",
    dag=dag,
)

t_check_exists = PythonOperator(
    task_id="check_file_exists",
    python_callable=check_file_exists,
    dag=dag,
)

t_check_not_empty = PythonOperator(
    task_id="check_file_not_empty",
    python_callable=check_file_not_empty,
    dag=dag,
)

t_check_quality = PythonOperator(
    task_id="check_data_quality",
    python_callable=check_data_quality,
    dag=dag,
)

t_branch = BranchPythonOperator(
    task_id="decide_execution_path",
    python_callable=decide_execution_path,
    dag=dag,
)

t_stop_pipeline = PythonOperator(
    task_id="stop_pipeline",
    python_callable=stop_pipeline,
    dag=dag,
)

t_load_data = PythonOperator(
    task_id="load_data",
    python_callable=load_data,
    dag=dag,
)

t_compute_kpis = PythonOperator(
    task_id="compute_global_kpis",
    python_callable=compute_global_kpis,
    dag=dag,
)

category_tasks = []
for cat in CATEGORIES:
    category_tasks.append(
        PythonOperator(
            task_id=f"analyze_category_{cat}",
            python_callable=analyze_category,
            op_kwargs={"category": cat},
            dag=dag,
        )
    )

# Exigence 6.2 - point 10 : Trigger Rules -> ces deux taches s'executent
# meme si des taches en amont ont echoue ou ete sautees (branche non prise).
t_generate_report = PythonOperator(
    task_id="generate_report",
    python_callable=generate_report,
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag,
)

t_store_mongodb = PythonOperator(
    task_id="store_mongodb",
    python_callable=store_mongodb,
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag,
)

# ---------------------------------------------------------------------------
# Dependances
# ---------------------------------------------------------------------------
wait_for_file >> t_check_exists >> t_check_not_empty >> t_check_quality >> t_branch
t_branch >> t_stop_pipeline >> t_generate_report
t_branch >> t_load_data >> t_compute_kpis
t_load_data >> category_tasks
category_tasks >> t_generate_report
t_compute_kpis >> t_generate_report
t_generate_report >> t_store_mongodb