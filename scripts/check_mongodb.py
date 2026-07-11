"""
Verifie qu'un document recent existe dans MongoDB pour le DAG donne.

Utilise comme porte de qualite par Jenkins (stage "Verify MongoDB") :
le build echoue (exit code 1) si aucun document recent n'est trouve,
ce qui prouve que l'execution Airflow a bien stocke ses metriques.

Usage :
    python scripts/check_mongodb.py --dag-id ecommerce_sales_pipeline --max-age-minutes 30
"""

import argparse
import os
import sys
from datetime import datetime, timedelta, timezone

from pymongo import MongoClient

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017/")
DB_NAME = "ecommerce_analytics"
COLLECTION_NAME = "sales_metrics"


def check_recent_document(dag_id: str, max_age_minutes: int) -> bool:
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    try:
        collection = client[DB_NAME][COLLECTION_NAME]
        doc = collection.find_one({"dag_id": dag_id}, sort=[("_id", -1)])

        if doc is None:
            print(f"Aucun document trouve pour dag_id={dag_id}")
            return False

        print("Dernier document trouve :")
        print(f"  execution_date : {doc.get('execution_date')}")
        print(f"  status         : {doc.get('status')}")
        print(f"  global_metrics : {doc.get('global_metrics')}")

        insertion_time = doc["_id"].generation_time
        age = datetime.now(timezone.utc) - insertion_time
        if age > timedelta(minutes=max_age_minutes):
            print(f"Document trouve mais trop ancien ({age}) > {max_age_minutes} min")
            return False

        return doc.get("status") in ("success", "partial")
    finally:
        client.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dag-id", default="ecommerce_sales_pipeline")
    parser.add_argument("--max-age-minutes", type=int, default=30)
    args = parser.parse_args()

    ok = check_recent_document(args.dag_id, args.max_age_minutes)
    sys.exit(0 if ok else 1)