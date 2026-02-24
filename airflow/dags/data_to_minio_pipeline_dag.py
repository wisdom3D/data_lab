import os
import json
import logging
import boto3
from datetime import datetime, timedelta
from botocore.exceptions import ClientError
from boto3.s3.transfer import TransferConfig

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

# ================================
# CONFIGURATION
# ================================
# Configuration MinIO (via Environment Variables)
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
BUCKET_NAME = "raw"
JSON_CONFIG_PATH = os.getenv("JSON_CONFIG_PATH", "/opt/airflow/data/sources.json")

# ================================
# LOGIQUE D'UPLOAD
# ================================

def upload_file_to_minio(file_name, local_path, **kwargs):
    """Effectue l'upload vers MinIO"""
    logging.info(f"Début de l'upload pour : {file_name}")

    if not os.path.exists(local_path):
        raise FileNotFoundError(f"Fichier source introuvable : {local_path}")

    s3_client = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )

    try:
        s3_client.head_bucket(Bucket=BUCKET_NAME)
    except ClientError:
        logging.info(f"Création du bucket {BUCKET_NAME}")
        s3_client.create_bucket(Bucket=BUCKET_NAME)

    # Partitionnement Hive-style
    # On utilise la date logique d'Airflow (ds) pour l'idempotence
    execution_date = kwargs.get('ds') 
    extension = os.path.splitext(local_path)[1]
    object_name = f"{file_name}/date={execution_date}/{file_name}{extension}"

    transfer_config = TransferConfig(
        multipart_threshold=25 * 1024 * 1024,
        max_concurrency=5,
    )

    s3_client.upload_file(local_path, BUCKET_NAME, object_name, Config=transfer_config)
    logging.info(f"Terminé : {object_name}")

# ================================
# DEFINITION DU DAG
# ================================

default_args = {
    "owner": "wis_data",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="raw_data_ingestion_minio",
    default_args=default_args,
    start_date=datetime(2026, 2, 16),
    schedule_interval="@daily",
    catchup=False,
    tags=["raw_data", "sequential", "csv", "json"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # Protection : Lecture sécurisée du JSON
    sources = []
    if os.path.exists(JSON_CONFIG_PATH):
        try:
            with open(JSON_CONFIG_PATH, 'r') as f:
                sources = json.load(f)
        except Exception as e:
            logging.error(f"Erreur lors de la lecture du JSON : {e}")

    # --- LA BOUCLE SEQUENTIELLE ---
    if not sources:
        # Tâche de secours si aucune source n'est trouvée
        start >> end
    else:
        previous_task = start
        for source in sources:
            # Nettoyage du nom pour éviter les IDs de tâche invalides
            safe_name = source['name'].replace(" ", "_").lower()
            task_id = f"upload_{safe_name}"
            
            current_task = PythonOperator(
                task_id=task_id,
                python_callable=upload_file_to_minio,
                op_kwargs={
                    "file_name": safe_name,
                    "local_path": source['path']
                },
                # On active l'accès au contexte pour récupérer 'ds'
                provide_context=True,
            )

            previous_task >> current_task
            previous_task = current_task

        previous_task >> end