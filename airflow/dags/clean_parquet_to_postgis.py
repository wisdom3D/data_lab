import os
import io
import logging
import pandas as pd
import boto3
from datetime import datetime, timedelta
from botocore.client import Config

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# ================================
# CONFIGURATION
# ================================
# Configuration MinIO (via Environment Variables)
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")

PG_CONN_ID = "postgres_analytics"
BUCKET_CLEAN = "clean"

def load_parquet_to_postgis_fn(**kwargs):
    # Client S3 avec Configuration Robuste
    s3_client = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version='s3v4', s3={'addressing_style': 'path'}),
        region_name='us-east-1'
    )

    # 2. Listing de TOUS les fichiers Parquet (Recursive scan)
    prefix = "events/"
    logging.info(f"Recherche récursive de TOUS les fichiers Parquet dans : {prefix}")
    
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=BUCKET_CLEAN, Prefix=prefix)

    pg_hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    engine = pg_hook.get_sqlalchemy_engine()
    
    total_rows = 0
    file_count = 0

    # 3. Ingestion des données
    for page in pages:
        if 'Contents' not in page:
            continue
            
        for obj in page['Contents']:
            if not obj['Key'].endswith('.parquet'):
                continue
                
            file_count += 1
            logging.info(f"Chargement [{file_count}]: {obj['Key']}...")
            file_obj = s3_client.get_object(Bucket=BUCKET_CLEAN, Key=obj['Key'])
            df = pd.read_parquet(io.BytesIO(file_obj['Body'].read()))

            if not df.empty:
                # Extraction de la catégorie depuis le key S3 si absente (ex: events/date=.../category=temperature/...)
                if 'category' not in df.columns or df['category'].isnull().all():
                    import re
                    match = re.search(r'category=([^/]+)/', obj['Key'])
                    if match:
                        df['category'] = match.group(1)
                
                # Insertion des données brutes dans le schéma datalab
                df.to_sql('raw_events', engine, if_exists='append', index=False, schema='datalab')
                total_rows += len(df)

    logging.info(f"{total_rows} lignes intégrées avec succès.")

# ================================
# DAG DEFINITION
# ================================

default_args = {
    "owner": "wis_data",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="load_events_full_pipeline",
    default_args=default_args,
    start_date=datetime(2026, 2, 16),
    schedule_interval="@daily",
    catchup=False,
    template_searchpath=['/opt/airflow/sql'] 
) as dag:

    start = EmptyOperator(task_id="start")

    # Tâche 1 : Création/Mise à jour de la structure SQL
    setup_table = PostgresOperator(
        task_id="setup_events_table",
        postgres_conn_id=PG_CONN_ID,
        sql="setup_events_table.sql"
    )

    # Tâche 2 : Ingestion Python
    ingest_data = PythonOperator(
        task_id="parquet_to_postgis",
        python_callable=load_parquet_to_postgis_fn,
        provide_context=True
    )

    end = EmptyOperator(task_id="end")

    start >> setup_table >> ingest_data >> end