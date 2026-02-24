import os
import io
import logging
import pandas as pd
import boto3
from datetime import datetime, timedelta
from botocore.exceptions import ClientError

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

# Configuration Postgres (via Airflow Connection)
PG_CONN_ID = "postgres_analytics"

BUCKET_RAW = "raw"
BUCKET_CLEAN = "clean"
CHUNK_SIZE = 10000
BAD_RECORDS_LOG_PATH = "/opt/airflow/logs/bad_records.log"

# ================================
# LOGIQUE DE LOGGING VERS POSTGRES
# ================================

def log_pipeline_metrics(run_id, dag_id, ds, read, written, errors, status, err_msg=None):
    """Enregistre les stats dans la table logging.pipeline_metrics via PostgresHook"""
    try:
        hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
        query = """
            INSERT INTO datalab.pipeline_metrics 
            (dag_id, run_id, table_target, execution_date, rows_read, rows_written, bad_records_count, status, error_message, finished_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (run_id, table_target) DO UPDATE SET
                rows_read = EXCLUDED.rows_read,
                rows_written = EXCLUDED.rows_written,
                bad_records_count = EXCLUDED.bad_records_count,
                status = EXCLUDED.status,
                finished_at = EXCLUDED.finished_at;
        """
        parameters = (dag_id, run_id, 'events', ds, read, written, errors, status, err_msg, datetime.now())
        hook.run(query, parameters=parameters)
    except Exception as e:
        logging.error(f"Erreur lors de l'écriture des logs dans Postgres: {e}")

# ================================
# FONCTION ETL PRINCIPALE
# ================================

def validate_and_partition_fn(**kwargs):
    ds = kwargs.get('ds')
    run_id = kwargs.get('run_id')
    dag_id = kwargs['dag'].dag_id
    source_key = f"events/date={ds}/events.csv"
    
    total_rows, total_written, bad_records = 0, 0, 0

    # Initialisation Client S3 avec Configuration Robuste
    # s3v4 + path-style addressing est la norme pour MinIO local
    s3_client = boto3.client(
        "s3", 
        endpoint_url=MINIO_ENDPOINT, 
        aws_access_key_id=MINIO_ACCESS_KEY, 
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=boto3.session.Config(
            signature_version='s3v4',
            s3={'addressing_style': 'path'}
        ),
        region_name='us-east-1'
    )
    
    # Setup bad records logger
    bad_logger = logging.getLogger("bad_records")
    if not bad_logger.handlers:
        handler = logging.FileHandler(BAD_RECORDS_LOG_PATH)
        handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))
        bad_logger.addHandler(handler)

    try:
        # Check Clean Bucket
        try: s3_client.head_bucket(Bucket=BUCKET_CLEAN)
        except ClientError: s3_client.create_bucket(Bucket=BUCKET_CLEAN)

        # Lecture Streamée depuis MinIO
        response = s3_client.get_object(Bucket=BUCKET_RAW, Key=source_key)
        csv_reader = pd.read_csv(response['Body'], chunksize=CHUNK_SIZE)

        logging.info(f"Début du traitement pour {source_key}")
        for i, chunk in enumerate(csv_reader):
            total_rows += len(chunk)
            
            # Validation
            mask_lat = chunk['latitude'].between(-90, 90)
            mask_lon = chunk['longitude'].between(-180, 180)
            mask_val = pd.to_numeric(chunk['value'], errors='coerce').fillna(-1) >= 0
            
            valid_mask = mask_lat & mask_lon & mask_val
            
            # Log bad records
            bad_df = chunk[~valid_mask]
            if not bad_df.empty:
                bad_records += len(bad_df)
                for idx, row in bad_df.iterrows():
                    # Identify specific reasons
                    reasons = []
                    if not mask_lat.loc[idx]: reasons.append("latitude invalide")
                    if not mask_lon.loc[idx]: reasons.append("longitude invalide")
                    if not mask_val.loc[idx]: reasons.append("valeur invalide")
                    
                    reason_str = ", ".join(reasons)
                    msg = f"Run:{run_id} | ID:{row.get('id', 'N/A')} | Rejet: {reason_str}"
                    
                    # Log to the specific file (Detailed)
                    bad_logger.warning(msg)

            # Transformation & Upload Parquet
            clean_chunk = chunk[valid_mask].copy()
            if not clean_chunk.empty:
                clean_chunk['timestamp'] = pd.to_datetime(clean_chunk['timestamp'])
                clean_chunk['date_str'] = clean_chunk['timestamp'].dt.strftime('%Y-%m-%d')

                for (date_val, category), group in clean_chunk.groupby(['date_str', 'category']):
                    target_key = f"events/date={date_val}/category={category}/part_{i}.parquet"
                    buf = io.BytesIO()
                    group.drop(columns=['date_str']).to_parquet(buf, index=False)
                    
                    # Chargement vers MinIO
                    s3_client.put_object(Bucket=BUCKET_CLEAN, Key=target_key, Body=buf.getvalue())
                    total_written += len(group)

        # Résumé des métriques dans les logs Airflow
        logging.info("=========================================")
        logging.info("RÉSUMÉ DE L'INGESTION")
        logging.info(f"Total lignes lues: {total_rows}")
        logging.info(f"Total lignes écrites: {total_written}")
        logging.info(f"Total mauvais enregistrements: {bad_records}")
        
        if total_rows > 0:
            error_rate = bad_records / total_rows
            if error_rate > 0.05:
                logging.warning(f"AVERTISSEMENT: Taux d'erreur élevé ({error_rate:.2%}) dépassant le seuil de 5%!")
        logging.info("=========================================")

        # Log Succès dans Postgres
        log_pipeline_metrics(run_id, dag_id, ds, total_rows, total_written, bad_records, "SUCCESS")

    except Exception as e:
        # Log Échec dans Postgres
        log_pipeline_metrics(run_id, dag_id, ds, total_rows, total_written, bad_records, "FAILED", str(e))
        raise

# ================================
# ORCHESTRATION
# ================================

with DAG(
    dag_id="validate_and_partition_events",
    default_args={"owner": "wis_data", "retries": 1, "retry_delay": timedelta(minutes=5)},
    start_date=datetime(2026, 2, 16),
    schedule_interval="@daily",
    catchup=False,
    template_searchpath=['/opt/airflow/sql']
) as dag:

    start = EmptyOperator(task_id="start")

    # 1. Assurer la présence de la table de logs
    setup_db = PostgresOperator(
        task_id="setup_logging_table",
        postgres_conn_id=PG_CONN_ID,
        sql="setup_logging.sql"
    )

    # 2. Exécuter l'ETL
    validate_task = PythonOperator(
        task_id="validate_and_partition_minio",
        python_callable=validate_and_partition_fn,
        provide_context=True,
    )

    end = EmptyOperator(task_id="end")

    start >> setup_db >> validate_task >> end