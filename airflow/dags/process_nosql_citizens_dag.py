import os
import io
import json
import logging
import pandas as pd
import boto3
from datetime import datetime, timedelta
from botocore.client import Config

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# ================================
# CONFIGURATION
# ================================
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")

PG_CONN_ID = "postgres_analytics"
BUCKET_RAW = "raw"
# Prefix corresponding to safe_name in data_to_minio_pipeline_dag
FILE_PREFIX = "demandes_services/"

def normalize_date(date_val):
    if not date_val:
        return None
    
    # Handle Unix timestamps (int or float)
    if isinstance(date_val, (int, float)):
        return datetime.fromtimestamp(date_val)
    
    # Handle Strings
    date_str = str(date_val).strip()
    formats = [
        "%Y-%m-%d",            # 2025-12-15
        "%d/%m/%Y",            # 18/11/2025
        "%Y/%m/%d",            # 2025/10/31
        "%Y-%m-%dT%H:%M:%S.%f", # 2025-11-03T05:45:27.245775
        "%Y-%m-%dT%H:%M:%S"
    ]
    
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    
    return None

def parse_citizen_report(doc):
    """
    Robust parser for heterogeneous formats in Togo Citizen Reports.
    Returns a dict with unified fields.
    """
    res = {
        "external_id": None,
        "source_timestamp": None,
        "category": None,
        "commune": None,
        "quartier": None,
        "status": None,
        "lat": None,
        "lon": None,
        "original_doc": json.dumps(doc)
    }

    # Format 1: {"event": {"id": ..., "timestamp": ..., "payload": {...}}}
    if "event" in doc:
        ev = doc["event"]
        res["external_id"] = ev.get("id")
        res["source_timestamp"] = normalize_date(ev.get("timestamp"))
        payload = ev.get("payload", {})
        res["category"] = payload.get("type")
        res["commune"] = payload.get("commune")
        res["quartier"] = payload.get("quartier")
    
    # Format 2: {"request_id": ..., "created_at": ..., "type": ...}
    elif "request_id" in doc:
        res["external_id"] = doc.get("request_id")
        res["source_timestamp"] = normalize_date(doc.get("created_at"))
        res["category"] = doc.get("type")
        res["commune"] = doc.get("city")
        res["quartier"] = doc.get("district")
        res["status"] = doc.get("status")

    # Format 3: {"id": ..., "date": ..., "category": ..., "geo": "lat,lon"}
    elif "id" in doc and "date" in doc:
        res["external_id"] = doc.get("id")
        res["source_timestamp"] = normalize_date(doc.get("date"))
        res["category"] = doc.get("category")
        res["status"] = doc.get("state")
        geo = doc.get("geo")
        if geo and isinstance(geo, str) and "," in geo:
            try:
                res["lat"], res["lon"] = map(float, geo.split(","))
            except: pass

    # Format 4: {"_id": "req_...", "date_creation": ..., "type_demande": ...}
    elif "_id" in doc and "type_demande" in doc:
        res["external_id"] = doc.get("_id")
        res["source_timestamp"] = normalize_date(doc.get("date_creation"))
        res["category"] = doc.get("type_demande")
        res["status"] = doc.get("statut")
        loc = doc.get("localisation", {})
        if isinstance(loc, dict):
            res["commune"] = loc.get("commune")
            res["quartier"] = loc.get("quartier")

    # Format 5: {"ref": ..., "date_signalement": ..., "service_type": ...}
    elif "ref" in doc:
        res["external_id"] = doc.get("ref")
        res["source_timestamp"] = normalize_date(doc.get("date_signalement"))
        res["category"] = doc.get("service_type")
        res["status"] = doc.get("status")
        loc = doc.get("localisation")
        if loc and " - " in loc:
            parts = loc.split(" - ")
            res["quartier"] = parts[0]
            res["commune"] = parts[1]

    # Format 6: {"_id": "REQ-...", "event_date": ..., "service": ...}
    elif "_id" in doc and "service" in doc:
        res["external_id"] = doc.get("_id")
        res["source_timestamp"] = normalize_date(doc.get("event_date"))
        res["category"] = doc.get("service")
        loc = doc.get("location", {})
        if isinstance(loc, dict):
            res["commune"] = loc.get("commune")

    # Format 7: {"created": ..., "type_demande": ...} (Minimal)
    elif "created" in doc:
        res["source_timestamp"] = normalize_date(doc.get("created"))
        res["category"] = doc.get("type_demande")

    return res

def process_nosql_data_fn(**kwargs):
    s3_client = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version='s3v4', s3={'addressing_style': 'path'}),
        region_name='us-east-1'
    )

    logging.info(f"Scanning for NoSQL data in MinIO bucket '{BUCKET_RAW}' with prefix '{FILE_PREFIX}'")
    
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=BUCKET_RAW, Prefix=FILE_PREFIX)

    processed_records_count = 0
    
    pg_hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    engine = pg_hook.get_sqlalchemy_engine()

    for page in pages:
        if 'Contents' not in page:
            continue
            
        for obj in page['Contents']:
            if not obj['Key'].endswith('.json'):
                continue
                
            logging.info(f"Processing object: {obj['Key']}")
            try:
                file_obj = s3_client.get_object(Bucket=BUCKET_RAW, Key=obj['Key'])
                data = json.loads(file_obj['Body'].read())
                
                # If the file contains a single list of objects (common format)
                if isinstance(data, list):
                    records = [parse_citizen_report(doc) for doc in data]
                else:
                    records = [parse_citizen_report(data)]
                
                if not records:
                    continue

                df = pd.DataFrame(records)
                df['category'] = df['category'].str.lower()
                
                # Deduplication: Ensure external_id is unique per batch to avoid CardinalityViolation
                # We drop rows with null external_id if they are frequent, or keep them if they are unique
                df = df.dropna(subset=['external_id'])
                df = df.drop_duplicates(subset=['external_id'], keep='last')
                
                if df.empty:
                    logging.info(f"No valid records found in {obj['Key']} after deduplication.")
                    continue

                # Use temporary table for Upsert logic
                temp_table = f"temp_{obj['Key'].replace('/', '_').replace('.', '_').replace('=', '_').replace('-', '_')}"
                df.to_sql(temp_table, engine, if_exists='replace', index=False)
                
                with engine.connect() as conn:
                    upsert_query = f"""
                        INSERT INTO datalab.citizen_reports (
                            external_id, source_timestamp, category, commune, quartier, status, geom, original_doc
                        )
                        SELECT 
                            external_id, 
                            source_timestamp, 
                            category, 
                            commune, 
                            quartier, 
                            status,
                            CASE 
                                WHEN lat IS NOT NULL AND lon IS NOT NULL THEN ST_SetSRID(ST_MakePoint(lon, lat), 4326)
                                ELSE NULL 
                            END,
                            original_doc::JSONB
                        FROM {temp_table}
                        ON CONFLICT (external_id) DO UPDATE SET
                            source_timestamp = EXCLUDED.source_timestamp,
                            category = EXCLUDED.category,
                            commune = EXCLUDED.commune,
                            quartier = EXCLUDED.quartier,
                            status = EXCLUDED.status,
                            geom = EXCLUDED.geom,
                            original_doc = EXCLUDED.original_doc;
                        
                        DROP TABLE {temp_table};
                    """
                    conn.execute(upsert_query)
                    processed_records_count += len(df)
            
            except Exception as e:
                logging.error(f"Error processing {obj['Key']}: {e}")

    logging.info(f"Successfully processed {processed_records_count} total records.")

# ================================
# DAG DEFINITION
# ================================

default_args = {
    "owner": "wis_data",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="nosql_citizen_reports_processing",
    default_args=default_args,
    start_date=datetime(2026, 2, 16),
    schedule_interval="@daily",
    catchup=False,
    template_searchpath=['/opt/airflow/sql'] 
) as dag:

    start = EmptyOperator(task_id="start")

    setup_schema = SQLExecuteQueryOperator(
        task_id="setup_citizen_reports_schema",
        conn_id=PG_CONN_ID,
        sql="setup_citizen_reports.sql"
    )

    ingest_nosql = PythonOperator(
        task_id="process_nosql_json",
        python_callable=process_nosql_data_fn,
        provide_context=True
    )

    end = EmptyOperator(task_id="end")

    start >> setup_schema >> ingest_nosql >> end
