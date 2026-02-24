import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

# ================================
# CONFIGURATION
# ================================
PG_CONN_ID = "postgres_analytics"

default_args = {
    "owner": "wis_data",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="postgis_events_processing",
    default_args=default_args,
    start_date=datetime(2026, 2, 16),
    schedule_interval="@daily",
    catchup=False,
    template_searchpath=['/opt/airflow/sql'] 
) as dag:

    start = EmptyOperator(task_id="start")

    setup_postgis_table = SQLExecuteQueryOperator(
        task_id="setup_postgis_events_table",
        conn_id=PG_CONN_ID,
        sql="create_postgis_events.sql"
    )

    # 1alt. S'assurer que la table regions existe
    setup_regions_table = SQLExecuteQueryOperator(
        task_id="setup_regions_table",
        conn_id=PG_CONN_ID,
        sql="setup_regions_table.sql"
    )

    # 2. Transformer raw_events en events (Upsert logic)
    # Note: On cast les types pour correspondre au schéma cible
    process_spatial_data = SQLExecuteQueryOperator(
        task_id="raw_to_spatial_events",
        conn_id=PG_CONN_ID,
        sql="""
            INSERT INTO datalab.events (id, timestamp, geom, value, category, source)
            SELECT 
                id::BIGINT,
                timestamp AT TIME ZONE 'UTC',
                ST_SetSRID(ST_MakePoint(longitude, latitude), 4326),
                value::NUMERIC,
                category,
                source
            FROM datalab.raw_events
            ON CONFLICT (id) DO UPDATE SET
                timestamp = EXCLUDED.timestamp,
                geom = EXCLUDED.geom,
                value = EXCLUDED.value,
                category = EXCLUDED.category,
                source = EXCLUDED.source;
        """
    )

    end = EmptyOperator(task_id="end")

    start >> [setup_postgis_table, setup_regions_table] >> process_spatial_data >> end
