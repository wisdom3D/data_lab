# Data Lab POC

A modern data platform built for ingesting, processing, and visualizing event data using a robust open-source stack.

## Architecture

The platform follows a classic Data Lakehouse pattern with a Medallion-like architecture (Raw -> Clean -> Curated).

## Infrastructure & Services

The following services are orchestrated via Docker Compose:

| Service | Port | Description |
| :--- | :--- | :--- |
| **PostgreSQL** | `5432` | Data Warehouse (PostGIS enabled) |
| **MinIO** | `9000/9001` | S3-compatible Object Storage (Data Lake) |
| **Airflow** | `8080` | Workflow Orchestration |
| **Superset** | `8088` | Data Visualization & BI |
| **pgAdmin** | `5050` | PostgreSQL Management UI |
| **Redis** | `6379` | Message Broker for Airflow/Celery |

## Getting Started

### Prerequisites
- Docker & Docker Compose
- Python 3.9+ (local development)

### Setup
1. **Clone the repository**
2. **Configure Environment**
   Create a `.env` file based on the provided template:
   ```bash
   cp .env.example .env
   ```
3. **Start the services**
   ```bash
   docker-compose up -d
   ```
4. **Access the platforms**
   - Airflow: [http://localhost:8080](http://localhost:8080)
   - Superset: [http://localhost:8088](http://localhost:8088)
   - MinIO Console: [http://localhost:9001](http://localhost:9001)

## Data Pipelines (DAGs)

- **`raw_data_ingestion_minio`**: Ingests files from local sources into the MinIO `raw` bucket.
- **`clean_parquet_to_postgis`**: Processes raw Parquet data from MinIO and loads it into the PostGIS database.
- **`process_raw_to_postgis_dag.py`**: A dedicated DAG for raw-to-db processing.
- **`validate_and_partition.py`**: Ensures data quality and manages database partitioning.

## Project Structure

- `airflow/dags/`: Data pipeline definitions.
- `sql/`: Database schemas and migration scripts.
- `data/`: Local landing zone for raw data (excluded from Git).
- `docker-compose.yml`: Infrastructure as code.
