-- Création du schéma si nécessaire pour séparer les logs des données métier
CREATE SCHEMA IF NOT EXISTS logging;

-- Création de la table de monitoring des pipelines
-- Cette table permet de suivre l'état de santé de l'ingestion (Exercice 1 & 2)
CREATE TABLE IF NOT EXISTS logging.pipeline_metrics (
    id SERIAL PRIMARY KEY,
    dag_id VARCHAR(100) NOT NULL,          -- Nom du DAG Airflow
    run_id VARCHAR(255) NOT NULL,          -- ID unique de l'exécution (Airflow run_id)
    table_target VARCHAR(50) NOT NULL,     -- Table concernée (ex: events, services_publics)
    execution_date TIMESTAMP NOT NULL,      -- Date logique de l'exécution (ds)
    
    -- Métriques de volumétrie
    rows_read INT DEFAULT 0,               -- Lignes lues depuis le Raw (MinIO)
    rows_written INT DEFAULT 0,            -- Lignes écrites vers le Clean/Analytics
    bad_records_count INT DEFAULT 0,       -- Lignes rejetées (Data Quality)
    
    -- Statuts et Audit
    status VARCHAR(20) NOT NULL,           -- SUCCESS, FAILED, WARNING
    error_message TEXT,                    -- Détails en cas d'échec
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    finished_at TIMESTAMP,
    
    -- Contrainte pour éviter les doublons sur un même run
    CONSTRAINT unique_run UNIQUE (run_id, table_target)
);

-- Index pour accélérer la lecture des rapports de performance dans Superset
CREATE INDEX IF NOT EXISTS idx_pipeline_metrics_date ON logging.pipeline_metrics(execution_date);
CREATE INDEX IF NOT EXISTS idx_pipeline_metrics_status ON logging.pipeline_metrics(status);

-- Commentaire pour la documentation de la base
COMMENT ON TABLE logging.pipeline_metrics IS 'Table de monitoring pour le suivi de la qualité et des performances des pipelines ETL du Togo Data Lab.';