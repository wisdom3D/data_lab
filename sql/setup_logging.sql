CREATE SCHEMA IF NOT EXISTS datalab;

CREATE TABLE IF NOT EXISTS datalab.pipeline_metrics (
    id SERIAL PRIMARY KEY,
    dag_id VARCHAR(100) NOT NULL,
    run_id VARCHAR(255) NOT NULL,
    table_target VARCHAR(50) NOT NULL,
    execution_date TIMESTAMP NOT NULL,
    rows_read INT DEFAULT 0,
    rows_written INT DEFAULT 0,
    bad_records_count INT DEFAULT 0,
    status VARCHAR(20) NOT NULL,
    error_message TEXT,
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    finished_at TIMESTAMP,
    CONSTRAINT unique_run UNIQUE (run_id, table_target)
);

CREATE INDEX IF NOT EXISTS idx_pipeline_metrics_date ON datalab.pipeline_metrics(execution_date);
CREATE INDEX IF NOT EXISTS idx_pipeline_metrics_status ON datalab.pipeline_metrics(status);