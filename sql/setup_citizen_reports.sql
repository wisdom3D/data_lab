-- Unified schema for citizen reports (NoSQL Ingestion)
CREATE SCHEMA IF NOT EXISTS datalab;

CREATE TABLE IF NOT EXISTS datalab.citizen_reports (
    id SERIAL PRIMARY KEY,
    external_id TEXT UNIQUE,
    source_timestamp TIMESTAMPTZ,
    category TEXT,
    commune TEXT,
    quartier TEXT,
    status TEXT,
    geom GEOMETRY(POINT, 4326),
    original_doc JSONB,
    ingested_at TIMESTAMPTZ DEFAULT NOW()
);

-- Indices for performance
CREATE INDEX IF NOT EXISTS idx_citizen_reports_geom ON datalab.citizen_reports USING GIST (geom);
CREATE INDEX IF NOT EXISTS idx_citizen_reports_timestamp ON datalab.citizen_reports (source_timestamp);
CREATE INDEX IF NOT EXISTS idx_citizen_reports_category ON datalab.citizen_reports (category);
CREATE INDEX IF NOT EXISTS idx_citizen_reports_json ON datalab.citizen_reports USING GIN (original_doc);
