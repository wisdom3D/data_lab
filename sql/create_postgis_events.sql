-- Création du schéma si nécessaire
CREATE SCHEMA IF NOT EXISTS datalab;

-- Activation de PostGIS si nécessaire (nécessite les droits superuser)
CREATE EXTENSION IF NOT EXISTS postgis;

-- Création de la table avec spatialisation
CREATE TABLE IF NOT EXISTS datalab.events (
    id BIGINT PRIMARY KEY,
    timestamp TIMESTAMPTZ,
    geom GEOMETRY(POINT, 4326),
    value NUMERIC,
    category TEXT,
    source TEXT
);

-- Indexation spatiale
CREATE INDEX IF NOT EXISTS idx_events_geom ON datalab.events USING GIST (geom);
-- Indexation temporelle
CREATE INDEX IF NOT EXISTS idx_events_timestamp_tz ON datalab.events(timestamp);
