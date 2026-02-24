-- Création du schéma si nécessaire
CREATE SCHEMA IF NOT EXISTS datalab;

-- Création de la table avec les types de données précis
CREATE TABLE IF NOT EXISTS datalab.raw_events (
    id VARCHAR(100),
    timestamp TIMESTAMP,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    value DOUBLE PRECISION,
    category VARCHAR(50),
    source VARCHAR(50)
);


-- Indexation sur le timestamp pour les analyses temporelles
CREATE INDEX IF NOT EXISTS idx_events_timestamp ON datalab.raw_events(timestamp);