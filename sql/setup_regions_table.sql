-- Création du schéma si nécessaire
CREATE SCHEMA IF NOT EXISTS datalab;

-- Création de la table des régions prédéfinies
CREATE TABLE IF NOT EXISTS datalab.regions (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    geom GEOMETRY(GEOMETRY, 4326)
);

-- Indexation spatiale pour optimiser les requêtes de jointure spatiale (ex: point-in-polygon)
CREATE INDEX IF NOT EXISTS idx_regions_geom ON datalab.regions USING GIST (geom);