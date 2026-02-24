# Documentation Technique - Data Lab POC

Ce document présente la solution technique pour le projet de collecte et d'analyse des politiques publiques au Togo, en mettant l'accent sur les critères d'évaluation de l'architecture, de la robustesse et de la productivité.

---

## 1. Cohérence Globale de l'Architecture
La solution repose sur une **Architecture Medallion** moderne, conçue pour séparer les responsabilités et assurer la traçabilité de la donnée :

- **Bronze (Brut)** : Stockage immuable des fichiers JSON, CSV et Parquet dans **MinIO**. Conservation de l'état original pour replay ou audit.
- **Silver (Structuré)** : Normalisation et typage dans **PostgreSQL/PostGIS**. C'est ici que la donnée "NoSQL" devient exploitable via des schémas relationnels tout en conservant sa flexibilité (JSONB).
- **Gold (Analytique)** : Vues et agrégations spatiales prêtes pour la visualisation dans **Superset**.

### Diagramme d'Architecture
![Architecture Globale](images/datalab_poc_HL.png)

---

## 2. Pertinence des Choix Techniques

| Outil | Rôle | Justification |
| :--- | :--- | :--- |
| **Airflow** | Orchestration | Permet une gestion fine des dépendances, des reprises sur erreur (retries) et une visibilité complète sur les pipelines (logs). |
| **MinIO** | Data Lake | Compatible S3, idéal pour stocker des volumes de données semi-structurées de manière économique et scalable. |
| **PostGIS** | Base Spatiale | Indispensable pour l'analyse territoriale (communes, quartiers) grâce à ses fonctions de jointure géographique (`ST_Within`). |
| **JSONB (Postgres)** | Stockage NoSQL | Offre le meilleur des deux mondes : la puissance du SQL relationnel pour les rapports et la flexibilité du NoSQL pour l'évolution des schémas. |
| **Docker Compose** | Déploiement | Garantit que l'environnement de développement est identique à la pré-production (Infrastructure as Code). |

---

## 3. Robustesse face aux Données Imparfaites
Le pipeline de l'Exercice 2 a été conçu pour être résilient face à l'hétérogénéité des données (7+ formats identifiés) :

- **Normalisation Multi-Format** : Un parseur Python agile gère les formats de date disparates (Unix, ISO, DD/MM/YYYY) et les structures géographiques variées (objet vs string).
- **Déduplication au Niveau du Pipeline** : Gestion automatique des `CardinalityViolation` via une déduplication par `external_id` avec Pandas avant l'UPSERT final.
- **Schéma-on-Read Partiel** : Les champs critiques sont extraits mathématiquement (Coordonnées -> Géométrie) tandis que le reste du document est préservé dans une colonne `JSONB` pour éviter toute perte d'information non prévue.
- **Indempotence** : Utilisation systématique du `ON CONFLICT DO UPDATE` pour permettre des relances de pipeline sans doublons.

---

## 4. Capacité de Mise en Œuvre en Production
Pour transformer ce POC en environnement de production réel :

- **Isolation des Dépendances** : Création d'un `superset.Dockerfile` dédié pour inclure les pilotes de base de données, évitant les installations volatiles au runtime.
- **Sécurisation via .env** : Externalisation complète des secrets (DB, MinIO, Airflow) pour une configuration par environnement.
- **Hive-Style Partitioning** : Organisation des données dans MinIO par date (`date=YYYY-MM-DD`), facilitant la gestion de l'historique et les chargements incrémentaux.
- **Monitoring** : Airflow fournit un tableau de bord complet pour surveiller la santé des jobs et alerter en cas d'erreur logicielle ou réseau.

---

## 5. Guide d'Exploitation Rapide
1. **Démarrage** : `docker compose up -d --build`
2. **Pipelines** :
   - `raw_data_ingestion_minio` : Collecte initiale.
   - `nosql_citizen_reports_processing` : Traitement des données Togo.
3. **Analyse** : Connexion de Superset via `postgresql+psycopg2://airflow:airflow@postgres:5432/analytics`.

---
*Ce projet démontre une capacité à intégrer des composants Big Data classiques dans un pipeline cohérent, prêt pour l'analyse stratégique des services publics.*
