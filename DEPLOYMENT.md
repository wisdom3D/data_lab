# Guide de Déploiement - Data Lab POC

Ce guide décrit les étapes pour déployer l'intégralité de la plateforme sur un serveur de production ou une machine virtuelle.

---

## 1. Prérequis Système
- **CPU** : 4 cœurs minimum (recommandé : 8)
- **RAM** : 8 Go minimum (recommandé : 16 Go pour Superset + Airflow)
- **Stockage** : 50 Go (SSD de préférence)
- **OS** : Linux (Ubuntu 22.04 LTS recommandé) ou Docker Desktop (Windows/Mac)

---

## 2. Installation des Dépendances
Sur votre serveur, assurez-vous d'avoir Docker et Git installés :

```bash
# Mise à jour et installation de Docker
sudo apt update
sudo apt install docker.io docker-compose git -y
sudo systemctl enable --now docker
```

---

## 3. Déploiement de la Solution

### Étape 1 : Récupération du code
```bash
git clone https://github.com/wisdom3D/data_lab.git
cd data_lab
```

### Étape 2 : Configuration de l'environnement
Copiez le template de configuration et modifiez les accès (utilisez des mots de passe robustes en production) :
```bash
cp .env.example .env
nano .env
```

### Étape 3 : Lancement de l'infrastructure
Utilisez la commande suivante pour construire les images optimisées (notamment Superset avec ses pilotes) et démarrer les services en arrière-plan :
```bash
docker compose up -d --build
```

---

## 4. Configuration Post-Déploiement

### Accès aux Services
- **Airflow** : `http://<IP_SERVEUR>:8080` (Identifiants dans `.env`)
- **Superset** : `http://<IP_SERVEUR>:8088` (Identifiants dans `.env`)
- **MinIO Console** : `http://<IP_SERVEUR>:9001`

### Configuration de la Connexion Base de Données (Superset)
1. Allez dans **Settings > Database Connections**.
2. Cliquez sur **+ Database**.
3. Choisissez **PostgreSQL**.
4. Utilisez l'URL SQLAlchemy :
   `postgresql+psycopg2://airflow:airflow@postgres:5432/analytics`

### Activation des Pipelines (Airflow)
1. Connectez-vous à l'interface Airflow.
2. Activez les DAGs suivants (cliquez sur le bouton "Off" pour le passer en "On") :
   - `raw_data_ingestion_minio` (Ingestion brute)
   - `process_raw_to_postgis_dag` (Traitement spatial)
   - `nosql_citizen_reports_processing` (Traitement NoSQL)

---

## 5. Maintenance et Logs
Pour surveiller les services en production :

- **Voir les logs d'un service** : `docker compose logs -f <nom_service>`
- **Redémarrer un service** : `docker compose restart <nom_service>`
- **Mettre à jour la solution** :
  ```bash
  git pull
  docker compose up -d --build
  ```

---
*Note 1 : Pour une production réelle exposée sur internet, il est fortement recommandé de suivre notre **[Guide de Sécurisation SSL](SSL_PRODUCTION.md)**.*

