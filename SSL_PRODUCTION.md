# Sécurisation Production : SSL & Reverse Proxy

Pour exposer votre plateforme sur Internet, il est crucial d'utiliser un **Reverse Proxy** pour gérer le chiffrement SSL (HTTPS) et centraliser les accès.

## Pourquoi utiliser Traefik ?
Nous recommandons **Traefik** car il est conçu pour Docker :
1. **Auto-découverte** : Il détecte vos conteneurs automatiquement.
2. **Auto-SSL** : Il gère le renouvellement des certificats Let's Encrypt tout seul.
3. **Sécurité** : Il masque l'accès direct aux ports internes des conteneurs.

---

## 1. Prérequis
1. **Nom de Domaine** : Vous devez posséder un domaine (ex: `datalab.togo.tg`).
2. **DNS** : Faites pointer des sous-domaines vers l'IP de votre serveur :
   - `airflow.datalab.togo.tg` -> IP_SERVEUR
   - `superset.datalab.togo.tg` -> IP_SERVEUR
   - `s3.datalab.togo.tg` -> IP_SERVEUR (MinIO)
3. **Ports** : Ouvrez les ports **80** (HTTP) et **443** (HTTPS) sur votre pare-feu.

---

## 2. Configuration (docker-compose.prod.yml)
Plutôt que de modifier le fichier de base, on utilise un fichier d'override.

### Étape 1 : Créer le fichier `docker-compose.prod.yml`
Ce fichier (fourni dans le projet) contient la configuration de Traefik. Il utilise des **labels** pour mapper les URLs aux services.

### Étape 2 : Lancer en mode Production
```bash
docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d --build
```

---

## 3. Exemple de Labels pour Airflow
Voici comment Traefik sait comment router le trafic (déjà configuré dans le template prod) :
```yaml
labels:
  - "traefik.enable=true"
  - "traefik.http.routers.airflow.rule=Host(`airflow.votre-domaine.com`)"
  - "traefik.http.routers.airflow.entrypoints=websecure"
  - "traefik.http.routers.airflow.tls.certresolver=myresolver"
```

---
