# ============================================================================
# Basé sur l'image officielle Airflow avec Python 3.10
# Ajoute les dépendances projet via un fichier requirements.txt
# ============================================================================

FROM apache/airflow:2.9.0-python3.10

# ============================================================================
# 📁 Répertoire de travail par défaut d’Airflow
# ============================================================================
WORKDIR /opt/airflow

# ============================================================================
# 📄 Installation des dépendances depuis requirements.txt (à la racine)
# ============================================================================
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ============================================================================
# ✅ Fin (CMD géré dans docker-compose.yml)
# ============================================================================
