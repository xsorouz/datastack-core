# === Script 02 - Upload des fichiers CSV vers MinIO (robuste & Airflow-compatible) ===
# Ce script envoie les fichiers CSV nettoyés (erp, web, liaison) vers le bucket MinIO,
# dans le dossier 'data/inputs/', en gérant les erreurs et logs de manière robuste.

import os
import sys
import warnings
from pathlib import Path
from loguru import logger
import boto3
from botocore.exceptions import ClientError

warnings.filterwarnings("ignore")

# ==============================================================================
# 🔧 Initialisation des chemins et logs
# ==============================================================================
AIRFLOW_LOG_PATH = os.getenv("AIRFLOW_LOG_PATH", "logs")
LOGS_PATH = Path(AIRFLOW_LOG_PATH)
LOGS_PATH.mkdir(parents=True, exist_ok=True)

LOG_FILE = LOGS_PATH / "upload_minio.log"
logger.remove()
logger.add(sys.stdout, level="INFO")
logger.add(LOG_FILE, level="INFO", rotation="500 KB")

# 📁 Répertoire des CSV à uploader
CSV_PATH = Path("/opt/airflow/data/inputs")
CSV_PATH.mkdir(parents=True, exist_ok=True)

# ==============================================================================
# ☁️ Configuration MinIO
# ==============================================================================
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "admin1234")
BUCKET_NAME = os.getenv("MINIO_BUCKET_NAME", "bottleneck")
DESTINATION_PREFIX = os.getenv("MINIO_DESTINATION_PREFIX", "data/inputs/")

FILES_TO_UPLOAD = ["erp.csv", "web.csv", "liaison.csv"]

# ==============================================================================
# 📤 Fonction d’upload vers MinIO
# ==============================================================================
def upload_to_minio():
    logger.info("🚀 Démarrage de l'upload des fichiers CSV vers MinIO...")

    # Connexion MinIO
    try:
        s3_client = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=ACCESS_KEY,
            aws_secret_access_key=SECRET_KEY,
            region_name="us-east-1",
        )
        logger.success("✅ Connexion à MinIO réussie.")
    except Exception as e:
        logger.error(f"❌ Connexion à MinIO échouée : {e}")
        sys.exit(1)

    # Vérification ou création du bucket
    try:
        s3_client.head_bucket(Bucket=BUCKET_NAME)
        logger.success(f"✅ Bucket '{BUCKET_NAME}' disponible.")
    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        if error_code == "404":
            try:
                s3_client.create_bucket(Bucket=BUCKET_NAME)
                logger.warning(f"📁 Bucket '{BUCKET_NAME}' créé automatiquement.")
            except Exception as e2:
                logger.error(f"❌ Erreur lors de la création du bucket : {e2}")
                sys.exit(1)
        else:
            logger.error(f"❌ Accès refusé au bucket : {e}")
            sys.exit(1)

    # Upload des fichiers
    for filename in FILES_TO_UPLOAD:
        local_file = CSV_PATH / filename
        s3_key = f"{DESTINATION_PREFIX}{filename}"

        if not local_file.exists():
            logger.error(f"❌ Fichier introuvable localement : {filename}")
            sys.exit(1)

        try:
            s3_client.upload_file(str(local_file), BUCKET_NAME, s3_key)
            logger.success(f"📤 Fichier uploadé : {filename} ➔ {s3_key}")
        except Exception as e:
            logger.error(f"❌ Échec de l'upload de {filename} : {e}")
            sys.exit(1)

    logger.success("🎯 Tous les fichiers CSV ont été uploadés avec succès dans MinIO.")

# ==============================================================================
# 🚀 Point d’entrée
# ==============================================================================
if __name__ == "__main__":
    try:
        upload_to_minio()
        sys.exit(0)
    except Exception as e:
        logger.error(f"❌ Erreur inattendue : {e}")
        sys.exit(1)
