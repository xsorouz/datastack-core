# === Script 04 - Téléchargement des fichiers CSV depuis MinIO vers data/inputs/ ===
# Ce script télécharge les fichiers CSV (erp, web, liaison) depuis le bucket MinIO
# et les enregistre dans '/opt/airflow/data/inputs/' pour la suite du pipeline.

import os
import sys
import warnings
from pathlib import Path
from loguru import logger
import boto3
from botocore.exceptions import ClientError

warnings.filterwarnings("ignore")

# ==============================================================================
# 🔧 Initialisation des logs
# ==============================================================================
AIRFLOW_LOG_PATH = os.getenv("AIRFLOW_LOG_PATH", "logs")
LOGS_PATH = Path(AIRFLOW_LOG_PATH)
LOGS_PATH.mkdir(parents=True, exist_ok=True)

LOG_FILE = LOGS_PATH / "download_from_minio.log"
logger.remove()
logger.add(sys.stdout, level="INFO")
logger.add(LOG_FILE, level="INFO", rotation="500 KB")

# ==============================================================================
# ☁️ Configuration MinIO
# ==============================================================================
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "admin1234")
BUCKET_NAME = os.getenv("MINIO_BUCKET_NAME", "bottleneck")
DESTINATION_PREFIX = os.getenv("MINIO_DESTINATION_PREFIX", "data/inputs/")

FILES_TO_DOWNLOAD = ["erp.csv", "web.csv", "liaison.csv"]
LOCAL_INPUTS_PATH = Path("/opt/airflow/data/inputs")
LOCAL_INPUTS_PATH.mkdir(parents=True, exist_ok=True)

# ==============================================================================
# 📥 Téléchargement MinIO ➝ local
# ==============================================================================
def download_from_minio():
    logger.info("📥 Démarrage du téléchargement depuis MinIO...")

    # Connexion MinIO
    try:
        s3_client = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=ACCESS_KEY,
            aws_secret_access_key=SECRET_KEY,
            region_name="us-east-1",
        )
        logger.success("✅ Connexion à MinIO établie.")
    except Exception as e:
        logger.error(f"❌ Connexion à MinIO échouée : {e}")
        sys.exit(1)

    # Téléchargement des fichiers un par un
    for filename in FILES_TO_DOWNLOAD:
        s3_key = f"{DESTINATION_PREFIX}{filename}"
        local_path = LOCAL_INPUTS_PATH / filename

        try:
            s3_client.download_file(BUCKET_NAME, s3_key, str(local_path))
            logger.success(f"📦 Fichier téléchargé avec succès : {filename}")
        except ClientError as e:
            logger.error(f"❌ Erreur lors du téléchargement de {filename} : {e}")
            sys.exit(1)

    logger.success("🎯 Tous les fichiers CSV ont été téléchargés dans 'data/inputs/'.")

# ==============================================================================
# 🚀 Point d’entrée
# ==============================================================================
if __name__ == "__main__":
    try:
        download_from_minio()
        sys.exit(0)
    except Exception as e:
        logger.error(f"❌ Erreur inattendue : {e}")
        sys.exit(1)
