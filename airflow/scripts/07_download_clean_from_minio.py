# === Script 07 - Téléchargement des fichiers nettoyés depuis MinIO ===
# Ce script télécharge les fichiers nettoyés ('erp_clean.csv', 'web_clean.csv', 'liaison_clean.csv')
# depuis le bucket MinIO (préfixe 'data/outputs/') et les enregistre dans '/opt/airflow/data/outputs/'.

import os
import sys
import warnings
from pathlib import Path
import boto3
from botocore.exceptions import ClientError
from loguru import logger

# ==============================================================================
# 🔧 Configuration des logs
# ==============================================================================
warnings.filterwarnings("ignore")

AIRFLOW_LOG_PATH = os.getenv("AIRFLOW_LOG_PATH", "logs")
LOGS_PATH = Path(AIRFLOW_LOG_PATH)
LOGS_PATH.mkdir(parents=True, exist_ok=True)

LOG_FILE = LOGS_PATH / "download_clean_from_minio.log"
logger.remove()
logger.add(sys.stdout, level="INFO")
logger.add(LOG_FILE, level="INFO", rotation="500 KB")

# ==============================================================================
# 📥 Fonction principale de téléchargement depuis MinIO
# ==============================================================================
def main():
    # 🌍 Paramètres de connexion MinIO
    MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
    SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "admin1234")
    BUCKET_NAME = os.getenv("MINIO_BUCKET_NAME", "bottleneck")
    DESTINATION_PREFIX = os.getenv("MINIO_DESTINATION_PREFIX", "data/outputs/")

    # 📁 Dossier local cible
    LOCAL_OUTPUTS_PATH = Path("/opt/airflow/data/outputs")
    LOCAL_OUTPUTS_PATH.mkdir(parents=True, exist_ok=True)

    files_to_download = ["erp_clean.csv", "web_clean.csv", "liaison_clean.csv"]

    # 🔌 Connexion à MinIO
    try:
        s3_client = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=ACCESS_KEY,
            aws_secret_access_key=SECRET_KEY,
            region_name="us-east-1"
        )
        logger.success("✅ Connexion à MinIO réussie.")
    except Exception as e:
        logger.error(f"❌ Échec de connexion à MinIO : {e}")
        sys.exit(1)

    # ✅ Vérification du bucket
    try:
        s3_client.head_bucket(Bucket=BUCKET_NAME)
        logger.success(f"✅ Bucket '{BUCKET_NAME}' disponible.")
    except ClientError as e:
        logger.error(f"❌ Bucket inaccessible ou inexistant : {e}")
        sys.exit(1)

    # 📥 Téléchargement des fichiers
    logger.info("📥 Démarrage du téléchargement des fichiers nettoyés depuis MinIO...")

    for filename in files_to_download:
        s3_key = f"{DESTINATION_PREFIX}{filename}"
        local_path = LOCAL_OUTPUTS_PATH / filename

        try:
            s3_client.download_file(
                Bucket=BUCKET_NAME,
                Key=s3_key,
                Filename=str(local_path)
            )
            logger.success(f"✅ Fichier téléchargé : {filename} ➔ {local_path}")
        except ClientError as e:
            logger.error(f"❌ Échec du téléchargement de {filename} : {e}")
            sys.exit(1)

    logger.success("🎯 Tous les fichiers ont été téléchargés avec succès depuis MinIO.")

# ==============================================================================
# 🚀 Point d’entrée du script
# ==============================================================================
if __name__ == "__main__":
    try:
        main()
        sys.exit(0)
    except Exception as e:
        logger.error(f"❌ Erreur inattendue : {e}")
        sys.exit(1)
