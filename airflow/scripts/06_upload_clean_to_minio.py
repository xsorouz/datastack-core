# === Script 06 - Upload des fichiers nettoyés vers MinIO ===
# Ce script envoie les fichiers CSV nettoyés depuis 'data/outputs/'
# vers un bucket MinIO, dans le dossier 'data/outputs/'.

import os
import sys
import warnings
from pathlib import Path
import boto3
from botocore.exceptions import ClientError
from loguru import logger

# ==============================================================================
# 🔧 Initialisation du logger et des chemins
# ==============================================================================
warnings.filterwarnings("ignore")

AIRFLOW_LOG_PATH = os.getenv("AIRFLOW_LOG_PATH", "logs")
LOGS_PATH = Path(AIRFLOW_LOG_PATH)
LOGS_PATH.mkdir(parents=True, exist_ok=True)

LOG_FILE = LOGS_PATH / "upload_clean_to_minio.log"
logger.remove()
logger.add(sys.stdout, level="INFO")
logger.add(LOG_FILE, level="INFO", rotation="500 KB")

# ==============================================================================
# 🚀 Fonction principale d’upload vers MinIO
# ==============================================================================
def main():
    # 🌍 Paramètres de connexion MinIO
    MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
    SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "admin1234")
    BUCKET_NAME = os.getenv("MINIO_BUCKET_NAME", "bottleneck")
    DESTINATION_PREFIX = os.getenv("MINIO_DESTINATION_PREFIX", "data/outputs/")

    # 📁 Répertoire des fichiers à envoyer
    OUTPUTS_PATH = Path("/opt/airflow/data/outputs")
    OUTPUTS_PATH.mkdir(parents=True, exist_ok=True)

    files_to_upload = ["erp_clean.csv", "web_clean.csv", "liaison_clean.csv"]

    # 🔌 Connexion au client S3 (MinIO)
    try:
        s3_client = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=ACCESS_KEY,
            aws_secret_access_key=SECRET_KEY,
            region_name="us-east-1"
        )
        logger.success("✅ Connexion à MinIO établie.")
    except Exception as e:
        logger.error(f"❌ Connexion à MinIO échouée : {e}")
        sys.exit(1)

    # 📦 Vérification de l'existence du bucket
    try:
        s3_client.head_bucket(Bucket=BUCKET_NAME)
        logger.success(f"✅ Bucket '{BUCKET_NAME}' trouvé.")
    except ClientError as e:
        logger.error(f"❌ Le bucket '{BUCKET_NAME}' est inaccessible ou inexistant : {e}")
        sys.exit(1)

    # 📤 Envoi des fichiers
    logger.info("📤 Démarrage de l’upload des fichiers nettoyés vers MinIO...")

    for filename in files_to_upload:
        local_path = OUTPUTS_PATH / filename
        s3_key = f"{DESTINATION_PREFIX}{filename}"

        if not local_path.exists():
            logger.error(f"❌ Fichier introuvable localement : {local_path}")
            sys.exit(1)

        try:
            s3_client.upload_file(
                Filename=str(local_path),
                Bucket=BUCKET_NAME,
                Key=s3_key
            )
            logger.success(f"✅ Upload réussi : {filename} ➔ {s3_key}")
        except Exception as e:
            logger.error(f"❌ Échec de l’upload de {filename} : {e}")
            sys.exit(1)

    logger.success("🎯 Tous les fichiers nettoyés ont été uploadés avec succès.")

# ==============================================================================
# 📌 Point d’entrée du script
# ==============================================================================
if __name__ == "__main__":
    try:
        main()
        sys.exit(0)
    except Exception as e:
        logger.error(f"❌ Erreur inattendue : {e}")
        sys.exit(1)
