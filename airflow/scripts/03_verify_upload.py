# === Script 03 - Vérification de la présence des fichiers dans MinIO ===
# Ce script vérifie que les fichiers CSV attendus (erp, web, liaison)
# sont bien présents dans le bucket MinIO après l’upload initial.

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

LOG_FILE = LOGS_PATH / "verify_upload.log"
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

EXPECTED_FILES = {
    f"{DESTINATION_PREFIX}erp.csv",
    f"{DESTINATION_PREFIX}web.csv",
    f"{DESTINATION_PREFIX}liaison.csv",
}

# ==============================================================================
# 🔎 Fonction de vérification
# ==============================================================================
def verify_minio_upload():
    logger.info("🔍 Démarrage de la vérification des fichiers dans MinIO...")

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

    # Vérification de l'existence du bucket
    try:
        s3_client.head_bucket(Bucket=BUCKET_NAME)
        logger.success(f"✅ Bucket '{BUCKET_NAME}' accessible.")
    except ClientError as e:
        logger.error(f"❌ Bucket inaccessible : {e}")
        sys.exit(1)

    # Listing et contrôle des fichiers
    try:
        response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=DESTINATION_PREFIX)
        contents = response.get("Contents", [])

        if not contents:
            logger.error(f"❌ Aucun fichier trouvé dans {BUCKET_NAME}/{DESTINATION_PREFIX}")
            sys.exit(1)

        found_files = {obj["Key"] for obj in contents}
        logger.info(f"📦 Fichiers trouvés : {len(found_files)}")
        for f in found_files:
            logger.info(f"   - {f}")

        missing = EXPECTED_FILES - found_files
        if missing:
            logger.error(f"❌ Fichiers manquants : {missing}")
            sys.exit(1)

        logger.success("🎯 Tous les fichiers attendus sont présents dans MinIO.")

    except Exception as e:
        logger.error(f"❌ Erreur lors du listing MinIO : {e}")
        sys.exit(1)

# ==============================================================================
# 🚀 Point d’entrée
# ==============================================================================
if __name__ == "__main__":
    try:
        verify_minio_upload()
        sys.exit(0)
    except Exception as e:
        logger.error(f"❌ Erreur inattendue : {e}")
        sys.exit(1)
