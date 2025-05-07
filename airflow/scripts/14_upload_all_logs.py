# === Script 14 - Upload automatique des fichiers logs vers MinIO ===
# Ce script explore le répertoire "logs" et transfère chaque fichier .log
# dans le bucket MinIO, sous le chemin distant 'logs/'.

import os
import sys
import boto3
from botocore.exceptions import ClientError
from pathlib import Path
from loguru import logger

# ==============================================================================
# 🔧 Initialisation des logs d'exécution
# ==============================================================================
AIRFLOW_LOG_PATH = os.getenv("AIRFLOW_LOG_PATH", "logs")
LOGS_PATH = Path(AIRFLOW_LOG_PATH)
LOGS_PATH.mkdir(parents=True, exist_ok=True)

LOG_FILE = LOGS_PATH / "upload_all_logs.log"
logger.remove()
logger.add(sys.stdout, level="INFO")
logger.add(LOG_FILE, level="INFO", rotation="500 KB")

# ==============================================================================
# 📤 Fonction principale : upload des logs
# ==============================================================================
def main():
    # 🌍 Configuration MinIO (via .env ou Airflow)
    MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
    SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "admin1234")
    BUCKET_NAME = os.getenv("MINIO_BUCKET_NAME", "bottleneck")

    # ✅ Connexion à MinIO
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

    # 📦 Recherche des fichiers logs
    logs_files = list(LOGS_PATH.glob("*.log"))
    if not logs_files:
        logger.warning("⚠️ Aucun fichier .log trouvé à uploader.")
        sys.exit(0)

    logger.info(f"📂 {len(logs_files)} fichier(s) log trouvé(s) à uploader.")

    # 🚀 Upload vers MinIO
    for log_file in logs_files:
        s3_key = f"logs/{log_file.name}"
        try:
            s3_client.upload_file(str(log_file), BUCKET_NAME, s3_key)
            logger.success(f"📤 Upload réussi : {log_file.name} ➔ {s3_key}")
        except ClientError as e:
            logger.error(f"❌ Échec de l’upload de {log_file.name} : {e}")
            sys.exit(1)

    logger.success("🎯 Tous les fichiers logs ont été uploadés avec succès.")

# ==============================================================================
# 🚀 Point d’entrée
# ==============================================================================
if __name__ == "__main__":
    try:
        main()
        sys.exit(0)
    except Exception as e:
        logger.error(f"❌ Erreur inattendue : {e}")
        sys.exit(1)
