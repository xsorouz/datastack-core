# === Script 11 - Calcul du chiffre d'affaires et upload dans MinIO ===
# Ce script calcule le CA par produit à partir de la table 'fusion',
# exporte les résultats en CSV/XLSX, et les upload dans MinIO.

import os
import sys
from pathlib import Path
import duckdb
import pandas as pd
import boto3
from botocore.exceptions import ClientError
from loguru import logger

# ==============================================================================
# 🔧 Initialisation des logs
# ==============================================================================
AIRFLOW_LOG_PATH = os.getenv("AIRFLOW_LOG_PATH", "logs")
LOGS_PATH = Path(AIRFLOW_LOG_PATH)
LOGS_PATH.mkdir(parents=True, exist_ok=True)

LOG_FILE = LOGS_PATH / "calcul_ca.log"
logger.remove()
logger.add(sys.stdout, level="INFO")
logger.add(LOG_FILE, level="INFO", rotation="500 KB")

# ==============================================================================
# 💰 Fonction principale : calcul CA, export, upload MinIO
# ==============================================================================
def main():
    DUCKDB_PATH = Path("/opt/airflow/data/bottleneck.duckdb")
    OUTPUTS_PATH = Path("/opt/airflow/data/outputs")
    OUTPUTS_PATH.mkdir(parents=True, exist_ok=True)

    # 📊 Connexion à DuckDB
    if not DUCKDB_PATH.exists():
        logger.error(f"❌ Base DuckDB introuvable : {DUCKDB_PATH}")
        sys.exit(1)

    try:
        con = duckdb.connect(str(DUCKDB_PATH))
        logger.success("✅ Connexion à DuckDB réussie.")
    except Exception as e:
        logger.error(f"❌ Connexion DuckDB échouée : {e}")
        sys.exit(1)

    # 🧮 Calcul du chiffre d'affaires
    try:
        con.execute("""
            CREATE OR REPLACE TABLE ca_par_produit AS
            SELECT
                product_id,
                post_title,
                price,
                stock_quantity,
                ROUND(price * stock_quantity, 2) AS chiffre_affaires
            FROM fusion
            WHERE stock_quantity > 0
              AND stock_status = 'instock'
        """)
        logger.success("✅ Table 'ca_par_produit' créée.")

        con.execute("""
            CREATE OR REPLACE TABLE ca_total AS
            SELECT ROUND(SUM(chiffre_affaires), 2) AS ca_total
            FROM ca_par_produit
        """)
        logger.success("✅ Table 'ca_total' créée.")
    except Exception as e:
        logger.error(f"❌ Erreur lors du calcul CA : {e}")
        sys.exit(1)

    # 💾 Export local en CSV/XLSX
    try:
        df_ca = con.execute("SELECT * FROM ca_par_produit").fetchdf()
        df_total = con.execute("SELECT * FROM ca_total").fetchdf()

        local_files = {
            "ca_par_produit.csv": df_ca,
            "ca_total.csv": df_total,
            "ca_par_produit.xlsx": df_ca,
        }

        for filename, df in local_files.items():
            local_path = OUTPUTS_PATH / filename
            if filename.endswith(".csv"):
                df.to_csv(local_path, index=False)
            elif filename.endswith(".xlsx"):
                df.to_excel(local_path, index=False)
            logger.success(f"📁 Fichier généré localement : {local_path}")
    except Exception as e:
        logger.error(f"❌ Erreur lors de la génération des fichiers CA : {e}")
        sys.exit(1)

    # ☁️ Upload dans MinIO
    MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
    SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "admin1234")
    BUCKET_NAME = os.getenv("MINIO_BUCKET_NAME", "bottleneck")
    DESTINATION_PREFIX = os.getenv("MINIO_DESTINATION_PREFIX", "data/outputs/")

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
        logger.error(f"❌ Échec de la connexion à MinIO : {e}")
        sys.exit(1)

    try:
        for filename in local_files:
            local_path = OUTPUTS_PATH / filename
            s3_key = f"{DESTINATION_PREFIX}{filename}"
            s3_client.upload_file(str(local_path), BUCKET_NAME, s3_key)
            logger.success(f"🚀 Upload réussi : {filename} ➔ {s3_key}")
    except ClientError as e:
        logger.error(f"❌ Erreur d'upload MinIO : {e}")
        sys.exit(1)

    logger.success("🎯 Tous les fichiers de CA ont été uploadés avec succès.")

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
