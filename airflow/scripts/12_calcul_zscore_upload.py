# === Script 12 - Calcul du Z-score et upload dans MinIO ===
# Ce script identifie les vins "millésimés" via un Z-score sur le prix.
# Les résultats sont exportés localement puis uploadés dans MinIO.

import os
import sys
import warnings
from pathlib import Path
import duckdb
import pandas as pd
import boto3
from botocore.exceptions import ClientError
from loguru import logger

warnings.filterwarnings("ignore")

# ==============================================================================
# 🔧 Initialisation des logs
# ==============================================================================
AIRFLOW_LOG_PATH = os.getenv("AIRFLOW_LOG_PATH", "logs")
LOGS_PATH = Path(AIRFLOW_LOG_PATH)
LOGS_PATH.mkdir(parents=True, exist_ok=True)

LOG_FILE = LOGS_PATH / "calcul_zscore.log"
logger.remove()
logger.add(sys.stdout, level="INFO")
logger.add(LOG_FILE, level="INFO", rotation="500 KB")

# ==============================================================================
# 🍷 Fonction principale : calcul du Z-score et upload MinIO
# ==============================================================================
def main():
    DUCKDB_PATH = Path("/opt/airflow/data/bottleneck.duckdb")
    OUTPUTS_PATH = Path("/opt/airflow/data/outputs")
    OUTPUTS_PATH.mkdir(parents=True, exist_ok=True)

    # 🦆 Connexion à DuckDB
    if not DUCKDB_PATH.exists():
        logger.error(f"❌ Base DuckDB introuvable : {DUCKDB_PATH}")
        sys.exit(1)

    try:
        con = duckdb.connect(str(DUCKDB_PATH))
        logger.success("✅ Connexion à DuckDB établie.")
    except Exception as e:
        logger.error(f"❌ Erreur de connexion à DuckDB : {e}")
        sys.exit(1)

    # 📊 Calcul du Z-score
    try:
        df = con.execute("""
            SELECT product_id, post_title, price
            FROM fusion
            WHERE price IS NOT NULL
        """).fetchdf()

        df["z_score"] = (df["price"] - df["price"].mean()) / df["price"].std()
        df["type"] = df["z_score"].apply(lambda z: "millésimé" if z > 2 else "ordinaire")

        nb_millesimes = (df["type"] == "millésimé").sum()
        nb_total = len(df)

        logger.info(f"🍷 Vins millésimés détectés : {nb_millesimes} (attendu : 30)")
        logger.info(f"📦 Vins ordinaires : {nb_total - nb_millesimes}")
    except Exception as e:
        logger.error(f"❌ Erreur lors du calcul du Z-score : {e}")
        sys.exit(1)

    # 💾 Export local
    try:
        vins_millesimes_path = OUTPUTS_PATH / "vins_millesimes.csv"
        vins_ordinaires_path = OUTPUTS_PATH / "vins_ordinaires.csv"

        df[df["type"] == "millésimé"].to_csv(vins_millesimes_path, index=False)
        df[df["type"] == "ordinaire"].to_csv(vins_ordinaires_path, index=False)

        logger.success(f"📄 Export local terminé : {vins_millesimes_path}, {vins_ordinaires_path}")
    except Exception as e:
        logger.error(f"❌ Erreur lors de l'export local : {e}")
        sys.exit(1)

    # ☁️ Connexion MinIO
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
        logger.success("✅ Connexion à MinIO établie.")
    except Exception as e:
        logger.error(f"❌ Connexion à MinIO échouée : {e}")
        sys.exit(1)

    # 🚀 Upload des fichiers vers MinIO
    try:
        for local_file in [vins_millesimes_path, vins_ordinaires_path]:
            s3_key = f"{DESTINATION_PREFIX}{local_file.name}"
            s3_client.upload_file(str(local_file), BUCKET_NAME, s3_key)
            logger.success(f"🚀 Upload réussi : {local_file.name} ➔ {s3_key}")
    except ClientError as e:
        logger.error(f"❌ Erreur lors de l'upload MinIO : {e}")
        sys.exit(1)

    # ✅ Tests de validation interne
    try:
        assert nb_millesimes == 30, f"❌ Nombre incorrect de vins millésimés : {nb_millesimes} (attendu : 30)"
        assert df[["price", "z_score"]].isnull().sum().sum() == 0, "❌ Valeurs nulles détectées dans price/z_score"
        assert df["z_score"].isin([float("inf"), float("-inf")]).sum() == 0, "❌ Valeurs infinies détectées dans z_score"
        logger.success("🧪 Validation des résultats Z-score : OK")
    except Exception as e:
        logger.error(f"❌ Échec des tests de validation Z-score : {e}")
        sys.exit(1)

    logger.success("🎯 Fichiers Z-score générés et uploadés avec succès.")

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
