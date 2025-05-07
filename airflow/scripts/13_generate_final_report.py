# === Script 13 - Génération du rapport final et upload dans MinIO ===
# Ce script synthétise toutes les métriques du pipeline, génère un rapport final,
# l’exporte en CSV/XLSX et l’upload dans MinIO.

import os
import sys
import duckdb
import pandas as pd
import boto3
from botocore.exceptions import ClientError
from pathlib import Path
from loguru import logger
import warnings

warnings.filterwarnings("ignore")

# ==============================================================================
# 🔧 Initialisation des logs
# ==============================================================================
AIRFLOW_LOG_PATH = os.getenv("AIRFLOW_LOG_PATH", "logs")
LOGS_PATH = Path(AIRFLOW_LOG_PATH)
LOGS_PATH.mkdir(parents=True, exist_ok=True)

LOG_FILE = LOGS_PATH / "rapport_final.log"
logger.remove()
logger.add(sys.stdout, level="INFO")
logger.add(LOG_FILE, level="INFO", rotation="500 KB")

# ==============================================================================
# 📊 Fonction principale
# ==============================================================================
def main():
    DATA_PATH = Path("/opt/airflow/data")
    DUCKDB_PATH = DATA_PATH / "bottleneck.duckdb"
    RAW_PATH = DATA_PATH / "inputs"
    OUTPUTS_PATH = DATA_PATH / "outputs"
    OUTPUTS_PATH.mkdir(parents=True, exist_ok=True)

    # ✅ Connexion à DuckDB
    if not DUCKDB_PATH.exists():
        logger.error(f"❌ Base DuckDB introuvable à : {DUCKDB_PATH}")
        sys.exit(1)

    try:
        con = duckdb.connect(str(DUCKDB_PATH))
        logger.success("✅ Connexion à DuckDB établie.")
    except Exception as e:
        logger.error(f"❌ Erreur de connexion à DuckDB : {e}")
        sys.exit(1)

    # 📥 Récupération des métriques du pipeline
    try:
        logger.info("📋 Récupération des métriques du pipeline...")

        metrics = {
            "ERP_brut": pd.read_csv(RAW_PATH / "erp.csv").shape[0],
            "Web_brut": pd.read_csv(RAW_PATH / "web.csv").shape[0],
            "Liaison_brut": pd.read_csv(RAW_PATH / "liaison.csv").shape[0],
            "ERP_nettoye": con.execute("SELECT COUNT(*) FROM erp_clean").fetchone()[0],
            "Web_nettoye": con.execute("SELECT COUNT(*) FROM web_clean").fetchone()[0],
            "Liaison_nettoye": con.execute("SELECT COUNT(*) FROM liaison_clean").fetchone()[0],
            "ERP_dedup": con.execute("SELECT COUNT(*) FROM erp_dedup").fetchone()[0],
            "Web_dedup": con.execute("SELECT COUNT(*) FROM web_dedup").fetchone()[0],
            "Liaison_dedup": con.execute("SELECT COUNT(*) FROM liaison_dedup").fetchone()[0],
            "Fusion": con.execute("SELECT COUNT(*) FROM fusion").fetchone()[0],
            "CA_total": con.execute("SELECT ca_total FROM ca_total").fetchone()[0],
            "Produits_CA": con.execute("SELECT COUNT(*) FROM ca_par_produit").fetchone()[0],
            "Vins_millesimes": pd.read_csv(OUTPUTS_PATH / "vins_millesimes.csv").shape[0],
        }

        logger.success("✅ Données récupérées avec succès.")
    except Exception as e:
        logger.error(f"❌ Erreur lors de la récupération des métriques : {e}")
        sys.exit(1)

    # 📄 Construction et export du rapport
    try:
        df_report = pd.DataFrame([
            {"Étape": "Brut - ERP", "Résultat": metrics["ERP_brut"], "Attendu": ""},
            {"Étape": "Brut - Web", "Résultat": metrics["Web_brut"], "Attendu": ""},
            {"Étape": "Brut - Liaison", "Résultat": metrics["Liaison_brut"], "Attendu": ""},
            {"Étape": "Nettoyé - ERP", "Résultat": metrics["ERP_nettoye"], "Attendu": ""},
            {"Étape": "Nettoyé - Web", "Résultat": metrics["Web_nettoye"], "Attendu": ""},
            {"Étape": "Nettoyé - Liaison", "Résultat": metrics["Liaison_nettoye"], "Attendu": ""},
            {"Étape": "Dédoublonné - ERP", "Résultat": metrics["ERP_dedup"], "Attendu": "825"},
            {"Étape": "Dédoublonné - Web", "Résultat": metrics["Web_dedup"], "Attendu": "714"},
            {"Étape": "Dédoublonné - Liaison", "Résultat": metrics["Liaison_dedup"], "Attendu": "825"},
            {"Étape": "Fusion finale", "Résultat": metrics["Fusion"], "Attendu": "714"},
            {"Étape": "Produits CA", "Résultat": metrics["Produits_CA"], "Attendu": "573"},
            {"Étape": "CA Total (€)", "Résultat": round(metrics["CA_total"], 2), "Attendu": "387837.60"},
            {"Étape": "Vins Millésimés", "Résultat": metrics["Vins_millesimes"], "Attendu": "30"},
        ])

        df_report.to_csv(OUTPUTS_PATH / "rapport_final.csv", index=False)
        df_report.to_excel(OUTPUTS_PATH / "rapport_final.xlsx", index=False)
        logger.success("📄 Rapport final généré et exporté (CSV + XLSX).")
    except Exception as e:
        logger.error(f"❌ Erreur lors de l'export du rapport : {e}")
        sys.exit(1)

    # ☁️ Upload vers MinIO
    try:
        s3_client = boto3.client(
            "s3",
            endpoint_url=os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
            aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "admin"),
            aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "admin1234"),
            region_name="us-east-1",
        )
        bucket_name = os.getenv("MINIO_BUCKET_NAME", "bottleneck")
        prefix = os.getenv("MINIO_DESTINATION_PREFIX", "data/outputs/")

        for filename in ["rapport_final.csv", "rapport_final.xlsx"]:
            local_path = OUTPUTS_PATH / filename
            s3_key = f"{prefix}{filename}"
            s3_client.upload_file(str(local_path), bucket_name, s3_key)
            logger.success(f"🚀 Upload réussi : {filename} ➔ {s3_key}")
    except Exception as e:
        logger.error(f"❌ Échec de l’upload vers MinIO : {e}")
        sys.exit(1)

    logger.success("🎯 Rapport final archivé avec succès dans MinIO.")

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
