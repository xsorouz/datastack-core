# === Script 05 - Nettoyage complet des fichiers bruts CSV avec DuckDB ===
# Ce script lit les fichiers CSV bruts depuis 'data/inputs/', applique des règles métier
# de nettoyage (valeurs nulles, seuils, cohérences), puis enregistre les résultats nettoyés
# dans 'data/outputs/' au format CSV et en base DuckDB. Un résumé statistique est aussi généré.

import os
import sys
import warnings
from pathlib import Path
import pandas as pd
import duckdb
from loguru import logger

# ==============================================================================
# 🔧 Configuration des chemins et du logger
# ==============================================================================
warnings.filterwarnings("ignore")

AIRFLOW_LOG_PATH = os.getenv("AIRFLOW_LOG_PATH", "logs")
LOGS_PATH = Path(AIRFLOW_LOG_PATH)
LOGS_PATH.mkdir(parents=True, exist_ok=True)

LOG_FILE = LOGS_PATH / "clean_data.log"
logger.remove()
logger.add(sys.stdout, level="INFO")
logger.add(LOG_FILE, level="INFO", rotation="500 KB")

# ==============================================================================
# 🧹 Fonction principale
# ==============================================================================
def main():
    # 📁 Chemins absolus Airflow-friendly
    INPUTS_PATH = Path("/opt/airflow/data/inputs")
    OUTPUTS_PATH = Path("/opt/airflow/data/outputs")
    DUCKDB_PATH = Path("/opt/airflow/data/bottleneck.duckdb")

    INPUTS_PATH.mkdir(parents=True, exist_ok=True)
    OUTPUTS_PATH.mkdir(parents=True, exist_ok=True)

    # 📥 Chargement des fichiers CSV bruts
    try:
        df_erp = pd.read_csv(INPUTS_PATH / "erp.csv")
        df_web = pd.read_csv(INPUTS_PATH / "web.csv")
        df_liaison = pd.read_csv(INPUTS_PATH / "liaison.csv")

        logger.info(f"ERP     : {len(df_erp)} lignes (lignes vides : {df_erp.isnull().all(axis=1).sum()})")
        logger.info(f"WEB     : {len(df_web)} lignes (lignes vides : {df_web.isnull().all(axis=1).sum()})")
        logger.info(f"LIAISON : {len(df_liaison)} lignes (lignes vides : {df_liaison.isnull().all(axis=1).sum()})")
    except Exception as e:
        logger.error(f"❌ Erreur lors du chargement initial des CSV : {e}")
        sys.exit(1)

    # 🦆 Connexion à DuckDB
    if not DUCKDB_PATH.exists():
        logger.info("ℹ️ Fichier DuckDB non trouvé, il sera créé.")
    try:
        con = duckdb.connect(str(DUCKDB_PATH))
        logger.success("✅ Connexion à DuckDB établie.")
    except Exception as e:
        logger.error(f"❌ Erreur de connexion à DuckDB : {e}")
        sys.exit(1)

    # 🧼 Nettoyage métier avec DuckDB (valeurs nulles, seuils, cohérence)
    try:
        con.execute(f"""
            CREATE OR REPLACE TABLE erp_clean AS
            SELECT * FROM read_csv_auto('{INPUTS_PATH}/erp.csv')
            WHERE product_id IS NOT NULL
              AND onsale_web IS NOT NULL
              AND price IS NOT NULL AND price > 0
              AND stock_quantity IS NOT NULL
              AND stock_status IS NOT NULL
        """)
        logger.success("✅ Table 'erp_clean' créée avec règles de filtrage.")

        con.execute(f"""
            CREATE OR REPLACE TABLE web_clean AS
            SELECT * FROM read_csv_auto('{INPUTS_PATH}/web.csv')
            WHERE sku IS NOT NULL
        """)
        logger.success("✅ Table 'web_clean' créée avec filtrage sur SKU.")

        con.execute(f"""
            CREATE OR REPLACE TABLE liaison_clean AS
            SELECT * FROM read_csv_auto('{INPUTS_PATH}/liaison.csv')
            WHERE product_id IS NOT NULL AND id_web IS NOT NULL
        """)
        logger.success("✅ Table 'liaison_clean' créée avec filtres de jointure.")
    except Exception as e:
        logger.error(f"❌ Erreur lors de la création des tables nettoyées : {e}")
        sys.exit(1)

    # 💾 Export des résultats nettoyés au format CSV
    try:
        con.execute(f"COPY erp_clean TO '{OUTPUTS_PATH}/erp_clean.csv' (HEADER, DELIMITER ',')")
        con.execute(f"COPY web_clean TO '{OUTPUTS_PATH}/web_clean.csv' (HEADER, DELIMITER ',')")
        con.execute(f"COPY liaison_clean TO '{OUTPUTS_PATH}/liaison_clean.csv' (HEADER, DELIMITER ',')")
        logger.success("✅ Données nettoyées exportées vers 'data/outputs/'.")
    except Exception as e:
        logger.error(f"❌ Erreur lors de l'export des fichiers CSV : {e}")
        sys.exit(1)

    # 📊 Résumé statistique des exclusions
    try:
        resume_df = pd.DataFrame({
            "source": ["erp", "web", "liaison"],
            "nb_lignes_initiales": [len(df_erp), len(df_web), len(df_liaison)],
            "nb_apres_nettoyage": [
                con.execute("SELECT COUNT(*) FROM erp_clean").fetchone()[0],
                con.execute("SELECT COUNT(*) FROM web_clean").fetchone()[0],
                con.execute("SELECT COUNT(*) FROM liaison_clean").fetchone()[0]
            ]
        })
        resume_df["nb_exclues"] = resume_df["nb_lignes_initiales"] - resume_df["nb_apres_nettoyage"]
        resume_df.to_csv(OUTPUTS_PATH / "resume_stats.csv", index=False)
        logger.success("📈 Statistiques sauvegardées dans resume_stats.csv")
    except Exception as e:
        logger.error(f"❌ Erreur lors de la génération du résumé : {e}")
        sys.exit(1)

    logger.success("🎯 Nettoyage terminé avec succès.")

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
