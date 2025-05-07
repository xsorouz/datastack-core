# === Script 08 - Dédoublonnage des fichiers nettoyés avec DuckDB ===
# Ce script applique des règles de dédoublonnage spécifiques sur les fichiers nettoyés.
# Il crée trois tables DuckDB (erp_dedup, web_dedup, liaison_dedup) et vérifie leur validité.

import os
import sys
import warnings
from pathlib import Path
import duckdb
from loguru import logger

# ==============================================================================
# 🔧 Initialisation des logs
# ==============================================================================
warnings.filterwarnings("ignore")

AIRFLOW_LOG_PATH = os.getenv("AIRFLOW_LOG_PATH", "logs")
LOGS_PATH = Path(AIRFLOW_LOG_PATH)
LOGS_PATH.mkdir(parents=True, exist_ok=True)

LOG_FILE = LOGS_PATH / "dedoublonnage.log"
logger.remove()
logger.add(sys.stdout, level="INFO")
logger.add(LOG_FILE, level="INFO", rotation="500 KB")

# ==============================================================================
# 💼 Fonction principale
# ==============================================================================
def main():
    # 📁 Définition des chemins
    DUCKDB_PATH = Path("/opt/airflow/data/bottleneck.duckdb")
    OUTPUTS_PATH = Path("/opt/airflow/data/outputs")
    OUTPUTS_PATH.mkdir(parents=True, exist_ok=True)

    if not DUCKDB_PATH.exists():
        logger.error(f"❌ Base DuckDB introuvable à {DUCKDB_PATH}")
        sys.exit(1)

    # 🦆 Connexion à DuckDB
    try:
        con = duckdb.connect(str(DUCKDB_PATH))
        logger.success(f"✅ Connexion à DuckDB : {DUCKDB_PATH}")
    except Exception as e:
        logger.error(f"❌ Erreur de connexion à DuckDB : {e}")
        sys.exit(1)

    # 🧹 Dédoublonnage ERP
    try:
        con.execute(f"""
            CREATE OR REPLACE TABLE erp_dedup AS
            SELECT 
                product_id,
                MAX(onsale_web)     AS onsale_web,
                MAX(price)          AS price,
                MAX(stock_quantity) AS stock_quantity,
                MAX(stock_status)   AS stock_status
            FROM read_csv_auto('{OUTPUTS_PATH}/erp_clean.csv')
            GROUP BY product_id
        """)
        logger.success("✅ Table erp_dedup créée avec regroupement par product_id.")
    except Exception as e:
        logger.error(f"❌ Erreur de dédoublonnage ERP : {e}")
        sys.exit(1)

    # 🔗 Dédoublonnage Liaison
    try:
        con.execute(f"""
            CREATE OR REPLACE TABLE liaison_dedup AS
            SELECT 
                product_id,
                MIN(id_web) AS id_web
            FROM read_csv_auto('{OUTPUTS_PATH}/liaison_clean.csv')
            GROUP BY product_id
        """)
        logger.success("✅ Table liaison_dedup créée avec MIN(id_web) par product_id.")
    except Exception as e:
        logger.error(f"❌ Erreur de dédoublonnage Liaison : {e}")
        sys.exit(1)

    # 🌐 Dédoublonnage Web
    try:
        con.execute(f"""
            CREATE OR REPLACE TABLE web_dedup AS
            SELECT * FROM (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY sku
                    ORDER BY post_date DESC
                ) AS rn
                FROM read_csv_auto('{OUTPUTS_PATH}/web_clean.csv')
                WHERE post_type = 'product'
            )
            WHERE rn = 1
        """)
        logger.success("✅ Table web_dedup créée avec filtre post_type='product' et ROW_NUMBER.")
    except Exception as e:
        logger.error(f"❌ Erreur de dédoublonnage Web : {e}")
        sys.exit(1)

    # ✅ Validation finale des données
    try:
        nb_erp = con.execute("SELECT COUNT(*) FROM erp_dedup").fetchone()[0]
        nb_web = con.execute("SELECT COUNT(*) FROM web_dedup").fetchone()[0]
        nb_liaison = con.execute("SELECT COUNT(*) FROM liaison_dedup").fetchone()[0]

        assert nb_erp > 0, "❌ Table erp_dedup vide"
        assert nb_web > 0, "❌ Table web_dedup vide"
        assert nb_liaison > 0, "❌ Table liaison_dedup vide"

        logger.info(f"✔️  Lignes dédoublonnées - ERP: {nb_erp}, Web: {nb_web}, Liaison: {nb_liaison}")
        logger.success("🎯 Dédoublonnage terminé avec succès et validé.")
    except Exception as e:
        logger.error(f"❌ Validation des tables dédoublonnées échouée : {e}")
        sys.exit(1)

# ==============================================================================
# 🚀 Point d'entrée du script
# ==============================================================================
if __name__ == "__main__":
    try:
        main()
        sys.exit(0)
    except Exception as e:
        logger.error(f"❌ Erreur inattendue : {e}")
        sys.exit(1)
