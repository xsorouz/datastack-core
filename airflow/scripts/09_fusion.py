# === Script 09 - Fusion des tables dédoublonnées en une table finale ===
# Ce script fusionne les tables erp_dedup, liaison_dedup et web_dedup dans DuckDB.
# Il vérifie que le nombre de lignes correspond à 714 et exporte la table fusionnée en CSV.

import os
import sys
import warnings
from pathlib import Path
import duckdb
import pandas as pd
from loguru import logger

# ==============================================================================
# 🔧 Initialisation des logs
# ==============================================================================
warnings.filterwarnings("ignore")

AIRFLOW_LOG_PATH = os.getenv("AIRFLOW_LOG_PATH", "logs")
LOGS_PATH = Path(AIRFLOW_LOG_PATH)
LOGS_PATH.mkdir(parents=True, exist_ok=True)

LOG_FILE = LOGS_PATH / "fusion.log"
logger.remove()
logger.add(sys.stdout, level="INFO")
logger.add(LOG_FILE, level="INFO", rotation="500 KB")

# ==============================================================================
# 🔗 Fonction principale : fusion logique
# ==============================================================================
def main():
    DUCKDB_PATH = Path("/opt/airflow/data/bottleneck.duckdb")
    OUTPUT_PATH = Path("/opt/airflow/data/outputs/fusion.csv")

    if not DUCKDB_PATH.exists():
        logger.error(f"❌ Base DuckDB introuvable à {DUCKDB_PATH}")
        sys.exit(1)

    # 🦆 Connexion à DuckDB
    try:
        con = duckdb.connect(str(DUCKDB_PATH))
        logger.success(f"✅ Connexion à DuckDB établie : {DUCKDB_PATH}")
    except Exception as e:
        logger.error(f"❌ Erreur de connexion à DuckDB : {e}")
        sys.exit(1)

    # 🔄 Création de la table fusionnée
    try:
        con.execute("""
            CREATE OR REPLACE TABLE fusion AS
            SELECT
                e.product_id,
                e.onsale_web,
                e.price,
                e.stock_quantity,
                e.stock_status,
                w.post_title,
                w.post_excerpt,
                w.post_status,
                w.post_type,
                w.average_rating,
                w.total_sales
            FROM erp_dedup e
            JOIN liaison_dedup l ON e.product_id = l.product_id
            JOIN web_dedup w ON l.id_web = w.sku
        """)
        logger.success("✅ Table 'fusion' créée avec succès à partir des jointures.")
    except Exception as e:
        logger.error(f"❌ Erreur lors de la création de la table fusion : {e}")
        sys.exit(1)

    # 📊 Validation et export
    try:
        nb_rows = con.execute("SELECT COUNT(*) FROM fusion").fetchone()[0]
        if nb_rows != 714:
            logger.warning(f"⚠️ La table fusion contient {nb_rows} lignes (attendu : 714)")
        else:
            logger.info("✔️ Nombre de lignes attendu : 714")

        fusion_df = con.execute("SELECT * FROM fusion").fetchdf()
        fusion_df.to_csv(OUTPUT_PATH, index=False)
        logger.success(f"📁 Table fusion exportée avec succès : {OUTPUT_PATH}")
    except Exception as e:
        logger.error(f"❌ Erreur lors de la validation ou de l'export : {e}")
        sys.exit(1)

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
