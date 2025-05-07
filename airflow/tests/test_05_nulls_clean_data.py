# === Script de test 05 - Vérification des NULLs après nettoyage ===
# Ce script vérifie que les colonnes critiques des tables nettoyées
# (erp_clean, web_clean, liaison_clean) ne contiennent aucune valeur NULL.

import os
import sys
import duckdb
from pathlib import Path
from loguru import logger
import warnings

warnings.filterwarnings("ignore")

# ==============================================================================
# 🔧 Configuration des logs
# ==============================================================================
AIRFLOW_LOG_PATH = os.getenv("AIRFLOW_LOG_PATH", "logs")
LOGS_PATH = Path(AIRFLOW_LOG_PATH)
LOGS_PATH.mkdir(parents=True, exist_ok=True)

LOG_FILE = LOGS_PATH / "test_05_nulls_clean_data.log"
logger.remove()
logger.add(sys.stdout, level="INFO")
logger.add(LOG_FILE, level="INFO", rotation="500 KB")

# ==============================================================================
# 🧪 Fonction principale : validation des NULLs
# ==============================================================================
def main():
    try:
        con = duckdb.connect("/opt/airflow/data/bottleneck.duckdb")
        logger.info("🧪 Connexion à DuckDB établie.")
    except Exception as e:
        logger.error(f"❌ Erreur de connexion à DuckDB : {e}")
        sys.exit(1)

    try:
        # Vérification table erp_clean
        erp_nulls = con.execute("""
            SELECT COUNT(*) FROM erp_clean
            WHERE product_id IS NULL OR onsale_web IS NULL
              OR price IS NULL OR stock_quantity IS NULL OR stock_status IS NULL
        """).fetchone()[0]
        assert erp_nulls == 0, f"❌ NULLs détectés dans 'erp_clean' : {erp_nulls}"

        # Vérification table web_clean
        web_nulls = con.execute("""
            SELECT COUNT(*) FROM web_clean
            WHERE sku IS NULL
        """).fetchone()[0]
        assert web_nulls == 0, f"❌ NULLs détectés dans 'web_clean' : {web_nulls}"

        # Vérification table liaison_clean
        liaison_nulls = con.execute("""
            SELECT COUNT(*) FROM liaison_clean
            WHERE product_id IS NULL OR id_web IS NULL
        """).fetchone()[0]
        assert liaison_nulls == 0, f"❌ NULLs détectés dans 'liaison_clean' : {liaison_nulls}"

        logger.success("✅ Aucune valeur NULL détectée dans les tables nettoyées.")
        logger.success("🎯 Test de validation des NULLs terminé avec succès.")

    except Exception as e:
        logger.error(f"❌ Échec du test de validation des NULLs : {e}")
        sys.exit(1)

# ==============================================================================
# 🚀 Lancement
# ==============================================================================
if __name__ == "__main__":
    main()
