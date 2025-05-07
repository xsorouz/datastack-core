# === Script de test 08 - Validation des tables dédoublonnées ===
# Ce script vérifie que les tables dédoublonnées (erp_dedup, web_dedup, liaison_dedup)
# existent dans la base DuckDB, contiennent des données, et sont prêtes pour la fusion.

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

LOG_FILE = LOGS_PATH / "test_08_dedoublonnage.log"
logger.remove()
logger.add(sys.stdout, level="INFO")
logger.add(LOG_FILE, level="INFO", rotation="500 KB")

# ==============================================================================
# 🧪 Fonction principale : validation de la présence des tables dédoublonnées
# ==============================================================================
def main():
    try:
        con = duckdb.connect("/opt/airflow/data/bottleneck.duckdb")
        logger.info("🧪 Connexion à DuckDB établie.")
    except Exception as e:
        logger.error(f"❌ Erreur de connexion à DuckDB : {e}")
        sys.exit(1)

    try:
        # Validation de l'existence et du contenu des 3 tables dédoublonnées
        for table_name in ["erp_dedup", "web_dedup", "liaison_dedup"]:
            count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            assert count > 0, f"❌ La table {table_name} est vide ou n’a pas été créée."
            logger.success(f"✅ {table_name} : {count} lignes présentes.")

        logger.success("🎯 Toutes les tables dédoublonnées sont valides et prêtes à être utilisées.")

    except Exception as e:
        logger.error(f"❌ Échec du test de dédoublonnage : {e}")
        sys.exit(1)

# ==============================================================================
# 🚀 Lancement
# ==============================================================================
if __name__ == "__main__":
    main()
