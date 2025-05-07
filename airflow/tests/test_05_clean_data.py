# === Script de test 05 - Validation du nettoyage des données ===
# Ce script vérifie que les tables nettoyées (erp_clean, web_clean, liaison_clean)
# existent dans DuckDB, sont non vides, et prêtes pour le dédoublonnage.

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

LOG_FILE = LOGS_PATH / "test_05_clean_data.log"
logger.remove()
logger.add(sys.stdout, level="INFO")
logger.add(LOG_FILE, level="INFO", rotation="500 KB")

# ==============================================================================
# 🧪 Fonction principale : vérification des tables nettoyées
# ==============================================================================
def main():
    try:
        con = duckdb.connect("/opt/airflow/data/bottleneck.duckdb")
        logger.info("🧪 Connexion à DuckDB réussie.")
    except Exception as e:
        logger.error(f"❌ Échec de connexion à DuckDB : {e}")
        sys.exit(1)

    try:
        # Vérifie la présence et le peuplement des tables nettoyées
        nb_erp = con.execute("SELECT COUNT(*) FROM erp_clean").fetchone()[0]
        nb_web = con.execute("SELECT COUNT(*) FROM web_clean").fetchone()[0]
        nb_liaison = con.execute("SELECT COUNT(*) FROM liaison_clean").fetchone()[0]

        assert nb_erp > 0, "❌ La table 'erp_clean' est vide"
        assert nb_web > 0, "❌ La table 'web_clean' est vide"
        assert nb_liaison > 0, "❌ La table 'liaison_clean' est vide"

        logger.success(f"✅ erp_clean : {nb_erp} lignes")
        logger.success(f"✅ web_clean : {nb_web} lignes")
        logger.success(f"✅ liaison_clean : {nb_liaison} lignes")
        logger.success("🎯 Données nettoyées valides et prêtes pour le dédoublonnage.")

    except Exception as e:
        logger.error(f"❌ Erreur durant la validation des données nettoyées : {e}")
        sys.exit(1)

# ==============================================================================
# 🚀 Point d’entrée
# ==============================================================================
if __name__ == "__main__":
    main()
