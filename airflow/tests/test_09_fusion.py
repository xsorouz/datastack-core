# === Script de test 09 - Validation de la fusion des données ===
# Ce script vérifie que la table 'fusion' existe, contient exactement 714 lignes,
# et inclut bien toutes les colonnes critiques nécessaires aux étapes suivantes.

import os
import sys
import duckdb
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

LOG_FILE = LOGS_PATH / "test_09_fusion.log"
logger.remove()
logger.add(sys.stdout, level="INFO")
logger.add(LOG_FILE, level="INFO", rotation="500 KB")

# ==============================================================================
# 🧪 Fonction principale : test de la table fusion
# ==============================================================================
def main():
    try:
        con = duckdb.connect("/opt/airflow/data/bottleneck.duckdb")
        logger.info("🧪 Connexion à DuckDB établie.")
    except Exception as e:
        logger.error(f"❌ Erreur de connexion à DuckDB : {e}")
        sys.exit(1)

    try:
        # 📏 Vérification du nombre de lignes
        nb_rows = con.execute("SELECT COUNT(*) FROM fusion").fetchone()[0]
        assert nb_rows == 714, f"❌ La table fusion contient {nb_rows} lignes (attendu : 714)"
        logger.success(f"✅ Nombre de lignes dans la table fusion : {nb_rows}")

        # 🧱 Vérification des colonnes critiques
        columns = con.execute("PRAGMA table_info('fusion')").fetchdf()["name"].tolist()
        colonnes_attendues = ["product_id", "price", "stock_status", "post_title"]

        for col in colonnes_attendues:
            assert col in columns, f"❌ Colonne manquante dans fusion : {col}"
        logger.success("✅ Toutes les colonnes critiques sont présentes : " + ", ".join(colonnes_attendues))

        logger.success("🎯 Test de validation de la table fusion passé avec succès.")

    except Exception as e:
        logger.error(f"❌ Échec du test de la table fusion : {e}")
        sys.exit(1)

# ==============================================================================
# 🚀 Lancement
# ==============================================================================
if __name__ == "__main__":
    main()
