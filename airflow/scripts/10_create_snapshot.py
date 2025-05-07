# === Script 10 - Snapshot de la base DuckDB après fusion ===
# Ce script copie le fichier de base de données DuckDB après fusion
# pour créer une sauvegarde figée dans '/opt/airflow/data/snapshots/'.

import os
import sys
import shutil
from pathlib import Path
from loguru import logger

# ==============================================================================
# 🔧 Initialisation des logs
# ==============================================================================
AIRFLOW_LOG_PATH = os.getenv("AIRFLOW_LOG_PATH", "logs")
LOGS_PATH = Path(AIRFLOW_LOG_PATH)
LOGS_PATH.mkdir(parents=True, exist_ok=True)

LOG_FILE = LOGS_PATH / "snapshot_duckdb.log"
logger.remove()
logger.add(sys.stdout, level="INFO")
logger.add(LOG_FILE, level="INFO", rotation="500 KB")

# ==============================================================================
# 💾 Fonction principale : snapshot DuckDB
# ==============================================================================
def main():
    DATA_PATH = Path("/opt/airflow/data")
    SOURCE_DUCKDB = DATA_PATH / "bottleneck.duckdb"
    SNAPSHOT_DIR = DATA_PATH / "snapshots"
    SNAPSHOT_FILE = SNAPSHOT_DIR / "bottleneck_fusion_ok.duckdb"

    # ✅ Vérification de la base source
    if not SOURCE_DUCKDB.exists():
        logger.error(f"❌ Fichier source introuvable : {SOURCE_DUCKDB}")
        sys.exit(1)

    try:
        SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)
        shutil.copy2(SOURCE_DUCKDB, SNAPSHOT_FILE)
        logger.success(f"✅ Snapshot créé avec succès : {SNAPSHOT_FILE}")
    except Exception as e:
        logger.error(f"❌ Erreur lors de la création du snapshot : {e}")
        sys.exit(1)

    logger.success("🎯 Sauvegarde de la base DuckDB terminée.")

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
