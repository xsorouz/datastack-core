# === Script de test 08b - Vérification de l'absence de doublons après dédoublonnage ===
# Ce script contrôle que les tables dédoublonnées ne contiennent aucun doublon
# en vérifiant l’unicité des clés primaires logiques : product_id ou sku.

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

LOG_FILE = LOGS_PATH / "test_08_doublons.log"
logger.remove()
logger.add(sys.stdout, level="INFO")
logger.add(LOG_FILE, level="INFO", rotation="500 KB")

# ==============================================================================
# 🧪 Fonction principale : vérification des doublons
# ==============================================================================
def main():
    try:
        con = duckdb.connect("/opt/airflow/data/bottleneck.duckdb")
        logger.info("🧪 Connexion à DuckDB réussie.")
    except Exception as e:
        logger.error(f"❌ Connexion échouée : {e}")
        sys.exit(1)

    try:
        # 📦 Test sur erp_dedup (clé : product_id)
        erp_dup = con.execute("""
            SELECT COUNT(*) - COUNT(DISTINCT product_id) FROM erp_dedup
        """).fetchone()[0]
        assert erp_dup == 0, f"❌ Doublons détectés dans 'erp_dedup' : {erp_dup}"
        logger.success("✅ Aucune duplication sur 'erp_dedup' (clé = product_id)")

        # 🌐 Test sur web_dedup (clé : sku)
        web_dup = con.execute("""
            SELECT COUNT(*) - COUNT(DISTINCT sku) FROM web_dedup
        """).fetchone()[0]
        assert web_dup == 0, f"❌ Doublons détectés dans 'web_dedup' : {web_dup}"
        logger.success("✅ Aucune duplication sur 'web_dedup' (clé = sku)")

        # 🔗 Test sur liaison_dedup (clé : product_id)
        liaison_dup = con.execute("""
            SELECT COUNT(*) - COUNT(DISTINCT product_id) FROM liaison_dedup
        """).fetchone()[0]
        assert liaison_dup == 0, f"❌ Doublons détectés dans 'liaison_dedup' : {liaison_dup}"
        logger.success("✅ Aucune duplication sur 'liaison_dedup' (clé = product_id)")

        logger.success("🎯 Test d’unicité post-dédoublonnage validé avec succès.")

    except Exception as e:
        logger.error(f"❌ Erreur lors de la vérification des doublons : {e}")
        sys.exit(1)

# ==============================================================================
# 🚀 Lancement
# ==============================================================================
if __name__ == "__main__":
    main()
