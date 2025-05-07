# === Script de test 11 - Validation du calcul du chiffre d'affaires ===
# Ce script vérifie la cohérence des tables `ca_par_produit` et `ca_total` dans DuckDB :
# - Montant du chiffre d'affaires total attendu (387837.60 €)
# - Nombre de lignes de produits (573)
# - Absence de valeurs nulles ou négatives

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

LOG_FILE = LOGS_PATH / "test_11_validate_ca.log"
logger.remove()
logger.add(sys.stdout, level="INFO")
logger.add(LOG_FILE, level="INFO", rotation="500 KB")

# ==============================================================================
# 🧪 Fonction principale : vérifications sur les tables de CA
# ==============================================================================
def main():
    try:
        con = duckdb.connect("/opt/airflow/data/bottleneck.duckdb")
        logger.success("✅ Connexion à DuckDB établie.")
    except Exception as e:
        logger.error(f"❌ Erreur de connexion à DuckDB : {e}")
        sys.exit(1)

    try:
        # 💰 Vérification du chiffre d'affaires total
        ca_total = con.execute("SELECT ca_total FROM ca_total").fetchone()[0]
        assert round(ca_total, 2) == 387837.60, (
            f"❌ Montant du CA incorrect : {ca_total:.2f} € (attendu : 387837.60 €)"
        )
        logger.success(f"💰 CA total validé : {ca_total:,.2f} € ✅")

        # 📦 Vérification du nombre de produits
        nb_produits = con.execute("SELECT COUNT(*) FROM ca_par_produit").fetchone()[0]
        assert nb_produits == 573, (
            f"❌ Nombre de lignes incorrect : {nb_produits} (attendu : 573)"
        )
        logger.success(f"📦 Nombre de produits dans ca_par_produit : {nb_produits} ✅")

        # 🧼 Vérification de la présence de valeurs nulles
        nb_nulls = con.execute("""
            SELECT COUNT(*) FROM ca_par_produit
            WHERE product_id IS NULL OR post_title IS NULL
              OR price IS NULL OR stock_quantity IS NULL
              OR chiffre_affaires IS NULL
        """).fetchone()[0]
        assert nb_nulls == 0, f"❌ Valeurs nulles détectées dans ca_par_produit : {nb_nulls} lignes"
        logger.success("✅ Aucune valeur nulle détectée dans ca_par_produit")

        # 🚫 Vérification d'absence de CA négatif
        nb_negatifs = con.execute("""
            SELECT COUNT(*) FROM ca_par_produit
            WHERE chiffre_affaires < 0
        """).fetchone()[0]
        assert nb_negatifs == 0, f"❌ CA négatif détecté pour {nb_negatifs} ligne(s)"
        logger.success("✅ Aucun CA négatif détecté dans ca_par_produit")

        logger.success("🎯 Tous les tests de validation du CA sont passés avec succès.")

    except Exception as e:
        logger.error(f"❌ Erreur dans les tests de CA : {e}")
        sys.exit(1)

# ==============================================================================
# 🚀 Lancement
# ==============================================================================
if __name__ == "__main__":
    main()
