# === Script de test 12 - Validation du fichier vins_millesimes.csv ===
# Ce script teste que le fichier contenant les vins millésimés a bien été généré,
# qu’il contient exactement 30 lignes, et que les colonnes 'price' et 'z_score'
# ne contiennent ni valeurs nulles, ni infinies.

import os
import sys
import pandas as pd
from pathlib import Path
from loguru import logger

# ==============================================================================
# 🔧 Configuration des chemins de logs
# ==============================================================================
AIRFLOW_LOG_PATH = os.getenv("AIRFLOW_LOG_PATH", "logs")
LOGS_PATH = Path(AIRFLOW_LOG_PATH)
LOGS_PATH.mkdir(parents=True, exist_ok=True)

LOG_FILE = LOGS_PATH / "test_12_validate_zscore.log"
logger.remove()
logger.add(sys.stdout, level="INFO")
logger.add(LOG_FILE, level="INFO", rotation="500 KB")

# ==============================================================================
# 🧪 Fonction principale : tests de validation du Z-score
# ==============================================================================
def main():
    path = Path("/opt/airflow/data/outputs/vins_millesimes.csv")

    # 📁 Vérification de la présence du fichier
    if not path.exists():
        logger.error("❌ Fichier manquant : vins_millesimes.csv introuvable.")
        sys.exit(1)

    try:
        df = pd.read_csv(path)
        nb_lignes = df.shape[0]
        logger.info(f"📄 Fichier chargé : {nb_lignes} lignes")

        # ✅ Vérification du nombre attendu de vins millésimés
        assert nb_lignes == 30, f"❌ Nombre de lignes incorrect : {nb_lignes} (attendu : 30)"
        logger.success(f"🍷 Nombre de vins millésimés correct : {nb_lignes}")

        # ✅ Vérification des colonnes numériques
        for col in ["price", "z_score"]:
            nulls = df[col].isnull().sum()
            infs = df[col].isin([float("inf"), float("-inf")]).sum()

            assert nulls == 0, f"❌ {col} contient {nulls} valeur(s) nulle(s)"
            assert infs == 0, f"❌ {col} contient {infs} valeur(s) infinie(s)"

            logger.success(f"✅ Colonne '{col}' : pas de NaN, pas d'inf.")

        logger.success("🎯 Test de validation du fichier vins_millesimes.csv terminé avec succès.")

    except Exception as e:
        logger.error(f"❌ Erreur lors du test de validation Z-score : {e}")
        sys.exit(1)

# ==============================================================================
# 🚀 Lancement
# ==============================================================================
if __name__ == "__main__":
    main()
