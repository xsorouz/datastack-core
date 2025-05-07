# === Script 01 - Conversion Excel ➔ CSV (robuste et compatible Airflow) ===
# Ce script convertit les fichiers Excel présents dans 'data/inputs/'
# en fichiers CSV avec un nettoyage minimal (lignes/colonnes vides).
# Il est conçu pour s'intégrer dans un pipeline Airflow.

import os
import sys
import warnings
from pathlib import Path
import pandas as pd
from loguru import logger

warnings.filterwarnings("ignore")

# ==============================================================================
# 🔧 Initialisation des logs
# ==============================================================================
AIRFLOW_LOG_PATH = os.getenv("AIRFLOW_LOG_PATH", "logs")
LOGS_PATH = Path(AIRFLOW_LOG_PATH)
LOGS_PATH.mkdir(parents=True, exist_ok=True)

LOG_FILE = LOGS_PATH / "conversion_excel_csv.log"
logger.remove()
logger.add(sys.stdout, level="INFO")
logger.add(LOG_FILE, level="INFO", rotation="500 KB")

# ==============================================================================
# 📁 Répertoires de travail
# ==============================================================================
INPUTS_PATH = Path("/opt/airflow/data/inputs")
OUTPUTS_PATH = INPUTS_PATH  # Même dossier pour les fichiers CSV

INPUTS_PATH.mkdir(parents=True, exist_ok=True)
OUTPUTS_PATH.mkdir(parents=True, exist_ok=True)

# ==============================================================================
# 🗂️ Fichiers à convertir (Excel ➝ CSV)
# ==============================================================================
FILES_MAPPING = {
    "Fichier_erp.xlsx": "erp.csv",
    "Fichier_web.xlsx": "web.csv",
    "fichier_liaison.xlsx": "liaison.csv",
}

# ==============================================================================
# 🧹 Nettoyage minimal des DataFrames
# ==============================================================================
def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df = df.dropna(how="all", axis=0)  # Supprime les lignes vides
    df = df.dropna(how="all", axis=1)  # Supprime les colonnes vides
    return df

# ==============================================================================
# 🚀 Fonction principale
# ==============================================================================
def main():
    logger.info("🔄 Démarrage de la conversion des fichiers Excel vers CSV...")

    for excel_file, csv_file in FILES_MAPPING.items():
        excel_path = INPUTS_PATH / excel_file
        csv_path = OUTPUTS_PATH / csv_file

        # Vérification de l'existence du fichier Excel
        if not excel_path.exists():
            logger.error(f"❌ Fichier manquant : {excel_path}")
            sys.exit(1)

        try:
            # Lecture et nettoyage
            df = pd.read_excel(excel_path)
            df = clean_dataframe(df)

            # Export CSV
            df.to_csv(csv_path, index=False)

            # Contrôles post-export
            if not csv_path.exists():
                raise FileNotFoundError(f"❌ Fichier CSV non généré : {csv_path}")
            if df.empty:
                raise ValueError(f"❌ Fichier CSV vide généré : {csv_file}")

            logger.success(f"✅ Conversion réussie : {excel_file} ➔ {csv_file} ({len(df)} lignes)")

        except Exception as e:
            logger.error(f"❌ Erreur lors de la conversion de {excel_file} : {e}")
            sys.exit(1)

    logger.success("🎯 Tous les fichiers Excel ont été convertis avec succès.")

# ==============================================================================
# 📌 Exécution directe
# ==============================================================================
if __name__ == "__main__":
    main()
