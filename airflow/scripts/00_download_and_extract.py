# === Script 00 - Téléchargement et extraction ZIP (Airflow-compatible) ===
# Ce script télécharge une archive ZIP depuis une URL,
# extrait les fichiers Excel dans 'data/inputs/', les renomme de manière sécurisée
# (ASCII-safe), puis vérifie leur présence pour garantir la suite du pipeline.

import os
import sys
import re
import unicodedata
import requests
from zipfile import ZipFile
from io import BytesIO
from pathlib import Path
from loguru import logger

# ==============================================================================
# 🔧 Configuration des chemins de logs
# ==============================================================================
AIRFLOW_LOG_PATH = os.getenv("AIRFLOW_LOG_PATH", "logs")
LOGS_PATH = Path(AIRFLOW_LOG_PATH)
LOGS_PATH.mkdir(parents=True, exist_ok=True)

LOG_FILE = LOGS_PATH / "download_extract.log"
logger.remove()
logger.add(sys.stdout, level="INFO")
logger.add(LOG_FILE, level="INFO", rotation="500 KB")

# ==============================================================================
# 📁 Chemin de destination des fichiers extraits
# ==============================================================================
INPUTS_PATH = Path("/opt/airflow/data/inputs")
INPUTS_PATH.mkdir(parents=True, exist_ok=True)

# ==============================================================================
# 🌐 Paramètres de l'archive à télécharger
# ==============================================================================
ZIP_URL = (
    "https://s3.eu-west-1.amazonaws.com/course.oc-static.com/projects/922_Data+Engineer/922_P10/bottleneck.zip"
)

EXPECTED_FILES = [
    "Fichier_erp.xlsx",
    "Fichier_web.xlsx",
    "fichier_liaison.xlsx",
]

# ==============================================================================
# 🔤 Normalisation ASCII sécurisée des noms de fichiers
# ==============================================================================
def normalize_filename(filename: str) -> str:
    nfkd = unicodedata.normalize("NFKD", filename)
    ascii_name = nfkd.encode("ASCII", "ignore").decode("ASCII")
    return re.sub(r"[^A-Za-z0-9_.-]", "_", ascii_name)

# ==============================================================================
# 📥 Téléchargement de l'archive ZIP
# ==============================================================================
def download_zip(url: str) -> bytes:
    logger.info(f"📦 Téléchargement de l'archive : {url}")
    try:
        response = requests.get(url)
        response.raise_for_status()
        logger.success("✅ Archive ZIP téléchargée avec succès.")
        return response.content
    except Exception as e:
        logger.error(f"❌ Erreur pendant le téléchargement : {e}")
        raise

# ==============================================================================
# 📂 Extraction et renommage sécurisé des fichiers
# ==============================================================================
def extract_and_normalize(zip_content: bytes, output_dir: Path) -> list:
    logger.info("📂 Début de l'extraction et du renommage des fichiers...")
    extracted_files = []

    try:
        with ZipFile(BytesIO(zip_content)) as zip_ref:
            for member in zip_ref.infolist():
                original_name = Path(member.filename).name
                if not original_name:
                    continue  # Ignore les dossiers

                safe_name = normalize_filename(original_name)
                target_path = output_dir / safe_name

                with open(target_path, "wb") as f:
                    f.write(zip_ref.read(member))

                extracted_files.append(safe_name)
                logger.info(f"✅ Fichier extrait : {safe_name}")

        logger.success(f"📁 Extraction terminée dans : {output_dir.resolve()}")
        return extracted_files

    except Exception as e:
        logger.error(f"❌ Erreur pendant l'extraction : {e}")
        raise

# ==============================================================================
# ✅ Validation des fichiers extraits
# ==============================================================================
def validate_files(expected: list, actual: list, base_dir: Path):
    expected_normalized = [normalize_filename(f) for f in expected]
    missing = [f for f in expected_normalized if f not in actual or not (base_dir / f).exists()]

    if missing:
        logger.error(f"❌ Fichiers manquants après extraction : {missing}")
        raise FileNotFoundError(f"Fichiers attendus manquants : {missing}")

    logger.success("🎯 Tous les fichiers attendus sont présents :")
    for f in expected_normalized:
        logger.info(f"   - {f}")

# ==============================================================================
# 🚀 Point d’entrée principal
# ==============================================================================
def main():
    zip_bytes = download_zip(ZIP_URL)
    extracted = extract_and_normalize(zip_bytes, INPUTS_PATH)
    validate_files(EXPECTED_FILES, extracted, INPUTS_PATH)
    logger.success("🎉 Téléchargement, extraction et validation terminés avec succès.")
    return 0

# ==============================================================================
# 📌 Lancement
# ==============================================================================
if __name__ == "__main__":
    try:
        main()
        print("✔️ Script terminé avec succès.")
        sys.exit(0)
    except Exception as e:
        logger.error(f"💥 Erreur inattendue : {e}")
        sys.exit(1)
