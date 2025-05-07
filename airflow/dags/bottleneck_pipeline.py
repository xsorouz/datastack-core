from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from datetime import timedelta
import pendulum

# =============================================
# Paramètres du DAG
# =============================================
default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
    'depends_on_past': False,
}

with DAG(
    dag_id='bottleneck_pipeline',
    default_args=default_args,
    description='Pipeline démonstrateur BottleNeck - version Airflow',
    schedule='0 9 15 * *',  # Tous les 15 du mois à 9h
    start_date=pendulum.today('UTC').add(days=-1),
    catchup=False,
    tags=['bottleneck', 'pipeline', 'airflow'],
) as dag:

    # 📦 Téléchargement des données
    telechargement_donnees = BashOperator(
        task_id='telechargement_donnees',
        bash_command='python /opt/airflow/scripts/00_download_and_extract.py',
    )

    # 📄 Conversion Excel -> CSV
    conversion_excel_csv = BashOperator(
        task_id='conversion_excel_csv',
        bash_command='python /opt/airflow/scripts/01_excel_to_csv.py',
    )

    # ☁️ Upload CSV bruts
    upload_csv_bruts = BashOperator(
        task_id='upload_csv_bruts',
        bash_command='python /opt/airflow/scripts/02_upload_to_minio.py',
    )

    # ✅ Vérification de l'upload
    verification_upload = BashOperator(
        task_id='verification_upload',
        bash_command='python /opt/airflow/scripts/03_verify_upload.py',
    )

    # 🧹 Nettoyage des données
    nettoyage_donnees = BashOperator(
        task_id='nettoyage_donnees',
        bash_command='python /opt/airflow/scripts/05_clean_data.py',
    )

    test_nettoyage = BashOperator(
        task_id='test_nettoyage',
        bash_command='python /opt/airflow/tests/test_05_clean_data.py',
    )

    test_nulls = BashOperator(
        task_id='test_nulls_post_nettoyage',
        bash_command='python /opt/airflow/tests/test_05_nulls_clean_data.py',
    )

    # 📅 Upload des fichiers nettoyés
    upload_clean = BashOperator(
        task_id='upload_clean',
        bash_command='python /opt/airflow/scripts/06_upload_clean_to_minio.py',
    )

    # 🗒️ Dédoublonnage
    with TaskGroup('dedoublonnage_group', tooltip="Dédoublonnage + tests") as dedoublonnage_group:
        dedoublonnage = BashOperator(
            task_id='dedoublonnage',
            bash_command='python /opt/airflow/scripts/08_dedoublonnage.py',
        )
        test_dedoublonnage = BashOperator(
            task_id='test_08_presence_tables',
            bash_command='python /opt/airflow/tests/test_08_dedoublonnage.py',
        )
        test_08_doublons = BashOperator(
            task_id='test_08_aucun_doublon',
            bash_command='python /opt/airflow/tests/test_08_doublons.py',
        )
        dedoublonnage >> [test_dedoublonnage, test_08_doublons]

    # 🔗 Fusion
    with TaskGroup('fusion_group', tooltip="Fusion + tests") as fusion_group:
        fusion = BashOperator(
            task_id='fusion',
            bash_command='python /opt/airflow/scripts/09_fusion.py',
        )
        tests_fusion = BashOperator(
            task_id='tests_fusion',
            bash_command='python /opt/airflow/tests/test_09_fusion.py',
        )
        fusion >> tests_fusion

    # 💾 Snapshot de la base
    snapshot_base = BashOperator(
        task_id='snapshot_base',
        bash_command='python /opt/airflow/scripts/10_create_snapshot.py',
    )

    # ✨ Calcul CA & Z-score parallèle
    with TaskGroup('calculs_parallel', tooltip="CA et Z-score") as calculs_parallel:
        with TaskGroup('ca_group', tooltip="Chiffre d\'affaires") as ca_group:
            calcul_ca = BashOperator(
                task_id='calcul_ca',
                bash_command='python /opt/airflow/scripts/11_calcul_ca.py',
            )
            test_ca = BashOperator(
                task_id='test_ca',
                bash_command='python /opt/airflow/tests/test_11_validate_ca.py',
            )
            calcul_ca >> test_ca

        with TaskGroup('zscore_group', tooltip="Z-score") as zscore_group:
            calcul_zscore = BashOperator(
                task_id='calcul_zscore',
                bash_command='python /opt/airflow/scripts/12_calcul_zscore_upload.py',
            )
            test_zscore = BashOperator(
                task_id='test_zscore',
                bash_command='python /opt/airflow/tests/test_12_validate_zscore.py',
            )
            calcul_zscore >> test_zscore

    # 📈 Rapport final
    rapport_final = BashOperator(
        task_id='rapport_final',
        bash_command='python /opt/airflow/scripts/13_generate_final_report.py',
    )

    # ☁️ Upload des logs
    upload_logs_final = BashOperator(
        task_id='upload_logs_final',
        bash_command='python /opt/airflow/scripts/14_upload_all_logs.py',
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # =============================================
    # Orchestration des dépendances
    # =============================================
    (
        telechargement_donnees
        >> conversion_excel_csv
        >> upload_csv_bruts
        >> verification_upload
        >> nettoyage_donnees
        >> [test_nettoyage, test_nulls]
        >> upload_clean
        >> dedoublonnage_group
        >> fusion_group
        >> snapshot_base
        >> calculs_parallel
        >> rapport_final
        >> upload_logs_final
    )
