# === DAG : test_dependencies.py — Vérification des librairies installées ===
#
# Ce DAG teste l’import et l’usage de librairies clés (numpy, pandas, s3fs, etc.)
# Il écrit un fichier sur MinIO et tente une requête sur Trino.
# À placer dans : ./dags/test_dependencies.py

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import logging

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 0,
}

with DAG(
    dag_id='test_dependencies',
    default_args=default_args,
    schedule_interval=None,  # DAG à déclenchement manuel
    catchup=False,
    description="DAG de test des dépendances Python installées",
    tags=['test', 'dependencies'],
) as dag:

    def test_imports():
        import numpy as np
        import pandas as pd
        import s3fs
        import boto3
        import trino
        import duckdb
        import requests
        import openpyxl
        from loguru import logger

        logger.info("✅ Toutes les librairies ont été importées avec succès.")

    def write_to_minio():
        import pandas as pd
        import s3fs

        df = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
        fs = s3fs.S3FileSystem(
            key='minio',
            secret='minio123',
            client_kwargs={'endpoint_url': 'http://minio:9000'}
        )
        df.to_csv('s3://airflow/test_dependencies_output.csv', index=False, storage_options={
            'key': 'minio',
            'secret': 'minio123',
            'client_kwargs': {'endpoint_url': 'http://minio:9000'}
        })

    def query_postgres_via_trino():
        from trino.dbapi import connect
        conn = connect(
            host='trino',
            port=8080,
            user='Sorouz',
            catalog='postgresql',
            schema='public'
        )
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT * FROM some_table LIMIT 5")  # Modifie selon ton schéma
            results = cursor.fetchall()
            for row in results:
                logging.info(f"Trino result row: {row}")
        except Exception as e:
            logging.warning(f"Trino query failed : {e}")

    test_imports_task = PythonOperator(
        task_id='test_imports',
        python_callable=test_imports,
    )

    write_minio_task = PythonOperator(
        task_id='write_to_minio',
        python_callable=write_to_minio,
    )

    query_trino_task = PythonOperator(
        task_id='query_postgres_via_trino',
        python_callable=query_postgres_via_trino,
    )

    test_imports_task >> write_minio_task >> query_trino_task
