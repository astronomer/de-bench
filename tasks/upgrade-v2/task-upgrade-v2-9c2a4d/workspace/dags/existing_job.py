"""Placeholder DAG that already exists in the project."""

from datetime import datetime

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

dag = DAG(
    "existing_job",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
)

PythonOperator(
    task_id="placeholder",
    python_callable=lambda: None,
    dag=dag,
)
