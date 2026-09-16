"""Placeholder DAG — the audit task will be added alongside this."""

from datetime import datetime

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

dag = DAG(
    "audit_job",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
)

PythonOperator(task_id="noop", python_callable=lambda: None, dag=dag)
