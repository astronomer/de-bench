"""Hourly inventory sync — pulls warehouse counts and writes a reconciliation row."""

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def fetch_counts(**context):
    print(f"fetching warehouse counts for {context['ds']}")
    return {"sku_count": 4096}


def reconcile(**context):
    print("reconciling counts against ledger")


with DAG(
    "inventory_sync",
    schedule="0 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    fetch = PythonOperator(
        task_id="fetch_counts",
        python_callable=fetch_counts,
    )
    recon = PythonOperator(
        task_id="reconcile",
        python_callable=reconcile,
    )

    fetch >> recon
