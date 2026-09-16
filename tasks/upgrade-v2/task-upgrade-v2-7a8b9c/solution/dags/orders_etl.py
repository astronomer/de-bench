"""Daily order ETL job."""

from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator


def extract_orders():
    return "orders"


with DAG(
    "orders_etl",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:
    PythonOperator(
        task_id="extract",
        python_callable=extract_orders,
    )
