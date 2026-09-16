"""Asset-driven DAG — produces and consumes a single asset."""

from datetime import datetime

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, Asset

ORDERS = Asset("s3://warehouse/orders/")


def _produce():
    return "wrote orders parquet"


def _consume():
    return "loaded orders into staging"


with DAG(
    "produce_orders",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as producer:
    PythonOperator(
        task_id="produce_orders",
        python_callable=_produce,
        outlets=[ORDERS],
    )


with DAG(
    "consume_orders",
    schedule=[ORDERS],
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as consumer:
    PythonOperator(task_id="load_orders", python_callable=_consume)
