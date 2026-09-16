"""Hourly orders ETL.

Polls the upstream orders API until the hourly extract is ready, then
loads and enriches the rows the API returns. The poll runs as a
deferrable wait so the worker slot is released between checks.

The reusable account-enrichment helpers now live in the importable
``common`` package and the deferrable trigger lives in the importable
``triggers`` package, so the dag resolves them the same way from any
process without manipulating sys.path.
"""

import pendulum

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from common.order_utils import enrich_accounts, fetch_orders
from triggers.orders_trigger import OrdersReadyTrigger  # noqa: F401


def wait_for_extract(**context):
    # Defers to OrdersReadyTrigger; on resume the API has the rows ready.
    return {"region": "us-east", "poll_interval": pendulum.duration(minutes=5)}


def load_orders(**context):
    region = context["ti"].xcom_pull(task_ids="wait_for_extract", key="return_value")["region"]
    rows = fetch_orders(region)
    return enrich_accounts(rows)


with DAG(
    dag_id="orders_etl",
    schedule="@hourly",
    start_date=pendulum.today("UTC").subtract(days=2),
    catchup=False,
) as dag:
    wait = PythonOperator(
        task_id="wait_for_extract",
        python_callable=wait_for_extract,
    )
    load = PythonOperator(
        task_id="load_orders",
        python_callable=load_orders,
    )

    wait >> load
