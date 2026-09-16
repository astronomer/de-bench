"""Hourly orders ETL.

Polls the upstream orders API until the hourly extract is ready, then
loads and enriches the rows the API returns. The poll runs as a
deferrable wait so the worker slot is released between checks.

The reusable account-enrichment helpers live in a shared folder beside
the dags folder. They are picked up by putting that folder on the
import path at parse time and importing the module by its bare name —
that is how every DAG in this project has always found them.
"""

import os
import sys
from datetime import timedelta

# Make the shared helper module importable. The project root used to be
# on sys.path implicitly so a bare `import order_utils` resolved; we add
# the shared folder explicitly here so the dags keep parsing.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), os.pardir, "common"))

import order_utils  # noqa: E402

from airflow import DAG  # noqa: E402
from airflow.operators.python import PythonOperator  # noqa: E402
from airflow.utils.dates import days_ago  # noqa: E402
from airflow.triggers.base import BaseTrigger, TriggerEvent  # noqa: E402


class OrdersReadyTrigger(BaseTrigger):
    """Fires once the upstream API reports the hourly extract is ready."""

    def __init__(self, region: str) -> None:
        super().__init__()
        self.region = region

    def serialize(self):
        return (
            "orders_etl.OrdersReadyTrigger",
            {"region": self.region},
        )

    async def run(self):
        # Real code awaits the API; the bench just needs the shape.
        yield TriggerEvent({"region": self.region, "ready": True})


def wait_for_extract(**context):
    # Defers to OrdersReadyTrigger; on resume the API has the rows ready.
    return {"region": "us-east", "poll_interval": timedelta(minutes=5)}


def load_orders(**context):
    region = context["ti"].xcom_pull(task_ids="wait_for_extract", key="return_value")["region"]
    rows = order_utils.fetch_orders(region)
    return order_utils.enrich_accounts(rows)


with DAG(
    dag_id="orders_etl",
    schedule_interval="@hourly",
    start_date=days_ago(2),
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
