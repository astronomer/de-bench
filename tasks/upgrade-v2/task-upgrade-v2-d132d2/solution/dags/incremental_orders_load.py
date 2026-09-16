"""Incrementally loads new order rows into the analytics warehouse.

Each run pulls only the orders created since the last time this pipeline
ran successfully, so the load window's lower bound is the logical date of
the most recent successful run (or a fixed backfill epoch on the very
first run).
"""

from datetime import datetime, timedelta

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

BACKFILL_EPOCH = datetime(2024, 1, 1)


def compute_window(**context):
    ti = context["ti"]
    upper = context["logical_date"]
    previous = ti.get_previous_dagrun(state="success")
    lower = previous.logical_date if previous is not None else BACKFILL_EPOCH
    ti.xcom_push(key="window_start", value=lower.isoformat())
    ti.xcom_push(key="window_end", value=upper.isoformat())
    return f"loading orders in ({lower.isoformat()}, {upper.isoformat()}]"


def load_orders(**context):
    ti = context["ti"]
    start = ti.xcom_pull(task_ids="compute_window", key="window_start")
    end = ti.xcom_pull(task_ids="compute_window", key="window_end")
    return f"loaded orders created in ({start}, {end}]"


dag = DAG(
    "incremental_orders_load",
    schedule="0 6 * * *",
    start_date=datetime(2026, 1, 1),
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    catchup=False,
)

compute = PythonOperator(
    task_id="compute_window",
    python_callable=compute_window,
    dag=dag,
)

load = PythonOperator(
    task_id="load_orders",
    python_callable=load_orders,
    dag=dag,
)

compute >> load
