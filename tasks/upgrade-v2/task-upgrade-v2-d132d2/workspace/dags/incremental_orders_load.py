"""Incrementally loads new order rows into the analytics warehouse.

Each run pulls only the orders created since the last time this pipeline
ran successfully, so the load window's lower bound is the logical date of
the most recent successful run (or a fixed backfill epoch on the very
first run).
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import DagRun
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.session import provide_session
from airflow.utils.state import DagRunState

BACKFILL_EPOCH = datetime(2024, 1, 1)


@provide_session
def _last_successful_logical_date(dag_id, before, session=None):
    last = (
        session.query(DagRun)
        .filter(
            DagRun.dag_id == dag_id,
            DagRun.state == DagRunState.SUCCESS,
            DagRun.execution_date < before,
        )
        .order_by(DagRun.execution_date.desc())
        .first()
    )
    if last is None:
        return BACKFILL_EPOCH
    return last.execution_date


def compute_window(**context):
    dag_run = context["dag_run"]
    lower = _last_successful_logical_date(dag_run.dag_id, dag_run.execution_date)
    upper = dag_run.execution_date
    context["ti"].xcom_push(key="window_start", value=lower.isoformat())
    context["ti"].xcom_push(key="window_end", value=upper.isoformat())
    return f"loading orders in ({lower.isoformat()}, {upper.isoformat()}]"


def load_orders(**context):
    ti = context["ti"]
    start = ti.xcom_pull(task_ids="compute_window", key="window_start")
    end = ti.xcom_pull(task_ids="compute_window", key="window_end")
    return f"loaded orders created in ({start}, {end}]"


dag = DAG(
    "incremental_orders_load",
    schedule_interval="0 6 * * *",
    start_date=days_ago(2),
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    catchup=False,
)

compute = PythonOperator(
    task_id="compute_window",
    python_callable=compute_window,
    provide_context=True,
    dag=dag,
)

load = PythonOperator(
    task_id="load_orders",
    python_callable=load_orders,
    provide_context=True,
    dag=dag,
)

compute >> load
