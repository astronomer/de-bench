"""Counts successful runs of the current DAG and alerts on regressions."""

from datetime import datetime, timedelta

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG


def alert_on_regression(**context):
    ti = context["ti"]
    dag_id = context["dag"].dag_id
    n = ti.get_dr_count(dag_id=dag_id, states=["success"])
    if n < 5:
        raise ValueError(f"only {n} successful prior runs — alert")


dag = DAG(
    "run_count_alert",
    schedule="0 3 * * *",
    start_date=datetime(2026, 1, 1),
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    catchup=False,
)

PythonOperator(
    task_id="alert",
    python_callable=alert_on_regression,
    dag=dag,
)
