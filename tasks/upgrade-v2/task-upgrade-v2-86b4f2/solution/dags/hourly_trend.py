"""Hourly trend job — emits per-run telemetry."""

from datetime import datetime, timedelta

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, TaskGroup


def emit_start(**context):
    ti = context["ti"]
    key = f"{ti.dag_id}__{ti.task_id}__{ti.run_id}"
    print(f"op={ti.task.task_type} key={key} at={ti.logical_date}")


def emit_end(**context):
    ti = context["ti"]
    print(f"op={ti.task.task_type} finished at={ti.logical_date}")


with DAG(
    "hourly_trend",
    schedule="0 * * * *",
    start_date=datetime(2026, 1, 1),
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    catchup=False,
) as dag:
    with TaskGroup(group_id="emit") as emit:
        PythonOperator(task_id="start", python_callable=emit_start)
        PythonOperator(task_id="end", python_callable=emit_end)
