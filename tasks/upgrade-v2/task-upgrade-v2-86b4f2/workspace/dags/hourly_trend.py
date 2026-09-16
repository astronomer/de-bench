"""Hourly trend job — emits per-run telemetry."""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup


def emit_start(**context):
    ti = context["ti"]
    # Log the operator class + current execution marker so ops can
    # correlate pages back to the task instance.
    print(f"op={ti.operator} key={ti.task_instance_key_str} at={ti.execution_date}")


def emit_end(**context):
    ti = context["ti"]
    print(f"op={ti.operator} finished at={ti.execution_date}")


with DAG(
    "hourly_trend",
    schedule_interval="0 * * * *",
    start_date=datetime(2026, 1, 1),
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    catchup=False,
) as dag:
    with TaskGroup(group_id="emit") as emit:
        PythonOperator(task_id="start", python_callable=emit_start)
        PythonOperator(task_id="end", python_callable=emit_end)
