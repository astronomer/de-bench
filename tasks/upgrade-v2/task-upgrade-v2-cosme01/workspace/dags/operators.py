"""Standalone DAG that runs a single dbt model via DbtRunLocalOperator."""

from datetime import datetime

from airflow import DAG
from cosmos.providers.dbt.core.operators.local import DbtRunLocalOperator

with DAG(
    dag_id="run_single_model",
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:
    DbtRunLocalOperator(
        task_id="run_orders",
        project_dir="/usr/local/airflow/dags/dbt/analytics",
        profile_args={"schema": "analytics"},
        select=["orders"],
        conn_id="snowflake_default",
    )
