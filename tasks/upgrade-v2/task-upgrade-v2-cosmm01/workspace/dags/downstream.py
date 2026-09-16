"""Downstream DAG triggered by the analytics dataset emitted from etl.py."""

from datetime import datetime

from airflow import DAG, Dataset
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="reporting",
    schedule=[
        Dataset("snowflake://snowflake_default/warehouse/analytics.orders"),
    ],
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:
    BashOperator(
        task_id="build_report",
        bash_command="echo build_report",
    )
