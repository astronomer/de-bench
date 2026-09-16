"""Downstream DAG triggered by the analytics dataset alias emitted
from etl.py. Cosmos 1.7 switched dataset emission to DatasetAlias
on AF >= 2.10."""

from datetime import datetime

from airflow import DAG
from airflow.datasets import DatasetAlias
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="reporting",
    schedule=[
        DatasetAlias("snowflake://snowflake_default/warehouse/analytics.orders"),
    ],
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:
    BashOperator(
        task_id="build_report",
        bash_command="echo build_report",
    )
