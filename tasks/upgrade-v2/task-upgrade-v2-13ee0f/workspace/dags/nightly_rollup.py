"""Nightly rollup — aggregates the day's orders and stamps a summary file."""

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def summarize(**context):
    print(f"summarizing orders for {context['ds']}")


with DAG(
    "nightly_rollup",
    schedule="30 2 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    stage = BashOperator(
        task_id="stage_partition",
        bash_command="echo staging partition for $DS",
    )
    summary = PythonOperator(
        task_id="write_summary",
        python_callable=summarize,
    )

    stage >> summary
