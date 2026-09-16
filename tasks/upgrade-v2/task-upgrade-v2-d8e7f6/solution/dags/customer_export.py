"""Daily customer export — runs at 03:00 UTC and reports completion."""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def report_done(**context):
    ti = context["task_instance"]
    print(f"finished {ti.dag_id}__{ti.task_id}__{ti.run_id}")


with DAG(
    "customer_export",
    schedule="0 3 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
) as dag:
    extract = BashOperator(
        task_id="extract",
        bash_command="echo extracting",
    )
    report = PythonOperator(
        task_id="report",
        python_callable=report_done,
    )

    extract >> report
