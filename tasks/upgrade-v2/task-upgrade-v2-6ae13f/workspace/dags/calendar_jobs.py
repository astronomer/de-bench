"""Two calendar-driven DAGs that share a naive `start_date`."""

from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

with DAG(
    "morning_run",
    start_date=datetime(2026, 1, 1),
    schedule="0 7 * * *",
    catchup=False,
) as morning:
    BashOperator(task_id="brew", bash_command="echo 'morning'")

with DAG(
    "evening_run",
    start_date=datetime(2026, 1, 1),
    schedule="0 19 * * *",
    catchup=False,
) as evening:
    BashOperator(task_id="brew", bash_command="echo 'evening'")
