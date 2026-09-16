"""Two calendar-driven DAGs that share a tz-aware `start_date`."""

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

with DAG(
    "morning_run",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule="0 7 * * *",
    catchup=False,
) as morning:
    BashOperator(task_id="brew", bash_command="echo 'morning'")

with DAG(
    "evening_run",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule="0 19 * * *",
    catchup=False,
) as evening:
    BashOperator(task_id="brew", bash_command="echo 'evening'")
