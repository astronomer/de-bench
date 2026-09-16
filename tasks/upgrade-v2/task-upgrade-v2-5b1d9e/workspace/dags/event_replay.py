"""Daily event replay — re-emits the prior day's events to a downstream queue."""

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def replay_window(**context):
    ds = context["ds"]
    print(f"replaying events for {ds}")


with DAG(
    "event_replay",
    schedule="0 4 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    replay = PythonOperator(
        task_id="replay",
        python_callable=replay_window,
    )
