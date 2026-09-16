"""Daily event replay — re-emits the prior day's events to a downstream queue."""

from datetime import datetime

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG


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
