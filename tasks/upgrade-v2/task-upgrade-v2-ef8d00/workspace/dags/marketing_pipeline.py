"""Marketing pipeline: ingest raw events, then transform them with dbt."""

from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from cosmos.task_group import DbtTaskGroup

with DAG(
    dag_id="marketing_pipeline",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    ingest = EmptyOperator(task_id="ingest_events")

    transform = DbtTaskGroup(
        group_id="transform",
        dbt_project_dir="/usr/local/airflow/dags/dbt/marketing",
        conn_id="bigquery_default",
        dbt_args={
            "schema": "marketing",
            "vars": {"event_window_days": 30},
        },
    )

    publish = EmptyOperator(task_id="publish_dashboards")

    ingest >> transform >> publish
