"""Marketing pipeline: ingest raw events, then transform them with dbt."""

from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from cosmos import ProfileConfig, ProjectConfig
from cosmos.airflow.task_group import DbtTaskGroup
from cosmos.profiles import GoogleCloudServiceAccountFileProfileMapping

with DAG(
    dag_id="marketing_pipeline",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    ingest = EmptyOperator(task_id="ingest_events")

    transform = DbtTaskGroup(
        group_id="transform",
        project_config=ProjectConfig(
            dbt_project_path="/usr/local/airflow/dags/dbt/marketing",
        ),
        profile_config=ProfileConfig(
            profile_name="marketing",
            target_name="prod",
            profile_mapping=GoogleCloudServiceAccountFileProfileMapping(
                conn_id="bigquery_default",
                profile_args={
                    "schema": "marketing",
                },
            ),
        ),
        operator_args={
            "vars": {"event_window_days": 30},
        },
    )

    publish = EmptyOperator(task_id="publish_dashboards")

    ingest >> transform >> publish
