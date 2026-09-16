"""Nightly warehouse pipeline: build the analytics models, then run the
data-quality checks against them. Both steps shell out to the project's
dbt models via standalone dbt operators rather than a generated task
group, so the run and the test stay as two explicit, separately
retryable tasks."""

from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from cosmos import ProfileConfig
from cosmos.operators.local import DbtRunLocalOperator, DbtTestLocalOperator
from cosmos.profiles import PostgresUserPasswordProfileMapping

DBT_PROJECT_DIR = "/usr/local/airflow/dags/dbt/analytics"

profile_config = ProfileConfig(
    profile_name="analytics",
    target_name="prod",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="warehouse_default",
        profile_args={
            "schema": "analytics",
            "threads": 4,
        },
    ),
)

with DAG(
    dag_id="warehouse_quality",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    start = EmptyOperator(task_id="start")

    build_models = DbtRunLocalOperator(
        task_id="build_models",
        project_dir=DBT_PROJECT_DIR,
        profile_config=profile_config,
        select="tag:nightly",
    )

    quality_checks = DbtTestLocalOperator(
        task_id="quality_checks",
        project_dir=DBT_PROJECT_DIR,
        profile_config=profile_config,
        select="tag:nightly",
    )

    finish = EmptyOperator(task_id="finish")

    start >> build_models >> quality_checks >> finish
