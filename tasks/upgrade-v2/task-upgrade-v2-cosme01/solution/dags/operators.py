"""Standalone DAG that runs a single dbt model via DbtRunLocalOperator."""

from datetime import datetime

from airflow import DAG
from cosmos.config import ProfileConfig
from cosmos.operators.local import DbtRunLocalOperator
from cosmos.profiles.snowflake.user_pass import SnowflakeUserPasswordProfileMapping

_profile_config = ProfileConfig(
    profile_name="analytics",
    target_name="prod",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_default",
        profile_args={"schema": "analytics"},
    ),
)

with DAG(
    dag_id="run_single_model",
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:
    DbtRunLocalOperator(
        task_id="run_orders",
        project_dir="/usr/local/airflow/dags/dbt/analytics",
        profile_config=_profile_config,
        select=["orders"],
    )
