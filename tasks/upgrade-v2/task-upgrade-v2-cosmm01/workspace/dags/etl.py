"""Renders the analytics dbt project as a daily DAG."""

from datetime import datetime

from cosmos import DbtDag, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.profiles.snowflake.user_pass import SnowflakeUserPasswordProfileMapping

dag = DbtDag(
    dag_id="analytics",
    project_config=ProjectConfig(
        dbt_project_path="/usr/local/airflow/dags/dbt/analytics",
    ),
    profile_config=ProfileConfig(
        profile_name="analytics",
        target_name="prod",
        profile_mapping=SnowflakeUserPasswordProfileMapping(
            conn_id="snowflake_default",
            profile_args={"schema": "analytics"},
        ),
    ),
    execution_config=ExecutionConfig(
        execution_mode="local",
    ),
    render_config=RenderConfig(
        emit_datasets=True,
    ),
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
)
