"""Renders the analytics dbt project as a daily DAG."""

from datetime import datetime

from cosmos.providers.dbt.dag import DbtDag

dag = DbtDag(
    dag_id="analytics",
    dbt_root_path="/usr/local/airflow/dags/dbt",
    dbt_project_name="analytics",
    conn_id="snowflake_default",
    profile_args={
        "schema": "analytics",
        "database": "warehouse",
    },
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
)
