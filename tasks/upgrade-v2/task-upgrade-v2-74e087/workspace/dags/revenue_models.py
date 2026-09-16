"""Revenue models: build the dbt warehouse models on a daily cadence."""

from datetime import datetime

from cosmos.providers.dbt.dag import DbtDag

revenue_models = DbtDag(
    dag_id="revenue_models",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    dbt_root_path="/usr/local/airflow/dags/dbt",
    dbt_project_name="revenue",
    conn_id="snowflake_default",
    profile_args={
        "schema": "analytics",
        "database": "prod",
        "warehouse": "transforming",
    },
    dbt_args={
        "vars": {"lookback_days": 90},
    },
)
