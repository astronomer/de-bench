"""Revenue models: build the dbt warehouse models on a daily cadence."""

from datetime import datetime

from cosmos import DbtDag, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import LoadMode
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

revenue_models = DbtDag(
    dag_id="revenue_models",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    project_config=ProjectConfig(
        dbt_project_path="/usr/local/airflow/dags/dbt/revenue",
    ),
    profile_config=ProfileConfig(
        profile_name="revenue",
        target_name="prod",
        profile_mapping=SnowflakeUserPasswordProfileMapping(
            conn_id="snowflake_default",
            profile_args={
                "schema": "analytics",
                "database": "prod",
                "warehouse": "transforming",
            },
        ),
    ),
    render_config=RenderConfig(
        load_mode=LoadMode.CUSTOM,
    ),
    operator_args={
        "vars": {"lookback_days": 90},
    },
)
