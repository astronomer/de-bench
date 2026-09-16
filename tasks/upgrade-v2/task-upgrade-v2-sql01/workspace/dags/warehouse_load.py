"""Daily warehouse load: stage rows in postgres, mirror into snowflake."""

from datetime import datetime

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

with DAG(
    "warehouse_load",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    stage = PostgresOperator(
        task_id="stage_rows",
        postgres_conn_id="warehouse_pg",
        sql="INSERT INTO staging.orders SELECT * FROM raw.orders WHERE updated_at > now() - interval '1 day';",
    )
    mirror = SnowflakeOperator(
        task_id="mirror_to_snowflake",
        snowflake_conn_id="warehouse_sf",
        sql="INSERT INTO warehouse.orders SELECT * FROM staging.orders WHERE _ts > current_timestamp() - interval '1 day';",
    )
    stage >> mirror
