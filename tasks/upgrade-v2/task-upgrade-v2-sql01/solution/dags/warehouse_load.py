"""Daily warehouse load: stage rows in postgres, mirror into snowflake."""

from datetime import datetime

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

with DAG(
    "warehouse_load",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    stage = SQLExecuteQueryOperator(
        task_id="stage_rows",
        conn_id="warehouse_pg",
        sql="INSERT INTO staging.orders SELECT * FROM raw.orders WHERE updated_at > now() - interval '1 day';",
    )
    mirror = SQLExecuteQueryOperator(
        task_id="mirror_to_snowflake",
        conn_id="warehouse_sf",
        sql="INSERT INTO warehouse.orders SELECT * FROM staging.orders WHERE _ts > current_timestamp() - interval '1 day';",
    )
    stage >> mirror
