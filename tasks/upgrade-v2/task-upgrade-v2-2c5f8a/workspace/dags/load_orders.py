"""Daily order load — copies S3 orders into the warehouse via Snowflake."""

from datetime import datetime

from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator


with DAG(
    "load_orders",
    schedule="0 3 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    copy_into_staging = SnowflakeOperator(
        task_id="copy_into_staging",
        sql="COPY INTO staging.orders FROM @s3_stage/orders/{{ ds }}/",
        snowflake_conn_id="snowflake_default",
    )
    merge_into_warehouse = SnowflakeOperator(
        task_id="merge_into_warehouse",
        sql="MERGE INTO warehouse.orders t USING staging.orders s ON t.id = s.id",
        snowflake_conn_id="snowflake_default",
    )
    copy_into_staging >> merge_into_warehouse
