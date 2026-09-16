"""Hourly invoice copy from S3 staging into the warehouse."""

from datetime import datetime

from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator


with DAG(
    "copy_invoices",
    schedule="@hourly",
    start_date=datetime(2024, 6, 1),
    catchup=False,
) as dag:
    copy = SnowflakeOperator(
        task_id="copy_into_invoices",
        sql="COPY INTO staging.invoices FROM @s3_stage/invoices/{{ ts_nodash }}/",
        snowflake_conn_id="snowflake_default",
    )
