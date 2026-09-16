"""Warehouse ETL — loads event data to Snowflake, aggregates in BigQuery."""

from datetime import timedelta

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.dates import days_ago

dag = DAG(
    "warehouse_etl",
    schedule_interval="0 6 * * *",
    start_date=days_ago(1),
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    catchup=False,
)

load_snowflake = SnowflakeOperator(
    task_id="load_snowflake",
    sql="COPY INTO raw.events FROM @gcs_stage/{{ ds }}/",
    snowflake_conn_id="snowflake_default",
    dag=dag,
)

aggregate_bq = BigQueryExecuteQueryOperator(
    task_id="aggregate_bq",
    bql="""
        SELECT DATE('{{ execution_date }}') AS report_date,
               COUNT(*) AS event_count
        FROM `analytics.events.raw_events`
        WHERE DATE(created_at) = '{{ ds }}'
    """,
    use_legacy_sql=False,
    destination_dataset_table="analytics.events.daily_summary${{ ds_nodash }}",
    dag=dag,
)

load_snowflake >> aggregate_bq
