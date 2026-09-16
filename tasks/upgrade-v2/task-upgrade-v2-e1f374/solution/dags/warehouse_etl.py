"""Warehouse ETL — loads event data to Snowflake, aggregates in BigQuery."""

from datetime import datetime, timedelta

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.sdk import DAG

dag = DAG(
    "warehouse_etl",
    schedule="0 6 * * *",
    start_date=datetime(2026, 1, 1),
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    catchup=False,
)

load_snowflake = SQLExecuteQueryOperator(
    task_id="load_snowflake",
    sql="COPY INTO raw.events FROM @gcs_stage/{{ ds }}/",
    conn_id="snowflake_default",
    dag=dag,
)

aggregate_bq = BigQueryInsertJobOperator(
    task_id="aggregate_bq",
    configuration={
        "query": {
            "query": """
                SELECT DATE('{{ logical_date }}') AS report_date,
                       COUNT(*) AS event_count
                FROM `analytics.events.raw_events`
                WHERE DATE(created_at) = '{{ ds }}'
            """,
            "useLegacySql": False,
            "destinationTable": {
                "projectId": "analytics",
                "datasetId": "events",
                "tableId": "daily_summary${{ ds_nodash }}",
            },
        }
    },
    dag=dag,
)

load_snowflake >> aggregate_bq
