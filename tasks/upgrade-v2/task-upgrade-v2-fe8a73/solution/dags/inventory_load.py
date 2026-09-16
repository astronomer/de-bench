"""Hourly inventory load — pulls counts from Snowflake into a summary table."""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

with DAG(
    "inventory_load",
    schedule="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
) as dag:
    refresh_counts = SQLExecuteQueryOperator(
        task_id="refresh_counts",
        conn_id="snowflake_warehouse",
        sql="MERGE INTO ops.inventory_summary USING staging.events AS s ON ops.sku_id = s.sku_id WHEN MATCHED THEN UPDATE SET total = s.total;",
    )

    audit = SQLExecuteQueryOperator(
        task_id="audit",
        conn_id="snowflake_warehouse",
        sql="INSERT INTO ops.inventory_audit (run_at) VALUES (CURRENT_TIMESTAMP);",
    )

    refresh_counts >> audit
