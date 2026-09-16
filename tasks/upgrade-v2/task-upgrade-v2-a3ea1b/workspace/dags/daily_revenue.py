"""Daily revenue rollup into the analytics warehouse."""

from datetime import datetime

from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

with DAG(
    "daily_revenue",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["revenue", "analytics"],
) as dag:
    truncate = SnowflakeOperator(
        task_id="truncate_daily_stage",
        snowflake_conn_id="analytics_wh",
        sql="TRUNCATE TABLE analytics.daily_revenue_stage;",
    )
    load = SnowflakeOperator(
        task_id="load_daily_revenue",
        snowflake_conn_id="analytics_wh",
        sql=(
            "INSERT INTO analytics.daily_revenue_stage "
            "SELECT order_date, SUM(amount) AS revenue "
            "FROM raw.orders "
            "WHERE order_date = '{{ ds }}' "
            "GROUP BY order_date;"
        ),
    )
    publish = SnowflakeOperator(
        task_id="publish_daily_revenue",
        snowflake_conn_id="analytics_wh",
        sql=(
            "MERGE INTO analytics.daily_revenue tgt "
            "USING analytics.daily_revenue_stage src "
            "ON tgt.order_date = src.order_date "
            "WHEN MATCHED THEN UPDATE SET tgt.revenue = src.revenue "
            "WHEN NOT MATCHED THEN INSERT (order_date, revenue) "
            "VALUES (src.order_date, src.revenue);"
        ),
    )
    truncate >> load >> publish
