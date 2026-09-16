"""Weekly cohort retention refresh in the analytics warehouse."""

from datetime import datetime

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

with DAG(
    "weekly_retention",
    schedule="@weekly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["retention", "analytics"],
) as dag:
    build_cohorts = SQLExecuteQueryOperator(
        task_id="build_cohorts",
        conn_id="analytics_wh",
        sql=(
            "CREATE OR REPLACE TABLE analytics.cohorts AS "
            "SELECT user_id, DATE_TRUNC('week', first_seen) AS cohort_week "
            "FROM raw.users;"
        ),
    )
    refresh = SQLExecuteQueryOperator(
        task_id="refresh_retention",
        conn_id="analytics_wh",
        sql=(
            "INSERT OVERWRITE INTO analytics.retention "
            "SELECT c.cohort_week, COUNT(DISTINCT e.user_id) AS active "
            "FROM analytics.cohorts c "
            "JOIN raw.events e ON e.user_id = c.user_id "
            "WHERE e.event_week = '{{ ds }}' "
            "GROUP BY c.cohort_week;"
        ),
    )
    build_cohorts >> refresh
