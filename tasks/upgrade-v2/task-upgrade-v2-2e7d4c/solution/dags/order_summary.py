"""Daily order-summary rollup — aggregates yesterday's orders into a daily totals table."""

from datetime import datetime

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG


def collect_order_count(**context):
    return 4271


def write_daily_totals(**context):
    ti = context["task_instance"]
    order_count = ti.xcom_pull(task_ids="collect", key="return_value")
    print(f"writing daily totals for {order_count} orders")


with DAG(
    "order_summary",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    collect = PythonOperator(
        task_id="collect",
        python_callable=collect_order_count,
    )
    write = PythonOperator(
        task_id="write",
        python_callable=write_daily_totals,
    )
    collect >> write
