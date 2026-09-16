"""Hourly low-stock alerts — checks the inventory database for SKUs
below their reorder threshold and writes the daily restock summary."""

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def check_stock_levels():
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    hook = PostgresHook(
        postgres_conn_id="warehouse_postgres",
        schema="inventory",
    )
    rows = hook.get_records(
        "SELECT sku_id, on_hand, reorder_point "
        "FROM warehouse.stock "
        "WHERE on_hand < reorder_point"
    )
    for sku_id, on_hand, reorder_point in rows:
        print(f"low stock: {sku_id} ({on_hand}/{reorder_point})")
    return len(rows)


def write_restock_summary():
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    hook = PostgresHook(
        postgres_conn_id="warehouse_postgres",
        schema="inventory",
    )
    hook.run(
        "INSERT INTO warehouse.restock_log (run_at, low_count) "
        "VALUES (NOW(), (SELECT COUNT(*) FROM warehouse.stock WHERE on_hand < reorder_point))"
    )


with DAG(
    "inventory_alerts",
    schedule="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    check = PythonOperator(
        task_id="check",
        python_callable=check_stock_levels,
    )
    summary = PythonOperator(
        task_id="summary",
        python_callable=write_restock_summary,
    )
    check >> summary
