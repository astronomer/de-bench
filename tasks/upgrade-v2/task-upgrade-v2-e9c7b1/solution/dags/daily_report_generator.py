"""Daily Report Generator — financial, user-metrics, and compliance reports."""

from datetime import datetime, timedelta

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG


def generate_financial_report(**context):
    """Build the daily financial summary."""
    logical_date = context["logical_date"]
    ds = context["ds"]
    tomorrow = (logical_date + timedelta(days=1)).strftime("%Y-%m-%d")
    yesterday = (logical_date - timedelta(days=1)).strftime("%Y-%m-%d")
    print(f"Financial Report for logical date: {logical_date}")
    print(f"  Report date (ds): {ds}")
    print(f"  Prior day: {yesterday}")
    print(f"  Next day: {tomorrow}")


def generate_user_metrics(**context):
    """Build daily user engagement metrics."""
    logical_date = context["logical_date"]
    interval_start = context["data_interval_start"]
    interval_end = context["data_interval_end"]
    print(f"User Metrics Report:")
    print(f"  Current run: {logical_date}")
    print(f"  Interval start: {interval_start}")
    print(f"  Interval end: {interval_end}")


def generate_compliance_check(**context):
    """Run compliance checks for regulatory reporting."""
    ds = context["ds"]
    logical_date = context["logical_date"]
    yesterday = (logical_date - timedelta(days=1)).strftime("%Y-%m-%d")
    ds_nodash = context["ds_nodash"]
    print(f"Compliance Check for {ds}")
    print(f"  Checking transactions from {yesterday} to {ds}")
    print(f"  Report file: compliance_report_{ds_nodash}.csv")


with DAG(
    dag_id="daily_report_generator",
    default_args={"owner": "analytics"},
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["reporting", "daily"],
):
    financial = PythonOperator(
        task_id="financial_report",
        python_callable=generate_financial_report,
    )

    users = PythonOperator(
        task_id="user_metrics",
        python_callable=generate_user_metrics,
    )

    compliance = PythonOperator(
        task_id="compliance_check",
        python_callable=generate_compliance_check,
    )

    financial >> users >> compliance
