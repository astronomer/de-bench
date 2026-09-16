"""Daily Report Generator — financial, user-metrics, and compliance reports."""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


def generate_financial_report(**context):
    """Build the daily financial summary."""
    exec_date = context["execution_date"]
    ds = context["ds"]
    tomorrow = context["tomorrow_ds"]
    yesterday = context["yesterday_ds"]
    print(f"Financial Report for execution date: {exec_date}")
    print(f"  Report date (ds): {ds}")
    print(f"  Prior day: {yesterday}")
    print(f"  Next day: {tomorrow}")


def generate_user_metrics(**context):
    """Build daily user engagement metrics."""
    exec_date = context["execution_date"]
    prev = context["prev_execution_date"]
    next_exec = context["next_execution_date"]
    print(f"User Metrics Report:")
    print(f"  Current run: {exec_date}")
    print(f"  Previous run: {prev}")
    print(f"  Next run: {next_exec}")


def generate_compliance_check(**context):
    """Run compliance checks for regulatory reporting."""
    ds = context["ds"]
    yesterday = context["yesterday_ds"]
    ds_nodash = context["ds_nodash"]
    print(f"Compliance Check for {ds}")
    print(f"  Checking transactions from {yesterday} to {ds}")
    print(f"  Report file: compliance_report_{ds_nodash}.csv")


with DAG(
    dag_id="daily_report_generator",
    default_args={"start_date": days_ago(1), "owner": "analytics"},
    schedule_interval="@daily",
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
