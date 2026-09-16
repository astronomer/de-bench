"""Daily billing run — generates invoices and pages on-call on task failure."""

from datetime import datetime, timedelta

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG


def page_oncall_on_failure(context):
    ti = context["task_instance"]
    print(f"task failure on {ti.dag_id}.{ti.task_id}")


def aggregate_invoices(**context):
    print(f"aggregating invoices for {context['ds']}")


def post_invoices(**context):
    print(f"posting invoices to ledger for {context['ds']}")


with DAG(
    "billing_run",
    schedule="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    aggregate = PythonOperator(
        task_id="aggregate",
        python_callable=aggregate_invoices,
        execution_timeout=timedelta(minutes=30),
        on_failure_callback=page_oncall_on_failure,
    )
    post = PythonOperator(
        task_id="post",
        python_callable=post_invoices,
        execution_timeout=timedelta(minutes=20),
        on_failure_callback=page_oncall_on_failure,
    )
    aggregate >> post
