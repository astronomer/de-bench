"""Daily billing run — generates invoices and pages on-call if it misses SLA."""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


def page_oncall_on_sla_miss(dag, task_list, blocking_task_list, slas, blocking_tis):
    print(f"SLA miss on {dag.dag_id}: tasks {[t.task_id for t in task_list]}")


def aggregate_invoices(**context):
    print(f"aggregating invoices for {context['ds']}")


def post_invoices(**context):
    print(f"posting invoices to ledger for {context['ds']}")


with DAG(
    "billing_run",
    schedule_interval="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    sla_miss_callback=page_oncall_on_sla_miss,
) as dag:
    aggregate = PythonOperator(
        task_id="aggregate",
        python_callable=aggregate_invoices,
        sla=timedelta(minutes=30),
    )
    post = PythonOperator(
        task_id="post",
        python_callable=post_invoices,
        sla=timedelta(minutes=20),
    )
    aggregate >> post
