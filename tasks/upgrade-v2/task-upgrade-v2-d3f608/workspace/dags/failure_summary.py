"""Builds a per-task failure summary for the previous run."""

from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


def summarise_failures(**context):
    dag_run = context["dag_run"]
    failed_tis = dag_run.get_task_instances(state=["failed", "upstream_failed"])
    failed_ids = sorted({ti.task_id for ti in failed_tis})
    if not failed_ids:
        return "all green"
    return f"failed task ids: {', '.join(failed_ids)}"


def emit_summary(**context):
    summary = summarise_failures(**context)
    context["ti"].xcom_push(key="failure_summary", value=summary)


dag = DAG(
    "failure_summary",
    schedule_interval="0 */4 * * *",
    start_date=days_ago(1),
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    catchup=False,
)

PythonOperator(
    task_id="emit_summary",
    python_callable=emit_summary,
    provide_context=True,
    dag=dag,
)
