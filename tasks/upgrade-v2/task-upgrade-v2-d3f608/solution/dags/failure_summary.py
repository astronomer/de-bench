"""Builds a per-task failure summary for the previous run."""

from datetime import datetime, timedelta

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG


def summarise_failures(**context):
    ti = context["ti"]
    dag = context["dag"]
    states = ti.get_task_states(
        dag_id=dag.dag_id,
        task_ids=list(dag.task_ids),
        run_ids=[context["run_id"]],
    )
    failed_ids = sorted(
        {tid for tid, state in states.items() if str(state) in ("failed", "upstream_failed")}
    )
    if not failed_ids:
        return "all green"
    return f"failed task ids: {', '.join(failed_ids)}"


def emit_summary(**context):
    summary = summarise_failures(**context)
    context["ti"].xcom_push(key="failure_summary", value=summary)


dag = DAG(
    "failure_summary",
    schedule="0 */4 * * *",
    start_date=datetime(2026, 1, 1),
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    catchup=False,
)

PythonOperator(
    task_id="emit_summary",
    python_callable=emit_summary,
    dag=dag,
)
