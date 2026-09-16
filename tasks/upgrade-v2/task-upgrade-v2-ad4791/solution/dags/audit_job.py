"""Audit the most recent run's task states and surface failures."""

from datetime import datetime

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG


def gather_prior_run(**context):
    ti = context["ti"]
    # get_previous_dagrun is available on AF 3.0+.
    prev = ti.get_previous_dagrun(state="success")
    return str(prev.logical_date) if prev else None


def audit_prior_run(**context):
    ti = context["ti"]
    dag = context["dag"]
    prev_run_id = context.get("prev_run_id")
    if not prev_run_id:
        return
    # get_task_states is available on AF 3.0+.
    states = ti.get_task_states(
        dag_id=dag.dag_id,
        task_ids=list(dag.task_ids),
        run_ids=[prev_run_id],
    )
    failing = {tid: s for tid, s in states.items() if str(s) != "success"}
    if failing:
        raise ValueError(f"prior run had failures: {failing}")


dag = DAG(
    "audit_job",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
)

PythonOperator(task_id="noop", python_callable=lambda: None, dag=dag)
PythonOperator(task_id="gather_prior_run", python_callable=gather_prior_run, dag=dag)
PythonOperator(task_id="audit_prior_run", python_callable=audit_prior_run, dag=dag)
