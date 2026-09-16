"""Audits the current run's task states and fails if any did not succeed."""

from datetime import datetime

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG


def audit_run_states(**context):
    ti = context["ti"]
    dag = context["dag"]
    # Task SDK IPC, not an ORM session: get_task_states is available on
    # AF 3.0+ and is the supported way to read state from inside a task.
    states = ti.get_task_states(
        dag_id=dag.dag_id,
        task_ids=list(dag.task_ids),
        run_ids=[context["run_id"]],
    )
    failing = {
        tid: state
        for tid, state in states.items()
        if tid != ti.task_id and str(state) != "success"
    }
    if failing:
        raise ValueError(f"tasks did not succeed: {failing}")


dag = DAG(
    "run_audit",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
)

PythonOperator(
    task_id="audit_run",
    python_callable=audit_run_states,
    dag=dag,
)
