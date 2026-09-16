"""Daily audit DAG — checks upstream task health and counts prior runs."""

from datetime import datetime, timedelta

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG


def audit_upstream(**context):
    """Count upstream task instances that did not succeed."""
    ti = context["ti"]
    task = context["task"]
    dag = context["dag"]
    upstream_ids = list(task.upstream_task_ids)
    if not upstream_ids:
        return

    task_states = ti.get_task_states(
        dag_id=dag.dag_id,
        task_ids=upstream_ids,
        run_ids=[context["run_id"]],
    )
    failing = sum(1 for state in task_states.values() if str(state) != "success")
    if failing:
        raise ValueError(f"{failing} upstream tasks did not succeed")


def count_prior_success(**context):
    """Count prior successful runs for this DAG."""
    ti = context["ti"]
    dag = context["dag"]
    total = ti.get_dr_count(dag_id=dag.dag_id, states=["success"])
    ti.xcom_push(key="prior_success_count", value=total)


dag = DAG(
    "run_audit",
    schedule="0 7 * * *",
    start_date=datetime(2026, 1, 1),
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    catchup=False,
)


audit = PythonOperator(
    task_id="audit_upstream",
    python_callable=audit_upstream,
    dag=dag,
)

count = PythonOperator(
    task_id="count_prior_success",
    python_callable=count_prior_success,
    dag=dag,
)

audit >> count
