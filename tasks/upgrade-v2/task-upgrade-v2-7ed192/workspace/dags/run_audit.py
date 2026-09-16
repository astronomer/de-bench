"""Daily audit DAG — checks upstream task health and counts prior runs."""

from contextlib import closing
from datetime import timedelta

from airflow import DAG, models, settings
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


def audit_upstream(**context):
    """Count upstream task instances that did not succeed."""
    task = context["task"]
    dag = context["dag"]
    execution_date = context["execution_date"]
    upstream_ids = list(task.upstream_task_ids)
    if not upstream_ids:
        return

    with closing(settings.Session()) as session:
        failing = (
            session.query(models.TaskInstance)
            .filter(
                models.TaskInstance.dag_id == dag.dag_id,
                models.TaskInstance.task_id.in_(upstream_ids),
                models.TaskInstance.execution_date == execution_date,
                models.TaskInstance.state != "success",
            )
            .count()
        )
    if failing:
        raise ValueError(f"{failing} upstream tasks did not succeed")


def count_prior_success(**context):
    """Count prior successful runs for this DAG."""
    dag = context["dag"]
    execution_date = context["execution_date"]
    with closing(settings.Session()) as session:
        total = (
            session.query(models.DagRun)
            .filter(
                models.DagRun.dag_id == dag.dag_id,
                models.DagRun.state == "success",
                models.DagRun.execution_date < execution_date,
            )
            .count()
        )
    context["ti"].xcom_push(key="prior_success_count", value=total)


dag = DAG(
    "run_audit",
    schedule_interval="0 7 * * *",
    start_date=days_ago(1),
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    catchup=False,
)


audit = PythonOperator(
    task_id="audit_upstream",
    python_callable=audit_upstream,
    provide_context=True,
    dag=dag,
)

count = PythonOperator(
    task_id="count_prior_success",
    python_callable=count_prior_success,
    provide_context=True,
    dag=dag,
)

audit >> count
