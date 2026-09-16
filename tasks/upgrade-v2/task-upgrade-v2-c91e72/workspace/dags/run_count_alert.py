"""Counts successful runs of the current DAG and alerts on regressions."""

from datetime import timedelta

from airflow import DAG, models, settings
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.session import provide_session


@provide_session
def count_recent_success(session=None, **context):
    dag_id = context["dag"].dag_id
    cutoff = context["execution_date"]
    return (
        session.query(models.DagRun)
        .filter(
            models.DagRun.dag_id == dag_id,
            models.DagRun.state == "success",
            models.DagRun.execution_date < cutoff,
        )
        .count()
    )


def alert_on_regression(**context):
    n = count_recent_success(**context)
    if n < 5:
        raise ValueError(f"only {n} successful prior runs — alert")


dag = DAG(
    "run_count_alert",
    schedule_interval="0 3 * * *",
    start_date=days_ago(1),
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    catchup=False,
)

PythonOperator(
    task_id="alert",
    python_callable=alert_on_regression,
    provide_context=True,
    dag=dag,
)
