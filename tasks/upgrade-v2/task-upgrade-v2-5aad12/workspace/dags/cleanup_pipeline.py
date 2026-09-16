"""Cleanup pipeline with a custom housekeeping operator."""

from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.decorators import apply_defaults


class HousekeepingOperator(PythonOperator):
    """Runs a cleanup callable with retry / retention bookkeeping."""

    @apply_defaults
    def __init__(self, retention_days=30, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.retention_days = retention_days

    def execute(self, context):
        ti = context["ti"]
        self.log.info(
            "cleanup op=%s retention=%s", ti.operator, self.retention_days
        )
        return super().execute(context)


def _cleanup(**ctx):
    return f"cleaned up to {ctx['execution_date']}"


dag = DAG(
    "cleanup_pipeline",
    schedule_interval="0 3 * * 0",
    start_date=days_ago(7),
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    catchup=False,
)

cleanup = HousekeepingOperator(
    task_id="cleanup",
    python_callable=_cleanup,
    retention_days=14,
    dag=dag,
)
