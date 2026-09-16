"""Cleanup pipeline with a custom housekeeping operator."""

from datetime import datetime, timedelta

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG


class HousekeepingOperator(PythonOperator):
    """Runs a cleanup callable with retry / retention bookkeeping."""

    def __init__(self, retention_days=30, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.retention_days = retention_days

    def execute(self, context):
        ti = context["ti"]
        self.log.info(
            "cleanup op=%s retention=%s", ti.task.task_type, self.retention_days
        )
        return super().execute(context)


def _cleanup(**ctx):
    return f"cleaned up to {ctx['logical_date']}"


dag = DAG(
    "cleanup_pipeline",
    schedule="0 3 * * 0",
    start_date=datetime(2026, 1, 1),
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    catchup=False,
)

cleanup = HousekeepingOperator(
    task_id="cleanup",
    python_callable=_cleanup,
    retention_days=14,
    dag=dag,
)
