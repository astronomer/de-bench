"""Wait for an upstream DAG, then run a downstream BashOperator."""

from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from airflow.sdk import DAG

with DAG(
    "wait_then_run",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:
    wait = ExternalTaskSensor(
        task_id="wait_for_upstream",
        external_dag_id="upstream_etl",
        external_task_id="finalize",
    )
    run = BashOperator(
        task_id="run_downstream",
        bash_command="echo 'go'",
    )
    wait >> run
