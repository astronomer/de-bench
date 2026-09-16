"""Master ETL Orchestrator — coordinates daily ETL across domains."""

from datetime import datetime
import importlib.util

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, TaskGroup

_spec = importlib.util.spec_from_file_location(
    "pipeline_config", "/usr/local/airflow/include/legacy_config_loader.py"
)
config_module = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(config_module)


default_args = {"owner": "data-platform"}

with DAG(
    dag_id="etl_master",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="0 2 * * *",
    catchup=True,
    fail_fast=True,
) as dag:

    start = BashOperator(
        task_id="start",
        bash_command="echo 'Starting daily ETL at {{ ds }}'",
    )

    domains = []
    for domain_name, tables in (
        ("process_users", ["user_profiles", "user_preferences", "user_sessions"]),
        ("process_transactions", ["payments", "refunds", "subscriptions", "invoices"]),
    ):
        with TaskGroup(group_id=domain_name) as group:
            for table in tables:
                extract = BashOperator(
                    task_id=f"extract_{table}",
                    bash_command=(
                        f"echo 'Extracting {table} for "
                        "{{ data_interval_start.strftime(\"%Y-%m-%d\") }} "
                        "to {{ data_interval_end.strftime(\"%Y-%m-%d\") }}'"
                    ),
                )
                transform = PythonOperator(
                    task_id=f"transform_{table}",
                    python_callable=lambda t=table: print(f"Transforming {t}"),
                )
                extract >> transform
        domains.append(group)

    load_warehouse = BashOperator(
        task_id="load_warehouse",
        bash_command="echo 'Loading domains to warehouse for {{ ds }}'",
    )

    notify = PythonOperator(
        task_id="notify_completion",
        python_callable=lambda **ctx: print(f"ETL complete for {ctx['ds']}"),
    )

    start >> domains >> load_warehouse >> notify
