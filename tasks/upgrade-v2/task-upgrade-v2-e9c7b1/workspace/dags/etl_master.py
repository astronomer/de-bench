"""Master ETL Orchestrator — coordinates daily ETL across domains."""

from airflow import DAG
from airflow.operators.subdag import SubDagOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import imp

config_module = imp.load_source(
    "pipeline_config", "/usr/local/airflow/include/legacy_config_loader.py"
)


def create_domain_subdag(parent_dag_id, child_dag_id, tables, args):
    with DAG(
        dag_id=f"{parent_dag_id}.{child_dag_id}",
        default_args=args,
        schedule_interval="@daily",
    ) as subdag:
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
                use_dill=True,
            )
            extract >> transform
    return subdag


default_args = {"start_date": days_ago(7), "owner": "data-platform"}

with DAG(
    dag_id="etl_master",
    default_args=default_args,
    schedule_interval="0 2 * * *",
    catchup=True,
    fail_stop=True,
) as dag:

    start = BashOperator(
        task_id="start",
        bash_command="echo 'Starting daily ETL at {{ ds }}'",
    )

    process_users = SubDagOperator(
        task_id="process_users",
        subdag=create_domain_subdag(
            "etl_master",
            "process_users",
            ["user_profiles", "user_preferences", "user_sessions"],
            default_args,
        ),
    )

    process_transactions = SubDagOperator(
        task_id="process_transactions",
        subdag=create_domain_subdag(
            "etl_master",
            "process_transactions",
            ["payments", "refunds", "subscriptions", "invoices"],
            default_args,
        ),
    )

    load_warehouse = BashOperator(
        task_id="load_warehouse",
        bash_command="echo 'Loading domains to warehouse for {{ ds }}'",
    )

    notify = PythonOperator(
        task_id="notify_completion",
        python_callable=lambda **ctx: print(f"ETL complete for {ctx['ds']}"),
    )

    start >> [process_users, process_transactions] >> load_warehouse >> notify
