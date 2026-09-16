from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="orders_ingest",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:
    extract = BashOperator(
        task_id="extract",
        bash_command="echo extracting orders for {{ execution_date }}",
    )
    load = BashOperator(
        task_id="load",
        bash_command="echo loading orders into the warehouse",
    )

    extract >> load
