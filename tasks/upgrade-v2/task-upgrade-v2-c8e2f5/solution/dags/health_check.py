"""Daily marketing-platform health check."""

from datetime import datetime

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG


def check_landing_zone(**context):
    print(f"checking landing zone for {context['ds']}")


def check_warehouse(**context):
    print(f"checking warehouse for {context['ds']}")


with DAG(
    "marketing_health_check",
    schedule="0 5 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    landing = PythonOperator(
        task_id="landing",
        python_callable=check_landing_zone,
    )
    warehouse = PythonOperator(
        task_id="warehouse",
        python_callable=check_warehouse,
    )

    landing >> warehouse
