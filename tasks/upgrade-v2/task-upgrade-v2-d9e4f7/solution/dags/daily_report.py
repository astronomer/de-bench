"""Daily report build — waits for three upstream signals, then builds."""

from datetime import datetime, timedelta

from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from airflow.sdk import DAG


def build_report(**context):
    print(f"building daily report for {context['ds']}")


with DAG(
    "daily_report",
    schedule="30 7 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    wait_extract = ExternalTaskSensor(
        task_id="wait_extract",
        external_dag_id="warehouse_extract",
        external_task_id="finalize",
        deferrable=True,
        poke_interval=60,
        execution_timeout=timedelta(minutes=45),
    )
    wait_partner_drop = S3KeySensor(
        task_id="wait_partner_drop",
        bucket_name="partner-drops",
        bucket_key="acme/{{ ds }}/orders.parquet",
        deferrable=True,
        poke_interval=120,
        execution_timeout=timedelta(minutes=60),
    )
    wait_pricing_api = HttpSensor(
        task_id="wait_pricing_api",
        endpoint="/v2/pricing/{{ ds }}",
        http_conn_id="pricing_default",
        deferrable=True,
        poke_interval=30,
        execution_timeout=timedelta(minutes=20),
    )
    build = PythonOperator(
        task_id="build",
        python_callable=build_report,
    )

    [wait_extract, wait_partner_drop, wait_pricing_api] >> build
