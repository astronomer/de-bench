import pendulum
from airflow import DAG

from libs.operators.data_service_op import DataServiceOperator, make_summary_task


def summarise(**ctx):
    return f"finished at {ctx['data_interval_start']}"


with DAG(
    dag_id="daily_pipeline",
    start_date=pendulum.yesterday("UTC"),
    schedule="@daily",
    catchup=False,
) as dag:
    fetch = DataServiceOperator(task_id="fetch", endpoint="/orders")
    report = make_summary_task("report", summarise)
    fetch >> report
