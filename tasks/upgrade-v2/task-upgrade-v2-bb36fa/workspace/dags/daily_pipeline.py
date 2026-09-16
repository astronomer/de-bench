from airflow import DAG
from airflow.utils.dates import days_ago

from libs.operators.data_service_op import DataServiceOperator, make_summary_task


def summarise(**ctx):
    return f"finished at {ctx['execution_date']}"


with DAG(
    dag_id="daily_pipeline",
    start_date=days_ago(1),
    schedule="@daily",
    catchup=False,
) as dag:
    fetch = DataServiceOperator(task_id="fetch", endpoint="/orders")
    report = make_summary_task("report", summarise)
    fetch >> report
