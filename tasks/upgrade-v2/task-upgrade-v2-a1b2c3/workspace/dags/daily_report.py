from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago


def build_report(**ctx):
    run_date = ctx["execution_date"]
    yday = ctx["yesterday_ds"]
    return f"{run_date.strftime('%Y-%m-%d')}: captured snapshot for {yday}"


with DAG(
    dag_id="daily_report",
    start_date=days_ago(1),
    schedule="@daily",
    catchup=False,
) as dag:
    PythonOperator(task_id="report", python_callable=build_report)
