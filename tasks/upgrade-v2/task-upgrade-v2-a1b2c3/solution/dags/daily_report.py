import pendulum
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator


def build_report(**ctx):
    run_date = ctx["logical_date"]
    yday = (run_date - pendulum.duration(days=1)).strftime("%Y-%m-%d")
    return f"{run_date.strftime('%Y-%m-%d')}: captured snapshot for {yday}"


with DAG(
    dag_id="daily_report",
    start_date=pendulum.yesterday("UTC"),
    schedule="@daily",
    catchup=False,
) as dag:
    PythonOperator(task_id="report", python_callable=build_report)
