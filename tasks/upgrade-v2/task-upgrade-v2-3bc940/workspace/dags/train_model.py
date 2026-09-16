"""Weekly model-training pipeline that runs on Kubernetes with GPU nodes."""

from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.dates import days_ago

dag = DAG(
    "train_model",
    schedule_interval="0 2 * * 1",
    start_date=days_ago(7),
    default_args={"retries": 1, "retry_delay": timedelta(minutes=15)},
    fail_stop=True,
    catchup=False,
)

prepare = BashOperator(
    task_id="prepare",
    bash_command="echo 'staging training data'",
    dag=dag,
)

train = KubernetesPodOperator(
    task_id="train",
    name="train-pod",
    namespace="ml",
    image="team/trainer:1.0",
    cmds=["python", "train.py"],
    arguments=["--epochs=50"],
    resources={
        "request_memory": "4Gi",
        "request_cpu": "2",
        "limit_memory": "8Gi",
        "limit_gpu": "1",
    },
    is_delete_operator_pod=True,
    get_logs=True,
    dag=dag,
)

evaluate = KubernetesPodOperator(
    task_id="evaluate",
    name="eval-pod",
    namespace="ml",
    image="team/evaluator:1.0",
    cmds=["python", "evaluate.py"],
    resources={"request_memory": "2Gi", "request_cpu": "1"},
    is_delete_operator_pod=True,
    dag=dag,
)

prepare >> train >> evaluate
