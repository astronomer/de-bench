"""Weekly model-training pipeline that runs on Kubernetes with GPU nodes."""

from datetime import datetime, timedelta

from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG
from kubernetes.client import models as k8s

dag = DAG(
    "train_model",
    schedule="0 2 * * 1",
    start_date=datetime(2026, 1, 1),
    default_args={"retries": 1, "retry_delay": timedelta(minutes=15)},
    fail_fast=True,
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
    container_resources=k8s.V1ResourceRequirements(
        requests={"memory": "4Gi", "cpu": "2"},
        limits={"memory": "8Gi", "nvidia.com/gpu": "1"},
    ),
    on_finish_action="delete_pod",
    get_logs=True,
    dag=dag,
)

evaluate = KubernetesPodOperator(
    task_id="evaluate",
    name="eval-pod",
    namespace="ml",
    image="team/evaluator:1.0",
    cmds=["python", "evaluate.py"],
    container_resources=k8s.V1ResourceRequirements(
        requests={"memory": "2Gi", "cpu": "1"},
    ),
    on_finish_action="delete_pod",
    dag=dag,
)

prepare >> train >> evaluate
