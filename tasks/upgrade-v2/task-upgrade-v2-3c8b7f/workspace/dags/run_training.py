"""Daily training run — schedules the model retrain pod."""

from datetime import datetime

from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.sdk import DAG


with DAG(
    "run_training",
    schedule="0 4 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    train = KubernetesPodOperator(
        task_id="train",
        name="train-{{ ds_nodash }}",
        namespace="ml-platform",
        image="ghcr.io/example/trainer:latest",
        cmds=["/bin/bash", "-c"],
        arguments=["python -m trainer.run --date={{ ds }}"],
        is_delete_operator_pod=True,
    )
