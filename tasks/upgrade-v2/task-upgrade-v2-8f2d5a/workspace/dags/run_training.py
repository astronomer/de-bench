"""Daily model retrain — schedules a KubernetesPodOperator on the GPU pool."""

from datetime import datetime

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator


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
    )
