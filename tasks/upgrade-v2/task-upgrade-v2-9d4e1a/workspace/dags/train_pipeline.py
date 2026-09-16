"""Daily model retraining — launches a cluster, runs the training step."""

from datetime import datetime

from airflow import DAG

from libs.operators.cluster import LaunchClusterOperator

with DAG(
    "train_pipeline",
    schedule_interval="0 2 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    primary = LaunchClusterOperator(
        task_id="primary",
        cluster_name="train-primary",
        region="us-east-1",
        instance_type="m6i.4xlarge",
    )
    backup = LaunchClusterOperator(
        task_id="backup",
        cluster_name="train-backup",
        region="us-west-2",
        instance_type="m6i.2xlarge",
    )

    primary >> backup
