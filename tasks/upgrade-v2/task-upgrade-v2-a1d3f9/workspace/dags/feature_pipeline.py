"""Daily feature build — runs a Spark job on Kubernetes and validates output."""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

with DAG(
    "feature_pipeline",
    schedule="0 4 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=10)},
) as dag:
    build_features = KubernetesPodOperator(
        task_id="build_features",
        name="features-builder",
        namespace="ml",
        image="company/features:1.4.2",
        cmds=["spark-submit", "/opt/jobs/build.py"],
        is_delete_operator_pod=True,
        get_logs=True,
    )

    validate = KubernetesPodOperator(
        task_id="validate",
        name="features-validator",
        namespace="ml",
        image="company/validator:1.4.2",
        cmds=["python", "/opt/jobs/validate.py"],
        is_delete_operator_pod=True,
        get_logs=True,
    )

    build_features >> validate
