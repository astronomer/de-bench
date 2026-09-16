"""Hourly invoice pipeline: GCS export + KubernetesPodOperator postprocess."""

from datetime import datetime

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import (
    BigQueryToGCSOperator,
)


with DAG(
    "process_invoices",
    schedule="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    export = BigQueryToGCSOperator(
        task_id="export_invoices",
        source_project_dataset_table="warehouse-prod.finance.invoices",
        destination_cloud_storage_uris=["gs://exports/invoices/{{ ts_nodash }}/*.parquet"],
        export_format="PARQUET",
        gcp_conn_id="google_cloud_default",
    )
    postprocess = KubernetesPodOperator(
        task_id="postprocess_invoices",
        name="postprocess-{{ ts_nodash }}",
        namespace="finance",
        image="ghcr.io/example/invoice-postprocess:latest",
        cmds=["/bin/bash", "-c"],
        arguments=["python -m invoice_postprocess --window={{ ts_nodash }}"],
    )
    export >> postprocess
