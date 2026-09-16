"""Daily export from BigQuery into GCS for downstream consumers."""

from datetime import datetime

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import (
    BigQueryToGCSOperator,
)


with DAG(
    "export_to_gcs",
    schedule="0 5 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    materialise = BigQueryInsertJobOperator(
        task_id="materialise_orders",
        configuration={
            "query": {
                "query": "SELECT * FROM analytics.orders WHERE date = '{{ ds }}'",
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": "warehouse-prod",
                    "datasetId": "analytics",
                    "tableId": "orders_daily_{{ ds_nodash }}",
                },
            },
        },
        gcp_conn_id="google_cloud_default",
    )
    export = BigQueryToGCSOperator(
        task_id="export_orders",
        source_project_dataset_table="warehouse-prod.analytics.orders_daily_{{ ds_nodash }}",
        destination_cloud_storage_uris=["gs://exports/orders/{{ ds }}/*.parquet"],
        export_format="PARQUET",
        gcp_conn_id="google_cloud_default",
    )
    materialise >> export
