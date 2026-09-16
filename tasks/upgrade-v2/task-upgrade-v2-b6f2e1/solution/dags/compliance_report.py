"""Daily compliance report — export account snapshots to the partner drop."""

from datetime import datetime

from airflow.sdk import DAG

from libs.operators.data_export import DataExportOperator

with DAG(
    "compliance_report",
    schedule="0 7 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    accounts = DataExportOperator(
        task_id="accounts",
        sql="SELECT account_id, balance FROM warehouse.accounts WHERE updated_at >= '{{ ds }}'",
        destination_path="s3://partner-drop/{{ ds }}/accounts.csv",
        partner_id="acme",
    )
    transactions = DataExportOperator(
        task_id="transactions",
        sql="SELECT txn_id, account_id, amount FROM warehouse.transactions WHERE booked_at >= '{{ ds }}'",
        destination_path="s3://partner-drop/{{ ds }}/transactions.csv",
        partner_id="acme",
    )

    accounts >> transactions
