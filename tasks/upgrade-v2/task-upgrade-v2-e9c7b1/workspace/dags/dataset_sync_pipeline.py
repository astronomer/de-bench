"""Dataset Sync Pipeline — producer/consumer DAGs over Airflow datasets."""

from airflow.decorators import dag, task
from airflow.datasets import Dataset, DatasetAlias
import pendulum

raw_transactions = Dataset("s3://warehouse-bucket/raw/transactions")
cleaned_transactions = Dataset("s3://warehouse-bucket/cleaned/transactions")
dynamic_output = DatasetAlias("transaction-partitions")


@dag(
    dag_id="transaction_producer",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule="0 3 * * *",
    catchup=False,
    tags=["etl", "transactions", "producer"],
    default_args={"owner": "data"},
)
def transaction_producer():
    @task(outlets=[raw_transactions, dynamic_output])
    def extract_and_clean(**context):
        ds = context["ds"]
        print(f"Extracting transactions for {ds}")
        return {"records_processed": 125000, "date": ds}

    @task(outlets=[cleaned_transactions])
    def validate_and_publish(stats):
        print(f"Validated {stats['records_processed']} records for {stats['date']}")
        return stats

    raw = extract_and_clean()
    validate_and_publish(raw)


@dag(
    dag_id="transaction_consumer",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=[cleaned_transactions],
    catchup=False,
    tags=["analytics", "transactions", "consumer"],
    default_args={"owner": "analytics"},
)
def transaction_consumer():
    @task
    def aggregate_daily_stats(**context):
        ds = context["ds"]
        print(f"Aggregating transaction stats for {ds}")
        return {"total_volume": 15_000_000.00, "transaction_count": 125000}

    @task
    def update_dashboard(stats):
        print(f"Dashboard updated: volume=${stats['total_volume']:,.2f}")

    stats = aggregate_daily_stats()
    update_dashboard(stats)


transaction_producer()
transaction_consumer()
