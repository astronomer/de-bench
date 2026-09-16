"""Daily payment reconciliation — compares the settlement ledger against
captured charges and records unmatched transactions for the finance team."""

import pendulum
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, BaseOperator


class ReconciliationOperator(BaseOperator):
    """Pulls captured charges from the payments database, flags the ones
    with no matching settlement row, and returns the count of mismatches."""

    def __init__(self, *, conn_id: str, ledger_db: str, **kwargs):
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.ledger_db = ledger_db

    def execute(self, context):
        hook = PostgresHook(
            postgres_conn_id=self.conn_id,
            database=self.ledger_db,
        )
        rows = hook.get_records(
            "SELECT charge_id, amount_cents, settled "
            "FROM payments.charges "
            "WHERE captured_at::date = CURRENT_DATE AND settled IS FALSE"
        )
        for charge_id, amount_cents, _ in rows:
            print(f"unsettled charge: {charge_id} ({amount_cents} cents)")
        return len(rows)


def archive_mismatches():
    hook = PostgresHook(
        postgres_conn_id="payments_postgres",
        database="reconciliation",
    )
    hook.run(
        "INSERT INTO reconciliation.daily_gaps (run_at, gap_count) "
        "VALUES (NOW(), (SELECT COUNT(*) FROM payments.charges "
        "WHERE captured_at::date = CURRENT_DATE AND settled IS FALSE))"
    )


with DAG(
    "payment_reconciliation",
    schedule="@daily",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
) as dag:
    reconcile = ReconciliationOperator(
        task_id="reconcile",
        conn_id="payments_postgres",
        ledger_db="reconciliation",
    )
    archive = PythonOperator(
        task_id="archive",
        python_callable=archive_mismatches,
    )
    reconcile >> archive
