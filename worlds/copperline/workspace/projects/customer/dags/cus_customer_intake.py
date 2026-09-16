"""Copperline's trade account book, from Halyard into `raw.customers`.

Halyard exports over a listing API rather than as a file, and the API pages:
fifty rows a page, a `next_page_token` on every page but the last, and
`total` always null. `JsonApiToWarehouseOperator` follows the token to the
end, which is the only way to know the answer is complete — a first page
looks exactly like a whole book.

The endpoint is a change feed, so the run asks for accounts changed since the
last successful run and merges them onto the book. The window comes from
`include.lib.watermark` rather than from `{{ ds }}`, because a re-run of an
old day has to ask for that day's window and not for today's.

**Deletes do not flow through this sync and never have.** The warehouse holds
accounts Halyard no longer has, `contracts/customer-360.md` records it as a
known gap, and this DAG does not quietly invent a fix for it.

Owned by customer. Everything with a customer on it is downstream of this.
"""

from __future__ import annotations

import pendulum
from airflow.sdk import dag, task

from include.lib import watermark
from include.lib.loaders import JsonApiToWarehouseOperator
from include.lib.notify import notify
from include.lib.pipeline import Reject, lake_task
from projects.customer.lib import book

#: The stream name the watermark is filed under.
STREAM = "halyard_accounts"

#: The inbox the change feed lands in before it is merged onto the book.
INBOX = "ops.crm_account_inbox"

DEFAULT_ARGS = {
    "owner": "customer",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
    "on_failure_callback": notify("customer", "account book intake failed",
                                  runbook="ops/runbooks/alerting.md"),
}

LAKE_CONFIG = {"reject_table": "ops.rejects",
               "retry": {"attempts": 2, "delay_seconds": 60}}


@dag(
    dag_id="cus_customer_intake",
    schedule="0 2 * * *",
    start_date=pendulum.datetime(2026, 1, 5, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params={"lake_config": LAKE_CONFIG},
    tags=["customer", "crm", "intake"],
    doc_md=__doc__,
)
def cus_customer_intake():

    @task
    def since_mark() -> str:
        """The low-water mark this run asks Halyard for.

        A plain task, so the value is an ordinary XCom the loader below can
        template into its query string.
        """
        return watermark.since(STREAM).isoformat()

    page_accounts = JsonApiToWarehouseOperator(
        task_id="page_accounts",
        endpoint="crm/accounts",
        params={"updated_since": "{{ ti.xcom_pull(task_ids='since_mark') }}"},
        table=INBOX,
        mode="append",
        retries=3,
    )

    @lake_task
    def merge_accounts() -> int:
        """Fold the inbox onto the book, newest version of each account wins.

        The inbox is a change feed and keeps every version it has seen, which
        is what makes a re-run harmless: merging the same changes twice
        leaves one row per account.
        """
        return book.merge_inbox(INBOX)

    @lake_task
    def check_book() -> dict[str, int]:
        """One row per account, every id in the current scheme.

        A `CUST####` id reaching the book means the re-key has come undone
        somewhere upstream, and every join downstream would silently miss
        that account.
        """
        problems = book.book_problems()
        if problems:
            raise Reject(f"{len(problems)} account(s) look wrong", rows=problems)
        return book.book_counts()

    @task
    def mark_loaded() -> str:
        """Move the watermark. Last, and only once the book is merged."""
        return watermark.advance(STREAM).isoformat()

    merged = merge_accounts()
    since_mark() >> page_accounts >> merged
    merged >> [mark_loaded(), check_book()]


cus_customer_intake()
