"""Halyard support tickets, landed hourly into `raw.support_tickets`.

The service desk exports over the same paginated listing API as the account
book: fifty rows a page, a token on every page but the last, `total` always
null. The loader follows the token to the end. A first-page answer would land
fifty tickets an hour and look exactly like a quiet hour.

**A ticket carries a party, and a party has a book.** `party_ref` with
`party_source` — `oms` for a Copperline account, `nwv` for an acquired one —
because Halyard's own desk has one kind of party and Copperline has two. The
pair is what joins a ticket to an account across the two books, and a join on
`party_ref` alone puts an acquired customer's tickets on whichever
Copperline account shares the string.

The window comes from the watermark, not from `{{ ds }}`: on an hourly
schedule `ds` is the same string for twenty-four consecutive runs.

Owned by customer. `cus_support_sla_daily` and the 360 both read this.
"""

from __future__ import annotations

import pendulum
from airflow.sdk import dag, task

from include.lib import watermark
from include.lib.loaders import JsonApiToWarehouseOperator
from include.lib.notify import notify
from include.lib.pipeline import Reject, lake_task
from projects.customer.lib import support

#: The stream name the watermark is filed under.
STREAM = "halyard_tickets"

#: The inbox the change feed lands in before it is folded onto the table.
INBOX = "ops.support_ticket_inbox"

DEFAULT_ARGS = {
    "owner": "customer",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
    "on_failure_callback": notify("customer", "support ticket intake failed",
                                  runbook="ops/runbooks/alerting.md"),
}

LAKE_CONFIG = {"reject_table": "ops.rejects",
               "retry": {"attempts": 2, "delay_seconds": 60}}


@dag(
    dag_id="cus_support_tickets_intake",
    schedule="15 * * * *",
    start_date=pendulum.datetime(2026, 1, 5, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params={"lake_config": LAKE_CONFIG},
    tags=["customer", "support", "intake"],
    doc_md=__doc__,
)
def cus_support_tickets_intake():

    @task
    def since_mark() -> str:
        """The low-water mark this run asks the desk for."""
        return watermark.since(STREAM).isoformat()

    page_tickets = JsonApiToWarehouseOperator(
        task_id="page_tickets",
        endpoint="support/tickets",
        params={"updated_since": "{{ ti.xcom_pull(task_ids='since_mark') }}"},
        table=INBOX,
        mode="append",
        retries=3,
    )

    @lake_task
    def merge_tickets() -> int:
        """Fold the inbox onto the table, newest version of each ticket wins.

        A ticket changes as it is worked — it is assigned, answered, closed,
        reopened — so the feed sends it several times and the table holds the
        latest state of each one.
        """
        return support.merge_inbox(INBOX)

    @lake_task
    def check_parties() -> dict[str, int]:
        """Every ticket names a party and a book, and the pair resolves.

        A ticket with a party the books do not have is a ticket that will
        never reach an account, and it disappears from the 360 without
        failing anything.
        """
        problems = support.party_problems()
        if problems:
            raise Reject(f"{len(problems)} ticket(s) name a party we do not "
                         "have", rows=problems)
        return support.ticket_counts()

    @task
    def mark_loaded() -> str:
        """Move the watermark. Last, and only once the tickets are merged."""
        return watermark.advance(STREAM).isoformat()

    merged = merge_tickets()
    since_mark() >> page_tickets >> merged
    merged >> [mark_loaded(), check_parties()]


cus_support_tickets_intake()
