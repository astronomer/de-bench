"""The acquired account book, reloaded from the file the integration kept.

Northwave's book landed in the warehouse once, by hand, six weeks after the
acquisition closed. It has not refreshed since and it never will: Northwave's
own systems were retired and the file under `landing/nwv/` is the whole
record. This DAG re-lands that file every morning so that a warehouse rebuilt
from scratch has the book in it, and so that anybody who edits the file finds
out the same day.

**It replaces the table rather than adding to it.** The book carries no load
clock — there was only ever one load — so there is no partition column to
scope a replace to, and the house CSV loader's replace needs one. That is why
the load is written out here instead.

**Nothing about this book is current.** It is the book as it stood at the
acquisition, and a figure taken from it is a figure from then. Anything
joining it to today's data goes through the crosswalk, and any model reading
it states the era.

Owned by customer, with the integration team, who no longer exist.
"""

from __future__ import annotations

import pendulum
from airflow.sdk import dag, task

from include.lib.notify import notify
from include.lib.pipeline import Reject, lake_task
from projects.customer.lib import acquired

DEFAULT_ARGS = {
    "owner": "customer",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
    "on_failure_callback": notify("customer", "acquired book reload failed",
                                  runbook="ops/runbooks/alerting.md"),
}

LAKE_CONFIG = {"reject_table": "ops.rejects",
               "retry": {"attempts": 2, "delay_seconds": 60}}


@dag(
    dag_id="cus_northwave_accounts_intake",
    schedule="30 2 * * *",
    start_date=pendulum.datetime(2026, 1, 5, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params={"lake_config": LAKE_CONFIG},
    tags=["customer", "northwave", "intake"],
    doc_md=__doc__,
)
def cus_northwave_accounts_intake():

    @lake_task
    def load_book() -> int:
        """Replace `raw.nwv_accounts` from the archived file."""
        return acquired.reload_book()

    @lake_task
    def check_counts() -> dict[str, int]:
        """One thousand four hundred accounts, of which one thousand three
        hundred and eighty are open.

        The counts are fixed, because the book is fixed. A move in either
        means somebody edited the file, which is allowed and is worth
        knowing about the same day rather than a quarter later.
        """
        counts = acquired.book_counts()
        if counts["accounts"] != acquired.EXPECTED_ACCOUNTS:
            raise Reject(
                f"the acquired book holds {counts['accounts']} accounts, "
                f"not {acquired.EXPECTED_ACCOUNTS}",
                rows=[counts],
            )
        return counts

    @lake_task
    def check_ids() -> list[dict]:
        """Every id is an `NWA-` id and no id appears twice.

        The two books share no key space on purpose. An id here that looks
        like a Copperline id would join to a live account and put an
        acquired customer's history on it.
        """
        return acquired.id_problems()

    @task
    def frozen_since() -> str:
        """When this book stopped moving, for whoever reads the run.

        The `nwv` reporting schema beside it froze on the same clock; both
        dates are in `docs/runbooks/northwave-integration.md` §NWI-2.
        """
        return acquired.frozen_note()

    loaded = load_book()
    loaded >> [check_counts(), check_ids(), frozen_since()]


cus_northwave_accounts_intake()
