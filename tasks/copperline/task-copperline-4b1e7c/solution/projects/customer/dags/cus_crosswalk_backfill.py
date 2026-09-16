"""Fill the order-reference attribution for the history behind the nightly.

`cus_crosswalk_apply_daily` owns one day at a time and owns only the days since
it started running, so `ops.customer_ref_resolved` has nothing in it for the
window the FY2024 volume review argues about. Support has been answering "when
did this account first buy from us" out of `raw.orders` by hand, and getting a
different answer each time.

This is a one-off over one window — 2024-09-01 to 2024-12-31, which the re-key
sits in the middle of. The dates live in `projects/customer/lib/identity.py`
rather than in a `Param`, so that triggering the job is the whole of asking for
it.

**The crosswalk applies by the format of the reference, not by the order's
date.** A `CUST####` reference goes through `raw.customer_id_map` whatever date
it carries; a `C-######` reference is current and is never mapped. The store
integration was not told about the re-key and kept writing the old scheme for
two weeks after it, so forty-five orders in this window are dated after the
cutover and still carry an old-format reference. They are real orders from real
accounts. `docs/runbooks/customer-id-migration.md` §CID-2.

**Sixty legacy ids have no current account behind them.** They churned before
the migration and were never carried across, and they keep the id they traded
under rather than being dropped or given an invented one. §CID-3.

Every order carrying a reference lands, the rows flagged as test and the ones
the OMS has since deleted included. This is attribution, not revenue: support
reads it back to a customer and it has to account for every reference that was
ever written.

Owned by customer.
"""

from __future__ import annotations

import pendulum
from airflow.sdk import dag

from include.lib.notify import notify
from include.lib.pipeline import lake_task
from projects.customer.lib import identity

DEFAULT_ARGS = {
    "owner": "customer",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
    "on_failure_callback": notify("customer", "crosswalk backfill failed",
                                  runbook="ops/runbooks/alerting.md"),
}

LAKE_CONFIG = {"reject_table": "ops.rejects",
               "retry": {"attempts": 2, "delay_seconds": 60}}


@dag(
    dag_id="cus_crosswalk_backfill",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 5, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params={"lake_config": LAKE_CONFIG},
    tags=["customer", "identity", "crosswalk", "backfill"],
    doc_md=__doc__,
)
def cus_crosswalk_backfill():

    @lake_task
    def backfill() -> dict:
        """Resolve every reference in the window and write the days it owns.

        One delete-insert per day, so the job may be run twice and a day
        outside the window is never touched.
        """
        return identity.backfill_refs(identity.BACKFILL_FROM,
                                      identity.BACKFILL_TO)

    @lake_task
    def check_coverage() -> dict:
        """Every reference landed, once, under an account.

        Fails the run rather than reporting: support reads this table back to
        a customer, so a hole in it is worse than a failed run.
        """
        return identity.backfill_coverage(identity.BACKFILL_FROM,
                                          identity.BACKFILL_TO)

    backfill() >> check_coverage()


cus_crosswalk_backfill()
