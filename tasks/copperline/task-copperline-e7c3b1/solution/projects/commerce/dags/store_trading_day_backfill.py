"""Fill the store trading-day pack for FY2025 periods 9 and 10.

Store ops read this beside a store's own end-of-day report, so the pack is
keyed on the day the store counted — `raw.pos_sales_header.business_date`,
the trading day the registers close at 23:05 store-local.

**There is no UTC stamp before 2025-11-03.** `event_time_utc` is NULL on every
transaction the registers sent before the standardization, so a pack keyed on
it holds nothing for period 9. Rebuilding one out of the local stamp and the
store's zone is worse: the pre-cutover local stamp is the 23:05 close, and
23:05 in an Americas zone is the next day in UTC, so the whole night of two
thirds of the estate lands on the day after the one it traded.

**From 2025-11-03 the registers send per-transaction times**, and an evening
sale's UTC date is not the trading day either. One rule holds over both
periods and it is the store's own date.

Voided transactions are counted and take no money; returns are counted and
carry the amount the till wrote. A store-day with no transactions behind it is
not a zero-sales day and does not get a row —
`docs/runbooks/pos-ingestion.md` POS-1.

One-off, triggered by hand. Owned by commerce.
"""

from __future__ import annotations

import pendulum
from airflow.sdk import dag

from include.lib.notify import notify
from include.lib.pipeline import lake_task
from projects.commerce.lib import trading_day

DEFAULT_ARGS = {
    "owner": "commerce",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
    "on_failure_callback": notify("commerce", "store trading-day backfill failed",
                                  runbook="ops/runbooks/alerting.md"),
}

LAKE_CONFIG = {"reject_table": "ops.rejects",
               "retry": {"attempts": 2, "delay_seconds": 60}}


@dag(
    dag_id="store_trading_day_backfill",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 5, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params={"lake_config": LAKE_CONFIG},
    tags=["commerce", "store", "pos", "backfill"],
    doc_md=__doc__,
)
def store_trading_day_backfill():

    @lake_task
    def build() -> dict:
        """Roll the till batches up to store and trading day, one day at a time.

        One delete-insert per trading day, so the job may be run twice and a
        day outside the window is never touched.
        """
        return trading_day.backfill(trading_day.BACKFILL_FROM,
                                    trading_day.BACKFILL_TO)

    @lake_task
    def check_coverage() -> dict:
        """Every store-day that traded landed once, and nothing else landed.

        Fails the run rather than reporting: a district manager reads a store's
        night straight off this table.
        """
        return trading_day.coverage(trading_day.BACKFILL_FROM,
                                    trading_day.BACKFILL_TO)

    build() >> check_coverage()


store_trading_day_backfill()
