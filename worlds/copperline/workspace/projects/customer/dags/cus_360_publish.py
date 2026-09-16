"""Publish the 360: the warehouse view, and the service-desk sync.

The one asset-scheduled DAG in this project. `marts.customer_360` is the
slowest model in the estate and its finishing time moves by minutes from one
morning to the next, so waiting on the table rather than on a clock is the
difference between publishing at 06:58 and publishing half a table at 06:45.

An asset-triggered run carries no meaningful interval, so nothing here reads
`{{ ds }}`. The day being published is the day the table holds.

**The published view hashes contact detail.** The unhashed values stay in the
warehouse, covered by `contracts/privacy.md`. Nothing in this path writes an
unhashed contact anywhere a person outside the data team can read it.

**The service-desk sync is a full replace every night.** There is no delta
path and nobody has needed one; support read the whole table anyway.

Owned by customer. Support notice within minutes when this does not run.
"""

from __future__ import annotations

import pendulum
from airflow.sdk import dag, task

from include.lib.notify import notify
from include.lib.pipeline import Reject, lake_task
from projects.customer.lib import publish
from projects.customer.lib.assets import CUSTOMER_360

DEFAULT_ARGS = {
    "owner": "customer",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
    "on_failure_callback": notify("customer", "the 360 publish failed",
                                  runbook="ops/runbooks/alerting.md"),
}

LAKE_CONFIG = {"reject_table": "ops.rejects",
               "retry": {"attempts": 2, "delay_seconds": 60}}


@dag(
    dag_id="cus_360_publish",
    schedule=[CUSTOMER_360],
    start_date=pendulum.datetime(2026, 1, 5, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params={"lake_config": LAKE_CONFIG},
    tags=["customer", "360", "publish"],
    doc_md=__doc__,
)
def cus_360_publish():

    @task
    def published_day() -> str:
        """The day the table holds, read from the table.

        A plain task, so the value is an ordinary XCom the steps below can
        read whatever the run's interval turns out to be.
        """
        return publish.latest_360_day()

    @lake_task
    def refresh_view(day: str) -> str:
        """Rebuild the published view, contact detail hashed."""
        return publish.refresh_view(day)

    @lake_task
    def sync_service_desk(day: str) -> int:
        """Write the whole table to the service desk, replacing yesterday's.

        A full replace: there is no delta path and support read the whole
        table anyway. The write is atomic, so the desk sees yesterday's file
        or today's and never half of one.
        """
        return publish.sync_service_desk(day)

    @lake_task
    def check_published() -> dict[str, int]:
        """The view has every account and no unhashed contact in it."""
        leaks = publish.unhashed_columns()
        if leaks:
            raise Reject("unhashed contact detail in the published view: "
                         + ", ".join(leaks),
                         rows=[{"column": column} for column in leaks])
        return publish.published_counts()

    day = published_day()
    refresh_view(day) >> [sync_service_desk(day), check_published()]


cus_360_publish()
