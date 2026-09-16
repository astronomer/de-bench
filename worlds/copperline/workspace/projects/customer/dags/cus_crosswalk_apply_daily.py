"""Resolve the day's order references to current account ids.

The commerce replatform re-keyed every trade account from `CUST####` to
`C-######`. The store integration was not told and kept writing old-format
ids for two more weeks, so `raw.orders.customer_ref` carries both schemes and
carries forty-five old-format refs dated after the cutover. Those forty-five
are real orders from real accounts.

**The crosswalk applies by id format, not by order date.** A ref that looks
like `CUST####` goes through `raw.customer_id_map` whatever date it carries;
a `C-######` ref is current and is never mapped. Switching on the order date
gives an answer that is complete and wrong by those forty-five.
`docs/runbooks/customer-id-migration.md` §CID-2.

**Sixty legacy ids have no map row**, and they roll up under their own id
rather than being dropped or given an invented one. §CID-3.

This runs at 03:30 so that the dimension at 04:00 has resolved references to
build on. `{{ ds }}` is the day this run fires and the orders it resolves are
the previous day's, so the date is written out as
`{{ macros.ds_add(ds, -1) }}` everywhere it is used.

Owned by customer.
"""

from __future__ import annotations

import pendulum
from airflow.sdk import dag, task

from include.lib.notify import notify
from include.lib.pipeline import lake_task
from projects.customer.lib import identity

DEFAULT_ARGS = {
    "owner": "customer",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
    "on_failure_callback": notify("customer", "crosswalk apply failed",
                                  runbook="ops/runbooks/alerting.md"),
}

LAKE_CONFIG = {"reject_table": "ops.rejects",
               "retry": {"attempts": 2, "delay_seconds": 60}}


@dag(
    dag_id="cus_crosswalk_apply_daily",
    schedule="30 3 * * *",
    start_date=pendulum.datetime(2026, 1, 5, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params={"lake_config": LAKE_CONFIG},
    tags=["customer", "identity", "crosswalk"],
    doc_md=__doc__,
)
def cus_crosswalk_apply_daily():

    @lake_task
    def resolve_refs(day: str) -> int:
        """Resolve the day's references and write
        `ops.customer_ref_resolved`."""
        return identity.resolve_refs(day)

    @lake_task
    def orphans(day: str) -> list[dict]:
        """References that went through the map and found nothing.

        Expected: sixty legacy ids were never carried across. Listed so that
        a NEW one is visible, because a legacy id surfacing now has nowhere
        to go and the map is a one-off load that nothing maintains.
        """
        return identity.orphan_refs(day)

    @lake_task
    def late_old_format(day: str) -> list[dict]:
        """Old-format refs on orders dated after the cutover.

        There are forty-five of them, through the two weeks the store
        integration kept writing the old scheme. They are real orders and
        they resolve correctly by format; this counts them so that the number
        stays the number.
        """
        return identity.post_cutover_old_refs(day)

    @lake_task
    def check_coverage(day: str) -> dict[str, int]:
        """Every order with a reference got a customer id."""
        return identity.resolution_counts(day)

    @task
    def publish_resolved(day: str) -> str:
        """Write the day's resolved references out for the dimension."""
        return identity.publish_resolved(day)

    day = "{{ macros.ds_add(ds, -1) }}"
    resolve_refs(day) >> [orphans(day), late_old_format(day),
                          check_coverage(day)] >> publish_resolved(day)


cus_crosswalk_apply_daily()
