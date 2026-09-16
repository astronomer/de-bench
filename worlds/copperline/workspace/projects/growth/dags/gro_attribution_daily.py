"""Touch-to-order attribution: which campaign got the credit, and how much.

Scheduled on the sessions asset, because attribution is a read of sessions
and there is nothing to attribute until they are rebuilt.

**Last non-direct touch, inside a thirty-day window.** That is the model the
ad platforms report against, so it is the only one our numbers and theirs can
be compared in. A session with no campaign is credited to `direct` rather
than dropped, which keeps the channels summing to the orders — an attribution
model whose parts do not add up to the whole is a model nobody can audit.

The order side comes from commerce's own fact, not from the event stream: a
`purchase` event says an order was placed, and `raw.orders` says what it was
worth. Revenue on an attributed order is `net_sales_cents` as
`docs/semantic-definitions.md` defines it, and this DAG does not invent a
second definition of it.

Owned by growth. `marts.channel_roi_daily` divides by what this produces.
"""

from __future__ import annotations

import pendulum
from airflow.sdk import dag, task

from include.lib.notify import notify
from include.lib.pipeline import Reject, lake_task
from projects.growth.lib import dbt, sessions
from projects.growth.lib.assets import CHANNEL_ATTRIBUTION, WEB_SESSIONS

#: The attribution window. `docs/late-data-policy.md` LD-1 asks for a measured
#: number rather than a copied one; this one was measured on the session feed
#: and covers the tail of the touch-to-order gap.
LOOKBACK_DAYS = 30

DEFAULT_ARGS = {
    "owner": "growth",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
    "on_failure_callback": notify("growth", "attribution build failed",
                                  runbook="ops/runbooks/alerting.md"),
}

LAKE_CONFIG = {"reject_table": "ops.rejects",
               "retry": {"attempts": 2, "delay_seconds": 60}}


@dag(
    dag_id="gro_attribution_daily",
    schedule=[WEB_SESSIONS],
    start_date=pendulum.datetime(2026, 1, 5, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params={"lake_config": LAKE_CONFIG},
    tags=["growth", "attribution", "mart"],
    doc_md=__doc__,
)
def gro_attribution_daily():

    @task
    def days_to_attribute() -> list[str]:
        """The session days whose attribution is missing or stale."""
        return sessions.days_pending(LOOKBACK_DAYS)

    stage_sessions = dbt.selection("stage_sessions", "int_sessions_funnel")
    stitch_touches = dbt.selection("stitch_touches", "int_session_attributed")

    @lake_task(map_index_template="{{ task.op_kwargs['day'] }}")
    def attribute_day(day: str) -> int:
        """Credit one day's orders to the last non-direct touch."""
        return sessions.attribute_touches(day, LOOKBACK_DAYS)

    order_touches = dbt.selection("order_touches", "int_touch_ordered",
                                  variables={"lookback_days": LOOKBACK_DAYS})
    channel_funnel = dbt.selection("channel_funnel", "agg_channel_funnel_daily")

    @lake_task
    def orders_reconcile(days: list[str]) -> dict[str, int]:
        """Attributed orders against converting sessions, day by day.

        Every converting session gets exactly one credit, so the two numbers
        are equal. A gap means a session converted against an order the fact
        does not have, which is worth a look before anybody quotes a return
        on spend.
        """
        gaps = sessions.attribution_gaps(days)
        if gaps:
            raise Reject(f"{len(gaps)} day(s) attribute a different number of "
                         "orders than converted", rows=gaps)
        return {"days": len(days)}

    @task(outlets=[CHANNEL_ATTRIBUTION])
    def publish_attribution(days: list[str]) -> list[str]:
        """Announce the days that changed."""
        return days

    days = days_to_attribute()
    attributed = attribute_day.expand(day=days)
    days >> stage_sessions >> stitch_touches >> attributed
    attributed >> order_touches >> channel_funnel
    channel_funnel >> orders_reconcile(days) >> publish_attribution(days)


gro_attribution_daily()
