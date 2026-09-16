"""`marts.funnel_daily`: how far the day's sessions got.

Five cumulative steps — a page view, a product view, a cart, a checkout, an
order — counted in sessions rather than in events, because a session that
adds three things to a cart is one session that reached the cart.

Scheduled on the sessions asset. The funnel is a read of sessions and has
nothing to do until they are rebuilt, so a clock here would only produce a
funnel over half a day.

**The denominator is stated on the row.** Every row carries the day's session
count beside the step count, so a rate can be computed without a second
query and two readers cannot pick two denominators. `int_sessions_funnel` in
the dbt project holds the same definition on the model side.

Owned by growth. The weekly trading meeting reads this.
"""

from __future__ import annotations

import pendulum
from airflow.sdk import dag, task

from include.lib.notify import notify
from include.lib.pipeline import Reject, lake_task
from projects.growth.lib import dbt, sessions
from projects.growth.lib.assets import FUNNEL_DAILY, WEB_SESSIONS

#: The window a rebuilt session day can reach back into.
LOOKBACK_DAYS = 3

DEFAULT_ARGS = {
    "owner": "growth",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
    "on_failure_callback": notify("growth", "funnel build failed",
                                  runbook="ops/runbooks/alerting.md"),
}

LAKE_CONFIG = {"reject_table": "ops.rejects",
               "retry": {"attempts": 2, "delay_seconds": 60}}


@dag(
    dag_id="gro_funnel_daily",
    schedule=[WEB_SESSIONS],
    start_date=pendulum.datetime(2026, 1, 5, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params={"lake_config": LAKE_CONFIG},
    tags=["growth", "clickstream", "mart"],
    doc_md=__doc__,
)
def gro_funnel_daily():

    @task
    def days_to_build() -> list[str]:
        """The session days whose funnel is missing or stale."""
        return sessions.days_pending(LOOKBACK_DAYS)

    @lake_task(map_index_template="{{ task.op_kwargs['day'] }}")
    def build_day(day: str) -> int:
        """Rebuild one day's funnel and return the rows."""
        return sessions.build_funnel(day)

    stage_funnel = dbt.selection("stage_funnel", "int_sessions_funnel")

    @lake_task
    def steps_monotonic(days: list[str]) -> int:
        """Each step is smaller than the one before it, or the day rejects.

        The steps are cumulative, so a later step counting more sessions than
        an earlier one is a join that fanned out. It reads as a conversion
        rate above one hundred per cent, which somebody always spots — but
        only after it has been in a deck.
        """
        broken = sessions.funnel_out_of_order(days)
        if broken:
            raise Reject(f"{len(broken)} funnel step(s) out of order",
                         rows=broken)
        return len(days)

    @task(outlets=[FUNNEL_DAILY])
    def publish_funnel(days: list[str]) -> list[str]:
        """Announce the days that changed."""
        return days

    days = days_to_build()
    build_day.expand(day=days) >> stage_funnel >> steps_monotonic(days) \
        >> publish_funnel(days)


gro_funnel_daily()
