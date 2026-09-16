"""Sessions, stitched from the event stream into `marts.fct_web_sessions`.

Scheduled on `raw.web_events` rather than on a clock, because the hourly
intake is what decides when there is anything to do.

An asset-triggered run carries no meaningful interval — `logical_date` can be
absent altogether — so nothing here templates `{{ ds }}` and nothing here
takes a watermark. The days to rebuild come from the feed itself: an event
day whose session rows no longer account for its events is a day to rebuild.
That is idempotent, it survives a run that fires twice, and it needs no clock.

A delivery window is a window of `load_time`, and the events inside it can
belong to several days — the nightly producer runs a day behind by design —
so a run rebuilds every day that moved, whole. Rebuilding the day rather than
appending to it is `docs/late-data-policy.md` §LD-2, and it is the difference
between a late event and a doubled session.

Owned by growth. `marts.fct_web_sessions` is the denominator of conversion
rate for the whole company, so a bad day here is a bad number in two teams'
dashboards.
"""

from __future__ import annotations

import pendulum
from airflow.sdk import Param, dag, get_current_context, task

from include.lib.notify import notify
from include.lib.pipeline import Reject, lake_task
from projects.growth.lib import sessions
from projects.growth.lib.assets import WEB_EVENTS, WEB_SESSIONS

#: How far back a moved event can drag a day. The nightly producer posts up
#: to twenty-six hours late, so two days covers it and three is the margin.
LOOKBACK_DAYS = 3

DEFAULT_ARGS = {
    "owner": "growth",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
    "on_failure_callback": notify("growth", "sessionization failed",
                                  runbook="ops/runbooks/alerting.md"),
}

LAKE_CONFIG = {"reject_table": "ops.rejects",
               "retry": {"attempts": 2, "delay_seconds": 60}}


@dag(
    dag_id="gro_sessionize_daily",
    schedule=[WEB_EVENTS],
    start_date=pendulum.datetime(2026, 1, 5, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params={
        "lake_config": LAKE_CONFIG,
        "days": Param([], type="array",
                      description="Event days to rebuild. Empty means the "
                                  "days whose events have moved."),
    },
    tags=["growth", "clickstream", "mart"],
    doc_md=__doc__,
)
def gro_sessionize_daily():

    @task
    def days_to_rebuild() -> list[str]:
        """The event days this run owns, oldest first.

        Mapped over, so it is a plain task and its list is an ordinary XCom.
        """
        asked = list(get_current_context()["params"].get("days") or [])
        return asked or sessions.days_pending(LOOKBACK_DAYS)

    @lake_task(map_index_template="{{ task.op_kwargs['day'] }}")
    def build_day(day: str) -> int:
        """Rebuild one event day's sessions and return the rows."""
        return sessions.build_sessions(day)

    @lake_task
    def stitch_orders(days: list[str]) -> int:
        """Attach the customer to sessions that converted as a guest.

        About fifteen per cent of orders carry no customer at checkout. The
        order knows who placed it, so the session is resolved afterwards and
        the per-customer numbers keep an honest denominator.
        """
        return sum(sessions.stitch_orders(day) for day in days)

    @lake_task
    def replay_duplicates(days: list[str]) -> int:
        """Events that arrived twice, counted by `event_id`.

        The January replay put the same events back at new stream offsets and
        both rows are in the feed on purpose, so this counts them rather than
        failing. A jump means the collector has replayed something again.
        """
        return sessions.replayed_events(days)

    @lake_task
    def session_grain(days: list[str]) -> int:
        """One row per session per day, or a reject naming the duplicates."""
        duplicates = sessions.duplicate_sessions(days)
        if duplicates:
            raise Reject(f"{len(duplicates)} session ids appear twice in a day",
                         rows=duplicates)
        return len(days)

    @lake_task
    def record_volume(days: list[str]) -> int:
        """Sessions per day into `ops.session_daily`.

        `config/alerts.yml` watches that table for volume, and an alert wants
        a small table it can read rather than the wide fact.
        """
        return sum(sessions.record_volume(day) for day in days)

    @task(outlets=[WEB_SESSIONS])
    def publish_sessions(days: list[str]) -> list[str]:
        """Announce the days that changed, so the marts downstream fire."""
        return days

    days = days_to_rebuild()
    build_day.expand(day=days) >> stitch_orders(days) >> [
        replay_duplicates(days),
        session_grain(days),
        record_volume(days),
    ] >> publish_sessions(days)


gro_sessionize_daily()
