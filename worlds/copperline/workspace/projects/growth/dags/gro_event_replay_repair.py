"""Repair a window of the event stream after Driftwood replays it.

Triggered by hand, with the window to repair. It exists because a replay is
additive: the collector writes the recovered events into fresh partitions
with new offsets, so an event that had already reached us is in the feed
twice — one `event_id`, two `offset` values, identical payloads. Nothing in
the feed is wrong and nothing fails; what changes is that anything counting
by position now counts high.

What this does is rebuild every session day the replayed events belong to,
from the feed as it now stands, and report what moved. It does not delete the
replayed rows: the collector team keep the original offsets for their own
auditing and `ops/incidents/2026-01-23-event-replay.md` says so.

The window is the REPLAY's window — the days the recovered events happened
on, not the days the collector sent them. Give it `from_ds` and `to_ds`.

Owned by growth. Nobody remembered this DAG existed during the January
replay, which is why the runbook now names it.
"""

from __future__ import annotations

import datetime as dt

import pendulum
from airflow.sdk import Param, dag, get_current_context, task

from include.lib.notify import notify
from include.lib.pipeline import lake_task
from projects.growth.lib import sessions
from projects.growth.lib.assets import WEB_SESSIONS

DEFAULT_ARGS = {
    "owner": "growth",
    "retries": 1,
    "on_failure_callback": notify("growth", "event replay repair failed",
                                  runbook="ops/runbooks/alerting.md"),
}

LAKE_CONFIG = {"reject_table": "ops.rejects", "retry": {"attempts": 1}}


@dag(
    dag_id="gro_event_replay_repair",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 5, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params={
        "lake_config": LAKE_CONFIG,
        "from_ds": Param(type="string", format="date",
                         description="First event day to repair."),
        "to_ds": Param(type="string", format="date",
                       description="Last event day to repair, inclusive."),
    },
    tags=["growth", "clickstream", "repair"],
    doc_md=__doc__,
)
def gro_event_replay_repair():

    @task
    def window() -> list[str]:
        """Every event day in the requested window, inclusive at both ends."""
        params = get_current_context()["params"]
        first = dt.date.fromisoformat(params["from_ds"])
        last = dt.date.fromisoformat(params["to_ds"])
        if last < first:
            raise ValueError(f"to_ds {last} is before from_ds {first}")
        return [(first + dt.timedelta(days=step)).isoformat()
                for step in range((last - first).days + 1)]

    @lake_task(map_index_template="{{ task.op_kwargs['day'] }}")
    def rebuild_day(day: str) -> int:
        """Rebuild one day's sessions from the feed as it now stands."""
        return sessions.build_sessions(day)

    @lake_task
    def duplicates_found(days: list[str]) -> int:
        """Events in the window that now exist at two offsets.

        This is the size of the replay's overlap. It is a number to report,
        not a fault to fix: both rows belong in the feed.
        """
        return sessions.replayed_events(days)

    @task(outlets=[WEB_SESSIONS])
    def publish_repair(days: list[str]) -> list[str]:
        """Announce the repaired days, so the marts downstream rebuild."""
        return days

    days = window()
    rebuild_day.expand(day=days) >> duplicates_found(days) >> publish_repair(days)


gro_event_replay_repair()
