"""Republish the cross-team marts as assets under the house URI.

Three marts are read by a team other than the one that builds them, and the
build that produces them is the cosmos-rendered DAG, whose asset URIs are
cosmos's rather than ours. This DAG sits between: it waits on what the build
publishes, checks the partition is really there, and emits the house asset the
other teams subscribe to.

That indirection is deliberate and it is the reason a change in how the build
names its assets does not silently unsubscribe five DAGs in three projects. It
has failed to protect us once, which is why the check in the middle exists.

**This DAG has no data interval.** It is asset-scheduled, so a run is triggered
by an event rather than by a clock and `logical_date` may be None. Nothing here
templates `{{ ds }}`. The day being republished comes from the asset events
that triggered the run, and there is a `Param` for the case where somebody has
to republish a specific day by hand.

Owned by data-platform.
"""

from __future__ import annotations

from typing import Any

import pendulum
from airflow.sdk import Asset, Metadata, Param, dag, task

from include.lib import warehouse
from include.lib.notify import notify
from include.lib.pipeline import lake_task

#: What the build publishes, and what this DAG waits on.
BUILT = [
    Asset("duckdb://warehouse/dbt/marts.order_economics"),
    Asset("duckdb://warehouse/dbt/marts.inventory_position"),
    Asset("duckdb://warehouse/dbt/marts.sell_through_daily"),
]

#: The house asset, and what every cross-team consumer subscribes to.
PUBLISHED = {
    "marts.order_economics": Asset("duckdb://warehouse/marts.order_economics"),
    "marts.inventory_position": Asset("duckdb://warehouse/marts.inventory_position"),
    "marts.sell_through_daily": Asset("duckdb://warehouse/marts.sell_through_daily"),
}

#: The partition column each of the three carries. They do not agree, which is
#: itself worth knowing before a sensor is written against one of them.
PARTITION_COLUMN = {
    "marts.order_economics": "order_date",
    "marts.inventory_position": "ds",
    "marts.sell_through_daily": "ds",
}

DEFAULT_ARGS = {
    "owner": "platform",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
    "on_failure_callback": notify(
        "platform", "cross-team consumers are still waiting on yesterday's marts"
    ),
}


@dag(
    dag_id="plat_asset_republish",
    schedule=BUILT,
    start_date=pendulum.datetime(2024, 2, 4, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params={
        "target_ds": Param(
            None, type=["null", "string"], format="date",
            description="the day to republish. Left empty, the triggering "
                        "asset events say which day it is.",
        ),
    },
    tags=["platform", "assets", "marts"],
    doc_md=__doc__,
)
def plat_asset_republish():
    @lake_task
    def target_day(params: dict, triggering_asset_events: Any) -> str:
        """Which day this run republishes.

        The param wins when it is set, because somebody setting it is somebody
        who knows. Otherwise it is the day carried on the asset events that
        triggered the run — the build writes it as `run_date` in the event's
        extra. A run with neither is a run that cannot know, and it fails here
        rather than republishing a day it guessed.
        """
        if params.get("target_ds"):
            return str(params["target_ds"])
        days = {
            str(event.extra.get("run_date"))
            for events in triggering_asset_events.values() for event in events
            if event.extra and event.extra.get("run_date")
        }
        if len(days) == 1:
            return days.pop()
        raise RuntimeError(
            "the triggering events name "
            f"{'no day' if not days else 'more than one day: ' + ', '.join(sorted(days))}. "
            "Re-run with the target_ds param set."
        )

    @lake_task
    def verify(day: str) -> dict[str, int]:
        """Every one of the three holds rows for that day.

        Waiting on an asset says a task said it was done. It does not say the
        table has rows in it, and the once this DAG failed to protect anybody
        the table was empty and the event had fired anyway.
        """
        counts = {}
        with warehouse.connect(read_only=True) as con:
            for table, column in sorted(PARTITION_COLUMN.items()):
                counts[table] = con.execute(
                    f"SELECT count(*) FROM {warehouse.qualify(table)} WHERE {column} = ?",
                    [day],
                ).fetchone()[0]
        empty = sorted(table for table, rows in counts.items() if not rows)
        if empty:
            raise RuntimeError(f"no rows for {day} in: {', '.join(empty)}")
        return counts

    # Airflow's own decorator, not the house one: `outlets` is not a kwarg
    # `@lake_task` forwards, so a lake task asked to publish an asset records
    # the request and emits nothing.
    @task(outlets=list(PUBLISHED.values()))
    def republish(day: str, counts: dict[str, int]):
        """Emit the house asset for each of the three, with the day on it.

        The row count rides along in the event, which is what lets a consumer
        decide whether a very small day is worth acting on before it opens the
        warehouse.
        """
        for table, asset in sorted(PUBLISHED.items()):
            yield Metadata(asset, {"run_date": day, "row_count": counts[table]})

    day = target_day()
    counts = verify(day)
    republish(day, counts)


plat_asset_republish()
