"""Driftwood's event stream, landed hourly into `raw.web_events`.

Every hour this takes the deliveries that arrived since the last run and puts
them in the warehouse. The window is a window of `load_time` — when Driftwood
sent it — because that is what the landing tree is partitioned on. An event's
own hour is `event_time_utc` and can be a day older, which is what
`gro_sessionize_daily` sorts out afterwards.

The window comes from `include.lib.watermark`, not from `{{ ds }}`. On an
hourly schedule `ds` is the same string for twenty-four consecutive runs, so
anything keyed on it either re-reads the whole day every hour or deletes the
sibling hours that already landed.

Two checks hang off the load rather than sitting in front of it. Both can
reject a batch, and a reject skips what is downstream of it, so neither is
allowed to stand between the load and the watermark.

Owned by growth. Breaks here stall sessions, the funnel and attribution, and
the campaign team notice by mid-morning.
"""

from __future__ import annotations

import datetime as dt

import pendulum
from airflow.sdk import dag, get_current_context, task

from include.lib import watermark
from include.lib.notify import notify
from include.lib.pipeline import Reject, lake_task, manifest
from projects.growth.lib import clickstream
from projects.growth.lib.assets import WEB_EVENTS

#: The stream name the watermark is filed under. One name, one stream.
STREAM = "web_events"

DEFAULT_ARGS = {
    "owner": "growth",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
    "on_failure_callback": notify("growth", "clickstream intake failed",
                                  runbook="ops/runbooks/alerting.md"),
}

LAKE_CONFIG = {"reject_table": "ops.rejects",
               "retry": {"attempts": 2, "delay_seconds": 60}}


@dag(
    dag_id="gro_clickstream_intake",
    schedule="0 * * * *",
    start_date=pendulum.datetime(2026, 1, 5, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params={"lake_config": LAKE_CONFIG},
    tags=["growth", "clickstream", "intake"],
    doc_md=__doc__,
)
def gro_clickstream_intake():

    @lake_task
    def delivery_window() -> dict[str, str]:
        """The window of `load_time` this run owns, half open."""
        context = get_current_context()
        start = watermark.since(STREAM, context)
        end = _naive(context["data_interval_end"])
        return {"start": start.isoformat(), "end": end.isoformat()}

    @lake_task
    def hour_partitions() -> list[str]:
        """The delivery directories the window covers.

        Driftwood keeps a week of files, so an hour older than that is not on
        disk and is not an error here — `raw.web_events` is where the history
        lives, and `gro_event_replay_repair` is the way back to a lost one.
        """
        start, end = _window()
        return clickstream.hour_partitions(start, end)

    # `outlets` is Airflow's argument, so the publishing step is a plain task.
    @task(outlets=[WEB_EVENTS])
    def load_events() -> int:
        """Replace the window's rows and return how many landed."""
        start, end = _window()
        return clickstream.load_window(manifest.get("hour_partitions") or [],
                                       start, end)

    @lake_task
    def producer_coverage() -> dict[str, int]:
        """Rows per producer, and a reject when a live producer went quiet.

        The nightly batch is absent for most hours of the day by design, so
        only the four live producers are expected every hour.
        """
        start, end = _window()
        counts = clickstream.producer_counts(start, end)
        silent = [name for name in clickstream.LIVE_PRODUCERS if not counts.get(name)]
        if silent:
            raise Reject(f"no events from {', '.join(silent)}",
                         rows=[{"producer": name} for name in silent])
        return counts

    @lake_task
    def delivery_lag() -> dict[str, int]:
        """The worst lag per producer, against that producer's own bound.

        Four hours for the live producers and twenty-six for the nightly
        batch, which sweeps the e-mail platform's store once a day.
        """
        start, end = _window()
        profile = clickstream.lag_profile(start, end)
        late = clickstream.over_bound(profile)
        if late:
            raise Reject(
                "delivery past the agreed lag: "
                + ", ".join(f"{name} {minutes}m" for name, minutes in late.items()),
                rows=[{"producer": name, "lag_minutes": minutes}
                      for name, minutes in late.items()],
            )
        return profile

    @task
    def mark_loaded() -> str:
        """Move the watermark. Last, and only once the rows are in."""
        return watermark.advance(STREAM).isoformat()

    window = delivery_window()
    partitions = hour_partitions()
    loaded = load_events()
    window >> partitions >> loaded
    loaded >> mark_loaded()
    loaded >> [producer_coverage(), delivery_lag()]


def _window() -> tuple[dt.datetime, dt.datetime]:
    """The window `delivery_window` recorded, for this run's interval."""
    recorded = manifest.get("delivery_window")
    return (dt.datetime.fromisoformat(recorded["start"]),
            dt.datetime.fromisoformat(recorded["end"]))


def _naive(value) -> dt.datetime:
    """An interval bound as naive UTC, which is how DuckDB holds a timestamp."""
    if value.tzinfo is not None:
        return value.astimezone(dt.timezone.utc).replace(tzinfo=None)
    return value


gro_clickstream_intake()
