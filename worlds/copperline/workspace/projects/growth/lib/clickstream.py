"""The Driftwood collector: what it sends, when it arrives, and how we load it.

Driftwood is the event collector in front of the web shop and the two apps.
It writes hourly parquet to `landing/events/`, partitioned on the clock it
*delivered* on rather than the clock the event happened on:

    landing/events/dt=<ds>/hr=<hh>/producer=<p>/part-000.parquet

That partitioning is the whole reason this module exists. A delivery
partition holds events from several hours, and an event's own hour is in
`event_time_utc`, not in the path. So the hourly intake owns a window of
`load_time` and takes whatever event times arrive inside it.

**The landing tree is short.** Driftwood keeps a week of files and no more;
`raw.web_events` holds the history. Anything that wants an older hour reads
the warehouse, not the tree, and `gro_event_replay_repair` is the way back
when the tree is the only copy.

**Each producer has its own lag, and they are not close.** The four live
producers post within four hours. `nightly_batch` is a once-a-day sweep from
the e-mail platform's own store and posts up to twenty-six hours behind the
event. One bound over both says the nightly feed is broken every night, and a
bound wide enough for the nightly feed says nothing about the live ones. So
the bound is per producer and `LAG_BOUND_HOURS` is where it is written down.
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path
from typing import Iterable

from include.lib import landing_dir, warehouse

__all__ = ["PRODUCERS", "LIVE_PRODUCERS", "NIGHTLY_PRODUCER", "LAG_BOUND_HOURS",
           "EVENT_COLUMNS", "hour_partitions", "load_window", "lag_profile",
           "producer_counts", "over_bound", "landing_root"]

#: Every producer Driftwood declares. A stream from anything else is a feed we
#: were not told about, and the intake rejects the batch rather than load it.
LIVE_PRODUCERS = ("web", "ios", "android", "email_click")
NIGHTLY_PRODUCER = "nightly_batch"
PRODUCERS = (*LIVE_PRODUCERS, NIGHTLY_PRODUCER)

#: How far behind the event a delivery may be, per producer, in hours.
LAG_BOUND_HOURS = {**{name: 4 for name in LIVE_PRODUCERS}, NIGHTLY_PRODUCER: 26}

#: The columns `raw.web_events` carries, in the order the collector writes
#: them. `dt` and `hr` are the delivery partition and are not columns.
EVENT_COLUMNS = (
    "event_id", "producer", '"partition"', '"offset"', "session_id",
    "anonymous_id", "customer_ref", "event_name", "event_time_utc", "load_time",
    "page_path", "sku", "order_id", "utm_source", "utm_medium", "utm_campaign",
    "device", "country_code",
)


def hour_partitions(start: dt.datetime, end: dt.datetime) -> list[str]:
    """The delivery partitions covering `[start, end)`, as glob patterns.

    One pattern per hour, over every producer. An hour with no directory is
    left out rather than globbed at: `read_parquet` raises on a pattern that
    matches nothing, and an hour in which nobody delivered is an ordinary
    quiet hour, not a failure.
    """
    root = landing_dir("events")
    patterns = []
    hour = start.replace(minute=0, second=0, microsecond=0)
    while hour < end:
        directory = root / f"dt={hour.date().isoformat()}" / f"hr={hour:%H}"
        if directory.is_dir():
            patterns.append(f"{directory}/producer=*/*.parquet")
        hour += dt.timedelta(hours=1)
    return patterns


def load_window(patterns: Iterable[str], start: dt.datetime,
                end: dt.datetime) -> int:
    """Replace `raw.web_events` for the delivery window and return the rows.

    The delete and the insert are one transaction over one window of
    `load_time`, so re-running an hour replaces that hour and touches no
    other. The window is half open — `start` is in, `end` is out — which is
    what keeps two neighbouring runs from claiming the same delivery.

    The replay is additive by design: an event that was delivered twice
    carries one `event_id` at two stream offsets, and both rows land. Readers
    that count events count by `event_id`.
    """
    patterns = list(patterns)
    table = warehouse.qualify("raw.web_events")
    columns = ", ".join(EVENT_COLUMNS)
    sources = ", ".join(f"'{pattern}'" for pattern in patterns)
    with warehouse.connect() as con:
        try:
            con.execute("BEGIN TRANSACTION")
            con.execute(
                f"DELETE FROM {table} WHERE load_time >= ? AND load_time < ?",
                [start, end],
            )
            loaded = 0
            if patterns:
                counted = con.execute(
                    f"INSERT INTO {table} BY NAME "
                    f"SELECT {columns} FROM read_parquet([{sources}], "
                    "hive_partitioning = true, union_by_name = true) "
                    "WHERE load_time >= ? AND load_time < ?",
                    [start, end],
                ).fetchall()
                loaded = int(counted[0][0]) if counted and counted[0] else 0
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise
    return loaded


def producer_counts(start: dt.datetime, end: dt.datetime) -> dict[str, int]:
    """Rows landed per producer in the delivery window."""
    with warehouse.connect(read_only=True) as con:
        rows = con.execute(
            f"SELECT producer, count(*) FROM {warehouse.qualify('raw.web_events')} "
            "WHERE load_time >= ? AND load_time < ? GROUP BY producer",
            [start, end],
        ).fetchall()
    return {producer: int(count) for producer, count in rows}


def lag_profile(start: dt.datetime, end: dt.datetime) -> dict[str, int]:
    """The worst delivery lag per producer in the window, in minutes.

    Compare each entry against `LAG_BOUND_HOURS` for the same producer. A
    producer that delivered nothing is absent from the answer, which is a
    different thing from a producer that delivered late.
    """
    with warehouse.connect(read_only=True) as con:
        rows = con.execute(
            "SELECT producer, "
            "max(date_diff('minute', event_time_utc, load_time)) "
            f"FROM {warehouse.qualify('raw.web_events')} "
            "WHERE load_time >= ? AND load_time < ? GROUP BY producer",
            [start, end],
        ).fetchall()
    return {producer: int(minutes or 0) for producer, minutes in rows}


def over_bound(profile: dict[str, int]) -> dict[str, int]:
    """The producers in `profile` that are past their own bound.

    Returns `{producer: minutes}` for each one over, empty when every
    producer is inside its bound. A producer nobody declared counts as over,
    because there is no bound to hold it to.
    """
    late = {}
    for producer, minutes in profile.items():
        bound = LAG_BOUND_HOURS.get(producer)
        if bound is None or minutes > bound * 60:
            late[producer] = minutes
    return late


def landing_root() -> Path:
    """`landing/events/`, for a task that wants to say where it looked."""
    return landing_dir("events")
