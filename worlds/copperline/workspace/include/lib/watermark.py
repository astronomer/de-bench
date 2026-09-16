"""Watermarks for incremental reads.

A watermark is one timestamp per named stream, kept in `ops.watermarks` in the
warehouse. It lives there rather than in Airflow's task state store because
five other things have to see it: the dbt incremental models, the freshness
checks, the ops dashboard, a person with a SQL prompt, and the run after this
one on a different worker.

Two functions, and they are used together or not at all.
"""

from __future__ import annotations

import datetime as dt
from typing import Any, Mapping

from . import warehouse

__all__ = ["TABLE", "since", "advance", "current"]

#: One row per (stream, interval). The table is created on first write.
TABLE = "ops.watermarks"

_DDL = (
    "CREATE SCHEMA IF NOT EXISTS ops",
    f"""CREATE TABLE IF NOT EXISTS {TABLE} (
        name VARCHAR NOT NULL,
        interval_start TIMESTAMP NOT NULL,
        mark TIMESTAMP NOT NULL,
        dag_id VARCHAR,
        task_id VARCHAR,
        run_id VARCHAR,
        PRIMARY KEY (name, interval_start)
    )""",
)


def _ensure(con) -> None:
    """Create the watermark table if this is the first stream to need it."""
    for statement in _DDL:
        con.execute(statement)


def since(name: str, context: Mapping[str, Any] | None = None) -> dt.datetime:
    """The low-water mark for this run, for an incremental read.

    Returns the stored mark for `name`, or `context["data_interval_start"]` if
    there is none — NOT `ds`, and not the wall clock. That matters the moment a
    DAG is not daily: on an hourly schedule `ds` is the same string for
    twenty-four consecutive runs, so a helper keyed by `ds` re-reads the whole
    day every hour, and a delete-insert scoped to `ds` deletes the sibling
    hours that already landed.

    The read is scoped to intervals BEFORE this one, so the mark a run gets is
    the mark it would have got the first time it ran. Replaying an old interval
    reproduces that interval's window and does not drag the mark backwards for
    the runs that came after it.

    Marks are UTC and naive, and the returned datetime is too. Use it as an
    open lower bound and the run's own `data_interval_end` as the closed upper
    one:

        rows = con.execute(
            "select * from copperline.raw.web_events "
            "where loaded_at >= ? and loaded_at < ?",
            [since("web_events"), context["data_interval_end"]],
        ).fetchall()
    """
    context = _context(context)
    start = _utc(context["data_interval_start"])
    with warehouse.connect() as con:
        _ensure(con)
        row = con.execute(
            f"SELECT max(mark) FROM {warehouse.qualify(TABLE)} "
            "WHERE name = ? AND interval_start < ?",
            [name, start],
        ).fetchone()
    return row[0] if row and row[0] is not None else start


def advance(name: str, context: Mapping[str, Any] | None = None) -> dt.datetime:
    """Store `data_interval_end` as the new mark for `name`, and return it.

    CALL IT LAST, AND ONLY ON SUCCESS. A task that reads with `since()` and
    never calls `advance()` re-reads the same window forever and never says so:
    the rows land again, the counts look plausible, and nothing fails. A task
    that calls `advance()` before it has written its rows loses the window it
    skipped, and that one is worse, because the rows are gone rather than
    doubled.

    The write is keyed by the run's own interval, so replaying an interval
    rewrites that interval's row and no other. It is an upsert: running the
    same interval twice stores the same mark twice and changes nothing.

    Under Airflow 3's default timetables a bare cron string gives a trigger
    timetable, where `data_interval_start` and `data_interval_end` are both the
    trigger time. That is still the right mark: the window a run owns is from
    the last trigger to this one, which is exactly what `since()` then returns.
    """
    context = _context(context)
    start, end = _utc(context["data_interval_start"]), _utc(context["data_interval_end"])
    task = context.get("task_instance")
    with warehouse.connect() as con:
        _ensure(con)
        con.execute(
            f"INSERT OR REPLACE INTO {warehouse.qualify(TABLE)} "
            "(name, interval_start, mark, dag_id, task_id, run_id) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            [name, start, end, getattr(task, "dag_id", None),
             getattr(task, "task_id", None), context.get("run_id")],
        )
    return end


def current(name: str) -> dt.datetime | None:
    """The newest mark stored for `name`, or None if the stream has never run.

    For people and for reports. A task reads with `since()`, which is scoped to
    the run's own interval; this one is not scoped to anything. It reads the
    snapshot, so a mark a run is still in the middle of writing is not here
    yet.
    """
    with warehouse.connect(read_only=True) as con:
        try:
            row = con.execute(
                f"SELECT max(mark) FROM {warehouse.qualify(TABLE)} WHERE name = ?",
                [name],
            ).fetchone()
        except Exception:  # the table does not exist until the first advance()
            return None
    return row[0] if row else None


def _context(context: Mapping[str, Any] | None) -> Mapping[str, Any]:
    if context is not None:
        return context
    from airflow.sdk import get_current_context

    return get_current_context()


def _utc(value: Any) -> dt.datetime:
    """An interval bound as a naive UTC datetime, which is how DuckDB stores
    a TIMESTAMP. A date is taken as midnight."""
    if isinstance(value, str):
        value = dt.datetime.fromisoformat(value)
    if isinstance(value, dt.datetime):
        if value.tzinfo is not None:
            value = value.astimezone(dt.timezone.utc).replace(tzinfo=None)
        return value
    if isinstance(value, dt.date):
        return dt.datetime(value.year, value.month, value.day)
    raise TypeError(f"not an interval bound: {value!r}")
