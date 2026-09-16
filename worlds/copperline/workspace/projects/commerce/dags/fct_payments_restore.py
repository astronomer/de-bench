"""Rebuild a window of `marts.fct_payments` from the payments landing files.

Triggered by hand, never scheduled. It was written after the archive gap in
February 2026, when a truncated fact table had to be rebuilt and nobody had a
sanctioned way to do it. `ops/incidents/2026-02-16-archive-gap.md` is the note.

Give it a date range and it rebuilds one day at a time from the landing copy of
the processor feeds — the immutable source — rather than from any mart. A
rebuild that reads a downstream aggregate produces a fact table that agrees with
itself and stands for nothing.

**It checks the source exists before it writes.** For a date whose landing files
are not there, the right answer is to say so and stop, not to interpolate the
day from its neighbours. The first task is that check, and it reports every
missing date rather than the first one.

Produces the named window of `marts.fct_payments`. Nothing schedules it.

Owned by commerce (J. Mwangi).
"""

from __future__ import annotations

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, Param

from include.lib import landing_dir, warehouse
from include.lib.notify import notify
from projects.commerce.lib import sql

DEFAULT_ARGS = {
    "owner": "commerce",
    "retries": 1,
    "retry_delay": pendulum.duration(minutes=5),
}


def window(params: dict) -> list[str]:
    """Every date in the requested range, inclusive at both ends."""
    start = pendulum.parse(params["start_date"]).date()
    end = pendulum.parse(params["end_date"]).date()
    if end < start:
        raise ValueError(f"end_date {end} is before start_date {start}")
    return [start.add(days=n).isoformat() for n in range((end - start).days + 1)]


def check_source_present(params: dict) -> int:
    """Every date in the window has a landing copy to rebuild from.

    Six days of February 2026 have none — the live copies were deleted and the
    archive copy was never written. For those dates there is no source, and
    saying so is the answer.
    """
    missing = [
        day for day in window(params)
        if not (landing_dir("meridian") / f"dt={day}").is_dir()
    ]
    if missing:
        raise ValueError(
            f"no payments landing files for {', '.join(missing)}. There is no "
            "source for those days; do not fill them from a mart."
        )
    return len(window(params))


def rebuild_days(params: dict) -> int:
    """Rebuild each day of the window from its own landing files.

    One day at a time, each a delete-insert on its own partition, so a rebuild
    that stops halfway leaves the days it finished correct and the rest
    untouched.
    """
    rebuilt = 0
    with warehouse.connect() as con:
        for day in window(params):
            rebuilt += int(con.execute(
                sql.read("payments_restore_day"), [day, day],
            ).fetchone()[0])
    return rebuilt


def check_against_source(params: dict) -> int:
    """The rebuilt days against the landing files they came from, row for row.

    A rebuild is only worth having if it reproduces the source exactly. This
    counts both sides per day and fails on the first day that disagrees.
    """
    with warehouse.connect(read_only=True) as con:
        for day in window(params):
            landed, rebuilt = con.execute(
                sql.read("payments_restore_check"), [day, day],
            ).fetchone()
            if landed != rebuilt:
                raise ValueError(
                    f"{day}: the landing files hold {landed} events and the "
                    f"rebuild wrote {rebuilt}"
                )
    return len(window(params))


def record_restore(params: dict) -> int:
    """A row per rebuilt day in `ops.restore_log`, so the rebuild is on the
    record and a later question has an answer."""
    days = window(params)
    with warehouse.connect() as con:
        con.execute("CREATE SCHEMA IF NOT EXISTS ops")
        con.execute(sql.read("restore_log_ddl"))
        for day in days:
            warehouse.delete_insert(
                "ops.restore_log", "ds", day,
                [{"ds": day, "table_name": "marts.fct_payments",
                  "reason": params["reason"]}],
                con=con,
            )
    return len(days)


with DAG(
    dag_id="fct_payments_restore",
    schedule=None,
    start_date=pendulum.datetime(2024, 2, 4, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params={
        "start_date": Param(type="string", format="date"),
        "end_date": Param(type="string", format="date"),
        "reason": Param("", type="string"),
    },
    tags=["commerce", "restore", "payments"],
    doc_md=__doc__,
    on_failure_callback=notify("commerce", "payments restore"),
) as dag:
    # `params` is a context key, so every callable below takes it by name and
    # none of them needs op_kwargs.
    source = PythonOperator(
        task_id="check_source_present", python_callable=check_source_present)
    rebuild = PythonOperator(
        task_id="rebuild_days", python_callable=rebuild_days)
    verify = PythonOperator(
        task_id="check_against_source", python_callable=check_against_source)
    record = PythonOperator(
        task_id="record_restore", python_callable=record_restore)

    source >> rebuild >> verify >> record
