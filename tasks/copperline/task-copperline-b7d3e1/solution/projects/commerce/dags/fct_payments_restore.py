"""Rebuild a window of Meridian's event record from the payments landing files.

Triggered by hand, never scheduled. It was written after the archive gap in
February 2026, when a truncated fact table had to be rebuilt and nobody had a
sanctioned way to do it. `ops/incidents/2026-02-16-archive-gap.md` is the note.

Give it a settlement window and it rebuilds `ops.payments_restored` one
settlement date at a time from the landing copy of the Meridian feed — the
immutable source — rather than from any table of ours. A rebuild that reads
`raw.pay_meridian_settlements` or a mart produces a record that agrees with
itself and stands for nothing, which is the one thing a disputed record must
not do.

**The tree is dated by arrival, not by settlement.**
`landing/meridian/dt=<ds>/hr=<hh>/events.jsonl` is the file Meridian delivered
in that hour (`payments_intake` says so), so an event settled on the 6th can
arrive on the 9th, or in April. Two things follow and both were wrong here
until RR-118:

* A settlement date has no directory of its own. The whole tree is read once
  and filtered on `settlement_date`, which is the only way to catch the events
  that settled inside the window and arrived outside it — including the tail
  that reaches back past the 90 live days RET-1 keeps.
* The absence of a `dt=` directory for a date says nothing about whether that
  settlement date can be served, so it is not a reason to abandon the run.

**It still never fills a gap.** A settlement date the landing copy cannot
reach comes out with the rows the copy holds and no others, which for some
dates is none. Naming those dates is the answer for them; interpolating them
from a neighbour, or lifting them from a mart, is not.

Produces `ops.payments_restored` over the named window, and a row per date in
`ops.restore_log`. Nothing schedules it.

The defaults serve RR-118, the Meridian dispute over February and March 2026,
so a plain trigger with no run configuration does what the restore request
asks. Give `start_date` and `end_date` in the run configuration for any other
window.

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

#: The table the restore writes. Not a mart: a mart is our reading of the feed
#: and this is the feed.
TABLE = "ops.payments_restored"


def source_glob() -> str:
    """Every hourly file in the live Meridian landing tree.

    One glob over the whole tree rather than one directory per date, for the
    reason in the module docstring: the directories are arrival dates and the
    window is a settlement window.
    """
    return str(landing_dir("meridian") / "dt=*" / "hr=*" / "events.jsonl")


def window(params: dict) -> list[str]:
    """Every settlement date in the requested range, inclusive at both ends."""
    start = pendulum.parse(params["start_date"]).date()
    end = pendulum.parse(params["end_date"]).date()
    if end < start:
        raise ValueError(f"end_date {end} is before start_date {start}")
    return [start.add(days=n).isoformat() for n in range((end - start).days + 1)]


def check_source_present(params: dict) -> int:
    """Which of the requested settlement dates the landing copy can reach.

    The question this step was written to ask is still the right one — a date
    with no source must never be filled in from somewhere else — but it was
    asking it of the wrong thing. It looked for a `dt=<settlement date>`
    directory, and the directories are arrival dates. Their absence proves
    nothing and their presence proves nothing.

    What the tree can be asked is which requested settlement dates it holds an
    event for. This logs the dates it holds none for and lets the run carry on:
    the restore serves what the landing copy has, and the dates it cannot reach
    belong in the restore note rather than in an exception that abandons the
    rest of the window.

    It still stops on the one condition that means there is no source at all —
    a landing tree with nothing in it.
    """
    days = window(params)
    with warehouse.connect(read_only=True) as con:
        covered = {
            str(row[0]) for row in con.execute(
                "SELECT DISTINCT settlement_date::DATE FROM read_json(?, "
                "format = 'newline_delimited', union_by_name = true) "
                "WHERE settlement_date::DATE BETWEEN ?::DATE AND ?::DATE",
                [source_glob(), days[0], days[-1]],
            ).fetchall()
        }
    if not covered:
        raise ValueError(
            f"the Meridian landing tree holds no event settled between "
            f"{days[0]} and {days[-1]}. There is no source for this window; do "
            "not fill it from a mart."
        )
    missing = [day for day in days if day not in covered]
    if missing:
        print(
            f"{len(missing)} of {len(days)} settlement dates have no rows in the "
            f"live landing tree and will come out empty: {', '.join(missing)}"
        )
    return len(days) - len(missing)


def rebuild_days(params: dict) -> int:
    """Rebuild each settlement date of the window from the landing files.

    The tree is scanned once into a staged copy, then each date is a
    delete-insert on its own partition, so a rebuild that stops halfway leaves
    the dates it finished correct and the rest untouched.
    """
    days = window(params)
    rebuilt = 0
    with warehouse.connect() as con:
        # Qualified: the connection's search path puts the frozen `nwv` copy
        # first, and it is attached read-only, so an unqualified CREATE goes
        # to it and raises.
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {warehouse.qualify('ops')}")
        con.execute(sql.read("payments_restored_ddl"))
        con.execute(sql.read("payments_restore_source"),
                    [source_glob(), days[0], days[-1]])
        for day in days:
            try:
                con.execute("BEGIN TRANSACTION")
                con.execute(sql.read("payments_restore_delete_day"), [day])
                counted = con.execute(
                    sql.read("payments_restore_day"), [day]).fetchone()
                con.execute("COMMIT")
            except Exception:
                con.execute("ROLLBACK")
                raise
            rebuilt += int(counted[0]) if counted else 0
    return rebuilt


def check_against_source(params: dict) -> int:
    """The rebuilt dates against the landing files they came from, row for row.

    A rebuild is only worth having if it reproduces the source exactly. Both
    sides are counted on `settlement_date`, and the first date that disagrees
    fails the run.
    """
    days = window(params)
    with warehouse.connect(read_only=True) as con:
        breaks = con.execute(
            sql.read("payments_restore_check"),
            [source_glob(), days[0], days[-1], days[0], days[-1]],
        ).fetchall()
    if breaks:
        day, landed, rebuilt = breaks[0]
        raise ValueError(
            f"{day}: the landing files hold {landed} events settled that day "
            f"and the rebuild wrote {rebuilt}"
        )
    return len(days)


def record_restore(params: dict) -> int:
    """A row per rebuilt date in `ops.restore_log`, so the rebuild is on the
    record and a later question has an answer."""
    days = window(params)
    with warehouse.connect() as con:
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {warehouse.qualify('ops')}")
        con.execute(sql.read("restore_log_ddl"))
        for day in days:
            warehouse.delete_insert(
                "ops.restore_log", "ds", day,
                [{"ds": day, "table_name": TABLE,
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
        "start_date": Param("2026-02-01", type="string", format="date"),
        "end_date": Param("2026-03-31", type="string", format="date"),
        "reason": Param(
            "RR-118 Meridian dispute, February and March 2026 settlements",
            type="string"),
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
