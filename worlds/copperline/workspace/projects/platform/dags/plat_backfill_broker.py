"""The sanctioned way to replay a date range.

Triggered by hand, with a DAG id and two dates. It works the range one day at a
time, in order, and records what it did. Use it instead of clearing runs in the
UI, for three reasons that have each cost somebody a week:

1. **A cleared run re-executes the code that run originally used.** Runs are
   served from a bundle and a run records its bundle version, so clearing a run
   from March replays March's code against today's data. A backfill takes the
   latest version. When the point of the replay is "run today's fix over old
   days", clearing is the wrong tool and looks like the right one.
2. **Nothing fills a gap by itself.** Every DAG here is `catchup=False`, so a
   day that was never scheduled is never scheduled later. Somebody has to ask
   for it, and this is the asking.
3. **A closed month does not get replayed.** `docs/finance-policy.md` §REV-8
   is a rule about the books, not about the pipeline, and this DAG refuses a
   range that reaches into one rather than leaving it to whoever typed the
   dates.

Owned by data-platform.
"""

from __future__ import annotations

import datetime as dt

import pendulum
from airflow.sdk import Param, dag

from include.lib import calendar as cal
from include.lib import warehouse
from include.lib.notify import notify
from include.lib.pipeline import lake_task
from projects.platform.lib.deployment import trigger_dag_run, wait_for_dag_run

#: What was replayed, when, by whom, and why. One row per requested range.
TABLE = "ops.backfill_log"

#: A range longer than this is almost always a typo, and the two that were not
#: were both planned a week in advance. Ask for it in two goes.
MAX_DAYS = 120

DEFAULT_ARGS = {
    "owner": "platform",
    "retries": 0,
    "on_failure_callback": notify("platform", "a requested backfill did not complete"),
}


@dag(
    dag_id="plat_backfill_broker",
    schedule=None,
    start_date=pendulum.datetime(2024, 2, 4, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params={
        "target_dag_id": Param(type="string", description="the DAG to replay"),
        "start_date": Param(type="string", format="date",
                            description="first day to replay, inclusive"),
        "end_date": Param(type="string", format="date",
                          description="last day to replay, inclusive"),
        "reason": Param(type="string",
                        description="why. It goes in the log and somebody reads it."),
        "requested_by": Param(type="string", description="who asked"),
        "dry_run": Param(True, type="boolean",
                         description="plan the range and record it, trigger nothing"),
    },
    tags=["platform", "backfill", "ops"],
    doc_md=__doc__,
)
def plat_backfill_broker():
    @lake_task
    def validate(params: dict) -> dict:
        """The request is well formed and inside the limits.

        Both dates are inclusive, the range runs forwards, and it is not longer
        than `MAX_DAYS`. Everything here is about the request; the next task is
        about the books.
        """
        start = dt.date.fromisoformat(str(params["start_date"]))
        end = dt.date.fromisoformat(str(params["end_date"]))
        if end < start:
            raise ValueError(f"the range runs backwards: {start} to {end}")
        days = (end - start).days + 1
        if days > MAX_DAYS:
            raise ValueError(f"{days} days asked for, and the limit is {MAX_DAYS}")
        if not str(params.get("reason") or "").strip():
            raise ValueError("a backfill needs a reason; it goes in the log")
        return {"target_dag_id": params["target_dag_id"], "start": start.isoformat(),
                "end": end.isoformat(), "days": days}

    @lake_task
    def refuse_closed_months(request: dict) -> dict:
        """Refuse a range that reaches into a closed fiscal month.

        A month closes on the 5th business day of the next one, and after that
        nothing restates it. Where a closed day genuinely has to be corrected,
        `fin_restatement_apply` is the path and it writes what it did.
        """
        start = dt.date.fromisoformat(request["start"])
        today = cal.today()
        with warehouse.connect(read_only=True) as con:
            closed_through = con.execute(
                f"SELECT max(close_date) FROM "
                f"{warehouse.qualify('raw.finance_close_calendar')} "
                "WHERE close_date <= ?",
                [today],
            ).fetchone()[0]
        if closed_through is not None and start <= closed_through:
            raise RuntimeError(
                f"{start} is inside a month closed on {closed_through}. "
                "fin_restatement_apply is the path for a closed month."
            )
        return request

    @lake_task
    def days(request: dict) -> list[str]:
        """The range, as a list of days, in order.

        In order matters. Several of the DAGs worth replaying carry a running
        figure from the day before, and a range replayed out of order builds
        each day on a day that has not been rebuilt yet.
        """
        start = dt.date.fromisoformat(request["start"])
        return [(start + dt.timedelta(days=offset)).isoformat()
                for offset in range(request["days"])]

    @lake_task(map_index_template="{{ task.op_kwargs['day'] }}",
               pool="warehouse_write")
    def replay(day: str, request: dict, params: dict) -> dict:
        """Start one day of the target DAG, and wait for it to finish.

        One at a time. The mapped task takes the `warehouse_write` pool, which
        has one slot, because the target writes the warehouse and the warehouse
        takes one writer. Four days at once would spend the time queueing
        anyway and would leave the log unreadable.

        It starts a new run rather than clearing the old one, on purpose. See
        the DAG's own note: a cleared run replays the code that run first used.
        """
        if params.get("dry_run"):
            return {"day": day, "state": "planned"}
        run_id = f"backfill__{day}"
        started = trigger_dag_run(request["target_dag_id"], day, run_id=run_id,
                                  conf={"backfill": True, "reason": params["reason"]})
        state = wait_for_dag_run(request["target_dag_id"],
                                 started.get("dag_run_id", run_id))
        if state != "success":
            raise RuntimeError(f"{request['target_dag_id']} for {day} ended {state}")
        return {"day": day, "state": state}

    @lake_task
    def record(results: list[dict], request: dict, params: dict,
               run_id: str) -> int:
        """One row per day replayed, with the reason and who asked."""
        rows = [
            {"run_id": run_id, "target_dag_id": request["target_dag_id"],
             "ds": result["day"], "state": result["state"],
             "reason": str(params["reason"]), "requested_by": str(params["requested_by"])}
            for result in results
        ]
        with warehouse.connect() as con:
            con.execute(f"CREATE SCHEMA IF NOT EXISTS {warehouse.qualify('ops')}")
            con.execute(
                f"""CREATE TABLE IF NOT EXISTS {warehouse.qualify(TABLE)} (
                    run_id VARCHAR, target_dag_id VARCHAR, ds VARCHAR,
                    state VARCHAR, reason VARCHAR, requested_by VARCHAR)"""
            )
            con.executemany(
                f"INSERT INTO {warehouse.qualify(TABLE)} VALUES (?, ?, ?, ?, ?, ?)",
                [[row["run_id"], row["target_dag_id"], row["ds"], row["state"],
                  row["reason"], row["requested_by"]] for row in rows],
            )
        return len(rows)

    request = refuse_closed_months(validate())
    results = replay.partial(request=request).expand(day=days(request))
    record(results, request=request)


plat_backfill_broker()
