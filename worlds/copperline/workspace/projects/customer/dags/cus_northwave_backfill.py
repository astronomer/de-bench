"""Replay a customer mart into the pre-acquisition era.

Triggered by hand, with the mart and the date range. It exists because the
acquired book's history predates Copperline's ownership of it: an account
opened in 2011 has orders from before we bought the company, and a mart
rebuilt only from the acquisition forward has a hole where they should be.

**Ask the broker, not this DAG, for an ordinary backfill.**
`plat_backfill_broker` is the sanctioned way to replay a date range for a
mart that has a normal history. This one is for the era before the book
landed, where the source is the frozen copy rather than the live feed, and
where a run has to say in its output which era it read.

**The frozen namespace is not a live source.** The `nwv` schema stopped
refreshing when the namespace was frozen and holds Northwave's own reporting
tables as they stood then. A figure taken from it is a figure from that date
whatever date you ask it for, so every read here is qualified onto the frozen
copy on purpose and the era is written onto every row.

**It writes its own partitions and nothing else.** A backfill that reached
outside its range would silently restate a month finance has closed.

Owned by customer, with the integration team, who no longer exist.
"""

from __future__ import annotations

import datetime as dt

import pendulum
from airflow.sdk import Param, dag, get_current_context, task

from include.lib.notify import notify
from include.lib.pipeline import lake_task
from projects.customer.lib import backfill

DEFAULT_ARGS = {
    "owner": "customer",
    "retries": 1,
    "on_failure_callback": notify("customer", "northwave backfill failed",
                                  runbook="ops/runbooks/alerting.md"),
}

LAKE_CONFIG = {"reject_table": "ops.rejects", "retry": {"attempts": 1}}


@dag(
    dag_id="cus_northwave_backfill",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 5, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params={
        "lake_config": LAKE_CONFIG,
        "mart": Param("marts.customer_360", type="string",
                      enum=sorted(backfill.REPLAYABLE),
                      description="The mart to replay."),
        "from_ds": Param(type="string", format="date",
                         description="First day to replay."),
        "to_ds": Param(type="string", format="date",
                       description="Last day to replay, inclusive."),
    },
    tags=["customer", "northwave", "backfill"],
    doc_md=__doc__,
)
def cus_northwave_backfill():

    @task
    def window() -> list[str]:
        """The days to replay, and the check that they are in the old era.

        A range that reaches past the day the book landed is refused rather
        than trimmed: half a replay from the frozen copy and half from the
        live feed produces a mart nobody can explain afterwards.
        """
        params = get_current_context()["params"]
        first = dt.date.fromisoformat(params["from_ds"])
        last = dt.date.fromisoformat(params["to_ds"])
        return backfill.era_window(first, last)

    @lake_task
    def source_rows(days: list[str]) -> dict[str, int]:
        """What the frozen copy holds for the window, before anything moves.

        Read first and recorded, so that the run has a baseline to compare
        its own output against. The frozen copy does not change, so this
        number is the same every time the same window is replayed.
        """
        return backfill.frozen_counts(days)

    @lake_task(map_index_template="{{ task.op_kwargs['day'] }}")
    def replay_day(day: str) -> int:
        """Rebuild one day of the mart from the frozen copy."""
        context = get_current_context()
        return backfill.replay(context["params"]["mart"], day)

    @lake_task
    def check_window(days: list[str]) -> dict[str, int]:
        """The replay wrote its own days and no others.

        Compared against the baseline the first task recorded, and against
        the range: a partition outside it would restate a month finance has
        closed.
        """
        context = get_current_context()
        return backfill.verify(context["params"]["mart"], days)

    days = window()
    source_rows(days) >> replay_day.expand(day=days) >> check_window(days)


cus_northwave_backfill()
