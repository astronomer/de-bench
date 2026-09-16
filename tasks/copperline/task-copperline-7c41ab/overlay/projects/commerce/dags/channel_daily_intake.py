"""Per-market channel feeds into `marts.channel_daily`.

Copperline sells in nine markets and each one sends its own daily channel file.
Not every market sends every day: `raw.market_calendar.feed_expected` says which
markets are due to send, and on the days a market shuts hard enough to send
nothing, a file that never arrives has arrived correctly.

So the graph branches. `pick_markets` reads the calendar for the run's own day
and returns the load tasks for the markets that owe a file; the rest skip. The
join then rolls whatever landed into `marts.channel_daily` and publishes the
same rows as `include/data/marts/channel_daily_<ds>.csv`.

A skipped market is not a failed market, so the join waits for every market to
be done and asks only that none of them failed.

Produces `marts.channel_daily` and the partition file beside it. Read by the
board pack's channel page, which reads the published files rather than the
table.

Owned by commerce (J. Mwangi).
"""

from __future__ import annotations

import pendulum
from airflow.providers.standard.operators.python import BranchPythonOperator, PythonOperator
from airflow.sdk import DAG

from include.lib import calendar, warehouse
from include.lib.notify import notify
from projects.commerce.lib import sql

DEFAULT_ARGS = {
    "owner": "commerce",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=10),
}

#: The nine markets, in the order `raw.market_calendar` spells them. A market is
#: added here and to the calendar together; `docs/runbooks/market-setup.md` is
#: the order those two go in.
MARKETS = ("US", "CA", "GB", "IE", "DE", "BR", "MX", "PL", "ID")

#: The published grain, in the order both writes take it.
CHANNEL_COLUMNS = ("ds", "market_code", "channel", "orders", "booked_cents")


def task_id_for(market: str) -> str:
    return f"load_{market.lower()}"


def pick_markets(ds: str) -> list[str]:
    """The load tasks for the markets due to send on `ds`.

    Read from `raw.market_calendar`, one row per market per date, for the day
    the run owns. A market that is not due is skipped rather than failed.
    """
    due = [m for m in MARKETS if calendar.feed_expected(ds, m)]
    return [task_id_for(market) for market in due]


def load_market(market: str, ds: str) -> int:
    """Replace one market's channel rows for the day."""
    with warehouse.connect() as con:
        con.execute("CREATE SCHEMA IF NOT EXISTS copperline.staging")
        con.execute(sql.read("channel_stage_ddl"))
        return int(con.execute(sql.read("channel_load_market"), [ds, market]).fetchone()[0])


def build_channel_daily(ds: str) -> int:
    """Roll the markets that landed up into `marts.channel_daily`, and publish
    the same rows as the day's partition file."""
    with warehouse.connect() as con:
        con.execute("CREATE SCHEMA IF NOT EXISTS copperline.marts")
        con.execute(sql.read("channel_mart_ddl"))
        rows = con.execute(sql.read("channel_daily_build"), [ds, ds]).fetchall()
    warehouse.delete_insert(
        "marts.channel_daily", "ds", ds, rows, columns=list(CHANNEL_COLUMNS)
    )
    warehouse.write_partition("marts", "channel_daily", ds, CHANNEL_COLUMNS, rows)
    return len(rows)


with DAG(
    dag_id="channel_daily_intake",
    schedule="0 4 * * *",
    start_date=pendulum.datetime(2024, 2, 4, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["commerce", "mart", "channel"],
    doc_md=__doc__,
    on_failure_callback=notify("commerce", "channel daily intake"),
) as dag:
    pick = BranchPythonOperator(
        task_id="pick_markets",
        python_callable=pick_markets,
        op_kwargs={"ds": "{{ ds }}"},
    )

    build = PythonOperator(
        task_id="build_channel_daily",
        python_callable=build_channel_daily,
        op_kwargs={"ds": "{{ ds }}"},
        trigger_rule="none_failed",
    )

    for _market in MARKETS:
        _load = PythonOperator(
            task_id=task_id_for(_market),
            python_callable=load_market,
            op_kwargs={"market": _market, "ds": "{{ ds }}"},
        )
        pick >> _load >> build
