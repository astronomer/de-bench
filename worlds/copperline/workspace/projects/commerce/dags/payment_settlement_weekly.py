"""The Monday settlement summary: `marts.settlement_weekly`.

One row per seller per fiscal week, per `contracts/settlement_weekly.yml`. The
week is a fiscal week and starts on Sunday, so the Monday run summarises the
week that closed the day before — the run for `ds` owns the seven days ending
`ds - 1`, and the week's start comes from `raw.fiscal_calendar` rather than from
weekday arithmetic.

Two sources meet here and they overlap. Copperline's own marketplace settlement
lines are the operator's record; the processor feed is the money's. Over the
quarter when both were live the same money can appear in both, and
`docs/billing-integration.md` B-8 says which side is authoritative on each
side of the boundary.

Produces `marts.settlement_weekly` and `exports/settlement/<week>.csv`. Read by
the seller-ops summary email, `fin_cash_recon_daily` and `marts.dispute_daily`.

Owned by commerce (J. Mwangi).
"""

from __future__ import annotations

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from include.lib import calendar, warehouse
from include.lib.notify import notify
from projects.commerce.lib import sql

DEFAULT_ARGS = {
    "owner": "commerce",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=10),
}

#: Where the summary email picks the file up.
EXPORT_LAYER = "settlement"


def week_bounds(ds: str) -> tuple[str, str]:
    """The fiscal week this run summarises: the seven days ending `ds - 1`.

    The start is the calendar's own `week_start` for the last closed day, not
    `ds` minus seven. `docs/retail-calendar.md` owns the week and the 4-5-4
    pattern means a week's start is authored, not derived.
    """
    closed = pendulum.parse(ds).subtract(days=1).date()
    return calendar.week_start(closed).isoformat(), closed.isoformat()


def build_week(ds: str) -> int:
    """Assemble the week's settlement lines into the working table."""
    start, end = week_bounds(ds)
    with warehouse.connect() as con:
        return int(con.execute(sql.read("settlement_week_lines"), [start, end]).fetchone()[0])


def apply_processor_boundary(ds: str) -> int:
    """Drop the side that is not authoritative over the overlap.

    Both feeds carry the same money for the overlap quarter, so a week inside it
    is summed twice unless one side is chosen. B-8 chooses; this applies it.
    """
    start, end = week_bounds(ds)
    with warehouse.connect() as con:
        return int(con.execute(sql.read("settlement_boundary"), [start, end]).fetchone()[0])


def apply_fee_schedule(ds: str) -> int:
    """Rate each line's commission and fulfilment fee on the schedule in force
    for the week, never on today's schedule."""
    start, end = week_bounds(ds)
    with warehouse.connect() as con:
        return int(con.execute(sql.read("settlement_fees"), [start, end]).fetchone()[0])


def publish_week(ds: str) -> int:
    """Replace this week's rows in `marts.settlement_weekly`.

    The write is keyed on `week_start`, so re-running a week replaces that
    week's rows and no other's. Seven runs of one week and one run of seven
    weeks leave the same table.
    """
    start, _ = week_bounds(ds)
    with warehouse.connect() as con:
        return int(con.execute(sql.read("settlement_publish"), [start, start]).fetchone()[0])


def check_grain(ds: str) -> int:
    """One row per seller per week, per the contract."""
    start, _ = week_bounds(ds)
    with warehouse.connect(read_only=True) as con:
        rows, sellers = con.execute(sql.read("settlement_grain"), [start, start]).fetchone()
    if rows != sellers:
        raise ValueError(
            f"week of {start}: {rows} rows for {sellers} sellers. "
            "contracts/settlement_weekly.yml is one row per seller per week."
        )
    return int(rows)


def tie_to_payouts(ds: str) -> int:
    """The week's payout total against what the payout table says was paid.

    Cent exact. A difference means the fee schedule this run applied is not the
    one the payout was computed on, which is the thing seller-ops asks about.
    """
    start, _ = week_bounds(ds)
    with warehouse.connect(read_only=True) as con:
        summarised, paid = con.execute(sql.read("settlement_payout_tie"), [start, start]).fetchone()
    if summarised != paid:
        raise ValueError(
            f"week of {start}: the summary totals {summarised} cents and the "
            f"payouts total {paid}. Do not send the summary on a break."
        )
    return int(paid or 0)


def export_week(ds: str) -> str:
    """Write `exports/settlement/<week>.csv` for the summary email."""
    start, _ = week_bounds(ds)
    with warehouse.connect(read_only=True) as con:
        result = con.execute(sql.read("settlement_export"), [start])
        header = [description[0] for description in result.description]
        rows = result.fetchall()
    return str(warehouse.write_partition(EXPORT_LAYER, "settlement", start, header, rows))


with DAG(
    dag_id="payment_settlement_weekly",
    schedule="0 5 * * 1",
    start_date=pendulum.datetime(2024, 2, 4, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["commerce", "mart", "settlement"],
    doc_md=__doc__,
    on_failure_callback=notify("commerce", "weekly settlement summary"),
) as dag:
    lines = PythonOperator(
        task_id="build_week",
        python_callable=build_week,
        op_kwargs={"ds": "{{ ds }}"},
    )

    boundary = PythonOperator(
        task_id="apply_processor_boundary",
        python_callable=apply_processor_boundary,
        op_kwargs={"ds": "{{ ds }}"},
    )

    fees = PythonOperator(
        task_id="apply_fee_schedule",
        python_callable=apply_fee_schedule,
        op_kwargs={"ds": "{{ ds }}"},
    )

    publish = PythonOperator(
        task_id="publish_week",
        python_callable=publish_week,
        op_kwargs={"ds": "{{ ds }}"},
    )

    grain = PythonOperator(
        task_id="check_grain",
        python_callable=check_grain,
        op_kwargs={"ds": "{{ ds }}"},
    )

    payout_tie = PythonOperator(
        task_id="tie_to_payouts",
        python_callable=tie_to_payouts,
        op_kwargs={"ds": "{{ ds }}"},
    )

    export = PythonOperator(
        task_id="export_summary",
        python_callable=export_week,
        op_kwargs={"ds": "{{ ds }}"},
    )

    lines >> boundary >> fees >> publish >> grain >> payout_tie >> export
