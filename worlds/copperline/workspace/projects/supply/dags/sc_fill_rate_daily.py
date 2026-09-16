"""Shipped over ordered, by lane and day.

On-time-in-full is computed from partial receipts against expected dates. There
is no scorecard table upstream and there is not meant to be: the number is a
definition, and a definition that arrives ready-made is somebody else's.

The same computation is finance-analytics' three-way match, so it sits in
`int_po_receipt_matched` and neither team rebuilds it. This DAG reads that model
and rolls it to lane and day.

A line ordered on one day and shipped on another belongs to the day it was
ordered, because a fill rate is about the promise and not about the shipment.
That is why the run's own day is the ordered day, and why the read reaches
forward over the fulfilment window rather than back.

Produces `marts.lane_fill_daily`. Read by the supplier scorecard, the lane
performance build and the merch weekly.

Owned by supply-chain (K. Duffy).
"""

from __future__ import annotations

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from include.lib import warehouse
from include.lib.notify import notify

DEFAULT_ARGS = {
    "owner": "supply",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=10),
}

#: How far forward a line ordered today may still be shipped and counted. The
#: measured fulfilment tail, not a round number and not copied from another
#: model. Re-measure it when the DC network changes.
FULFILMENT_WINDOW_DAYS = 21

ORDERED = """
CREATE OR REPLACE TABLE copperline.staging.fill_ordered AS
SELECT m.lane_id, m.ordered_on AS ds,
       sum(m.ordered_units)  AS ordered_units,
       sum(m.received_units) AS received_units
FROM copperline.marts.int_po_receipt_matched m
WHERE m.ordered_on = ?::DATE
  AND m.received_on <= ?::DATE + CAST(? AS INTEGER)
GROUP BY m.lane_id, m.ordered_on
"""

PUBLISH = """
-- Shipped over ordered. Integer division is not what is wanted here and a
-- zero denominator is not an infinity: a lane that ordered nothing has no fill
-- rate, and the column is null rather than a number.
INSERT INTO copperline.marts.lane_fill_daily BY NAME
SELECT ds, lane_id, ordered_units, received_units,
       CASE WHEN ordered_units > 0
            THEN round(received_units * 10000.0 / ordered_units)::INTEGER
       END AS fill_rate_bps
FROM copperline.staging.fill_ordered
WHERE ds = ?::DATE
"""

DENOMINATOR = """
SELECT count(*) FROM copperline.marts.lane_fill_daily
WHERE ds = ?::DATE AND ordered_units = 0 AND fill_rate_bps IS NOT NULL
"""


def build_ordered(ds: str) -> int:
    """The day's ordered and received units, by lane."""
    with warehouse.connect() as con:
        return int(con.execute(
            ORDERED, [ds, ds, FULFILMENT_WINDOW_DAYS],
        ).fetchone()[0])


def publish(ds: str) -> int:
    """Replace the day's partition of `marts.lane_fill_daily`."""
    with warehouse.connect() as con:
        con.execute(f"DELETE FROM {warehouse.qualify('marts.lane_fill_daily')} "
                    "WHERE ds = ?", [ds])
        return int(con.execute(PUBLISH, [ds]).fetchone()[0])


def check_denominator(ds: str) -> int:
    """A lane that ordered nothing has no fill rate, and the column says so.

    The alternative is a division that returns an infinity rather than an error
    and a lane that reads as perfectly filled while it ordered nothing.
    """
    with warehouse.connect(read_only=True) as con:
        bad = int(con.execute(DENOMINATOR, [ds]).fetchone()[0])
    if bad:
        raise ValueError(
            f"{ds}: {bad} lanes carry a fill rate with no units ordered behind it"
        )
    return bad


def check_lane_coverage(ds: str) -> int:
    """Every active lane with an order appears once."""
    with warehouse.connect(read_only=True) as con:
        missing = int(con.execute(
            "SELECT count(*) FROM ("
            f"  SELECT DISTINCT lane_id FROM {warehouse.qualify('staging.fill_ordered')}"
            "   WHERE ds = ?::DATE"
            "  EXCEPT"
            f"  SELECT lane_id FROM {warehouse.qualify('marts.lane_fill_daily')}"
            "   WHERE ds = ?::DATE)",
            [ds, ds],
        ).fetchone()[0])
    if missing:
        raise ValueError(f"{ds}: {missing} lanes ordered and did not publish")
    return missing


def report_worst_lanes(ds: str) -> list[str]:
    """The five lanes that filled worst, for the morning note."""
    with warehouse.connect(read_only=True) as con:
        rows = con.execute(
            f"SELECT lane_id, fill_rate_bps FROM "
            f"{warehouse.qualify('marts.lane_fill_daily')} "
            "WHERE ds = ?::DATE AND fill_rate_bps IS NOT NULL "
            "ORDER BY fill_rate_bps LIMIT 5",
            [ds],
        ).fetchall()
    return [f"{lane}: {rate / 100:.1f}%" for lane, rate in rows]


with DAG(
    dag_id="sc_fill_rate_daily",
    schedule="30 4 * * *",
    start_date=pendulum.datetime(2024, 2, 4, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["supply", "mart", "service"],
    doc_md=__doc__,
    on_failure_callback=notify("supply", "fill rate"),
) as dag:
    ordered = PythonOperator(
        task_id="build_ordered", python_callable=build_ordered,
        op_kwargs={"ds": "{{ ds }}"})
    write = PythonOperator(
        task_id="publish_fill_rate", python_callable=publish,
        op_kwargs={"ds": "{{ ds }}"})
    denominator = PythonOperator(
        task_id="check_denominator", python_callable=check_denominator,
        op_kwargs={"ds": "{{ ds }}"})
    coverage = PythonOperator(
        task_id="check_lane_coverage", python_callable=check_lane_coverage,
        op_kwargs={"ds": "{{ ds }}"})
    worst = PythonOperator(
        task_id="report_worst_lanes", python_callable=report_worst_lanes,
        op_kwargs={"ds": "{{ ds }}"})

    ordered >> write >> denominator >> coverage >> worst
