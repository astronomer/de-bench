"""Supplier fill and lead time, for the fiscal week that closed.

Four numbers per supplier: how much of what we ordered arrived, how much of it
arrived in one delivery, how long it took, and how far that is from what the
supplier promised. All four come out of `int_po_receipt_matched`, which is the
three-way match finance-analytics also reads, so neither team computes it twice.

**On-time-in-full is a definition, not a table.** There is no scorecard upstream
and there is not meant to be: a supplier's own portal will tell you it delivers
on time, measured its way. This computes it from partial receipts against
expected dates, which is Copperline's way, and
`docs/semantic-definitions.md` fixes the words.

The week runs Sunday to Saturday and comes from `raw.fiscal_calendar`. The run
is on Tuesday, because the receipts for a week are not all posted until the
Monday after it.

Produces `marts.supplier_scorecard_weekly`. Read by merchandising, by the
quarterly supplier reviews and by `sc_backorder_daily`.

Owned by supply-chain (K. Duffy).
"""

from __future__ import annotations

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from include.lib import calendar, warehouse
from include.lib.notify import notify

DEFAULT_ARGS = {
    "owner": "supply",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=10),
}

#: How long after a week closes the receipts for it are all posted.
RECEIPT_LAG_DAYS = 3

FILL = """
CREATE OR REPLACE TABLE copperline.staging.supplier_fill AS
SELECT m.supplier_id,
       sum(m.ordered_units)   AS ordered_units,
       sum(m.received_units)  AS received_units,
       count(*)               AS order_lines,
       count(*) FILTER (WHERE m.received_units >= m.ordered_units) AS full_lines
FROM copperline.marts.int_po_receipt_matched m
WHERE m.ordered_on >= ?::DATE AND m.ordered_on < ?::DATE + 7
GROUP BY m.supplier_id
"""

LEAD_TIME = """
CREATE OR REPLACE TABLE copperline.staging.supplier_lead_time AS
SELECT m.supplier_id,
       median(date_diff('day', m.ordered_on, m.received_on))::INTEGER
           AS lead_time_days,
       median(date_diff('day', m.expected_on, m.received_on))::INTEGER
           AS days_late
FROM copperline.marts.int_po_receipt_matched m
WHERE m.ordered_on >= ?::DATE AND m.ordered_on < ?::DATE + 7
  AND m.received_on IS NOT NULL
GROUP BY m.supplier_id
"""

PUBLISH = """
INSERT INTO copperline.marts.supplier_scorecard_weekly BY NAME
SELECT ?::DATE AS week_start, f.supplier_id,
       f.ordered_units, f.received_units,
       CASE WHEN f.ordered_units > 0
            THEN round(f.received_units * 10000.0 / f.ordered_units)::INTEGER
       END AS fill_rate_bps,
       CASE WHEN f.order_lines > 0
            THEN round(f.full_lines * 10000.0 / f.order_lines)::INTEGER
       END AS in_full_bps,
       l.lead_time_days, l.days_late
FROM copperline.staging.supplier_fill f
LEFT JOIN copperline.staging.supplier_lead_time l ON l.supplier_id = f.supplier_id
"""


def week_covered(ds: str) -> str:
    """The Sunday that opened the week this run covers."""
    closed = pendulum.parse(ds).subtract(days=RECEIPT_LAG_DAYS).date()
    return calendar.week_start(closed).isoformat()


def build_fill(ds: str) -> int:
    week = week_covered(ds)
    with warehouse.connect() as con:
        return int(con.execute(FILL, [week, week]).fetchone()[0])


def build_lead_time(ds: str) -> int:
    week = week_covered(ds)
    with warehouse.connect() as con:
        return int(con.execute(LEAD_TIME, [week, week]).fetchone()[0])


def publish(ds: str) -> int:
    """Replace the covered week's rows."""
    week = week_covered(ds)
    with warehouse.connect() as con:
        con.execute(
            f"DELETE FROM {warehouse.qualify('marts.supplier_scorecard_weekly')} "
            "WHERE week_start = ?", [week],
        )
        return int(con.execute(PUBLISH, [week]).fetchone()[0])


def check_denominators(ds: str) -> int:
    """A supplier we ordered nothing from has no fill rate, and says so.

    The alternative is a division that returns an infinity rather than an error,
    and a supplier that reads as perfect while it supplied nothing.
    """
    week = week_covered(ds)
    with warehouse.connect(read_only=True) as con:
        bad = int(con.execute(
            f"SELECT count(*) FROM "
            f"{warehouse.qualify('marts.supplier_scorecard_weekly')} "
            "WHERE week_start = ?::DATE AND ordered_units = 0 "
            "AND fill_rate_bps IS NOT NULL", [week],
        ).fetchone()[0])
    if bad:
        raise ValueError(
            f"week of {week}: {bad} suppliers carry a fill rate with nothing "
            "ordered behind it"
        )
    return bad


def check_supplier_coverage(ds: str) -> int:
    """Every supplier with a receipt in the week has a scorecard row."""
    week = week_covered(ds)
    with warehouse.connect(read_only=True) as con:
        missing = int(con.execute(
            "SELECT count(*) FROM ("
            f"  SELECT DISTINCT supplier_id FROM "
            f"  {warehouse.qualify('staging.supplier_fill')}"
            "  EXCEPT"
            f"  SELECT supplier_id FROM "
            f"  {warehouse.qualify('marts.supplier_scorecard_weekly')}"
            "   WHERE week_start = ?::DATE)", [week],
        ).fetchone()[0])
    if missing:
        raise ValueError(f"week of {week}: {missing} suppliers did not publish")
    return missing


def report_worst_suppliers(ds: str) -> list[str]:
    """The five that filled worst, for the merchandising note."""
    week = week_covered(ds)
    with warehouse.connect(read_only=True) as con:
        rows = con.execute(
            f"SELECT supplier_id, fill_rate_bps FROM "
            f"{warehouse.qualify('marts.supplier_scorecard_weekly')} "
            "WHERE week_start = ?::DATE AND fill_rate_bps IS NOT NULL "
            "ORDER BY fill_rate_bps LIMIT 5", [week],
        ).fetchall()
    return [f"{supplier}: {rate / 100:.1f}%" for supplier, rate in rows]


with DAG(
    dag_id="sc_supplier_scorecard_weekly",
    schedule="0 7 * * 2",
    start_date=pendulum.datetime(2024, 2, 4, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["supply", "mart", "suppliers"],
    doc_md=__doc__,
    on_failure_callback=notify("supply", "supplier scorecard"),
) as dag:
    fill = PythonOperator(
        task_id="build_fill", python_callable=build_fill,
        op_kwargs={"ds": "{{ ds }}"})
    lead = PythonOperator(
        task_id="build_lead_time", python_callable=build_lead_time,
        op_kwargs={"ds": "{{ ds }}"})
    write = PythonOperator(
        task_id="publish_scorecard", python_callable=publish,
        op_kwargs={"ds": "{{ ds }}"})
    denominators = PythonOperator(
        task_id="check_denominators", python_callable=check_denominators,
        op_kwargs={"ds": "{{ ds }}"})
    coverage = PythonOperator(
        task_id="check_supplier_coverage", python_callable=check_supplier_coverage,
        op_kwargs={"ds": "{{ ds }}"})
    worst = PythonOperator(
        task_id="report_worst_suppliers", python_callable=report_worst_suppliers,
        op_kwargs={"ds": "{{ ds }}"})

    [fill, lead] >> write >> denominators >> coverage >> worst
