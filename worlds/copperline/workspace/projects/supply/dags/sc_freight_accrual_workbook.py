"""The monthly freight accrual, from the analyst's workbook and the cost mart.

An analyst in supply finance keeps a workbook of freight commitments the
warehouse does not know about — quotes agreed but not yet shipped, and the
adjustments the carriers have promised. `plat_workbook_inbox` picks it up out of
`landing/finance/` and hands it here. This DAG reads it beside
`marts.fct_shipping_costs` and publishes the accrual finance books.

`docs/memos/fy26-cost-restatement.md` says what the workbook's `freight_cost`
column means on each side of the FY2026 boundary, and it is not the same thing
on both sides. `docs/rate-policy.md` rates packages; it does not say what the
workbook's column means.

**The wait is on a name.** The cost build writes one file per carrier and this
waits for them by pattern rather than by task, because the two DAGs are not
wired together and never have been. Rename the cost export or move its layer and
this sits in `running` until it times out.

Produces `marts.freight_accrual_monthly`. Read by `fin_accrual_freight`.

Owned by supply-chain (K. Duffy).
"""

from __future__ import annotations

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.sdk import DAG

from include.lib import calendar, warehouse
from include.lib.notify import notify
from projects.supply.lib import paths

DEFAULT_ARGS = {
    "owner": "supply",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=15),
}

#: The workbook, as `plat_workbook_inbox` leaves it.
WORKBOOK = "landing/finance/logistics_cost_workbook_fy26q1.csv"

ACCRUAL = """
-- The month's rated cost, plus what the workbook says is committed and not yet
-- shipped. Both in integer cents; the workbook's column is parsed at the edge
-- and nothing downstream of it carries a float.
INSERT INTO copperline.marts.freight_accrual_monthly BY NAME
SELECT ?::VARCHAR                                       AS fiscal_period,
       c.carrier_code,
       coalesce(sum(c.rated_cents), 0)                  AS rated_cents,
       coalesce(max(w.committed_cents), 0)              AS committed_cents,
       coalesce(sum(c.rated_cents), 0)
           + coalesce(max(w.committed_cents), 0)        AS accrual_cents
FROM copperline.marts.fct_shipping_costs c
LEFT JOIN copperline.staging.freight_workbook w
       ON w.carrier_code = c.carrier_code AND w.fiscal_period = ?::VARCHAR
WHERE c.ds >= ?::DATE AND c.ds <= ?::DATE
GROUP BY c.carrier_code
"""

WORKBOOK_LOAD = """
CREATE OR REPLACE TABLE copperline.staging.freight_workbook AS
SELECT carrier_code, fiscal_period,
       round(freight_cost * 100)::BIGINT AS committed_cents
FROM read_csv(?, header = true, union_by_name = true)
"""


def period_bounds(ds: str) -> tuple[str, str, str]:
    """(fiscal period, first day, last day) for the period `ds` falls in."""
    row = calendar.fiscal(ds)
    period = f"{row['fiscal_year']}-P{int(row['fiscal_period']):02d}"
    return period, str(row["week_start"]), ds


def load_workbook(ds: str) -> int:
    """Read the analyst's workbook into a staging table.

    The workbook is the one file in the world that carries money as a decimal
    rather than as integer cents, and it is converted here, once, at the edge.
    """
    with warehouse.connect() as con:
        return int(con.execute(WORKBOOK_LOAD, [WORKBOOK]).fetchone()[0])


def build_accrual(ds: str) -> int:
    """Replace the period's rows in `marts.freight_accrual_monthly`."""
    period, first_day, last_day = period_bounds(ds)
    with warehouse.connect() as con:
        con.execute(
            f"DELETE FROM {warehouse.qualify('marts.freight_accrual_monthly')} "
            "WHERE fiscal_period = ?", [period],
        )
        return int(con.execute(
            ACCRUAL, [period, period, first_day, last_day],
        ).fetchone()[0])


def check_carriers_covered(ds: str) -> int:
    """Every carrier that shipped in the period has an accrual row."""
    period, first_day, last_day = period_bounds(ds)
    with warehouse.connect(read_only=True) as con:
        missing = int(con.execute(
            "SELECT count(*) FROM ("
            f"  SELECT DISTINCT carrier_code FROM "
            f"  {warehouse.qualify('marts.fct_shipping_costs')} "
            "   WHERE ds >= ?::DATE AND ds <= ?::DATE"
            "  EXCEPT"
            f"  SELECT carrier_code FROM "
            f"  {warehouse.qualify('marts.freight_accrual_monthly')} "
            "   WHERE fiscal_period = ?)",
            [first_day, last_day, period],
        ).fetchone()[0])
    if missing:
        raise ValueError(
            f"{period}: {missing} carriers shipped and have no accrual row"
        )
    return missing


def report_accrual(ds: str) -> list[str]:
    """The period's accrual by carrier, for the note finance reads."""
    period, _, _ = period_bounds(ds)
    with warehouse.connect(read_only=True) as con:
        rows = con.execute(
            f"SELECT carrier_code, accrual_cents FROM "
            f"{warehouse.qualify('marts.freight_accrual_monthly')} "
            "WHERE fiscal_period = ? ORDER BY carrier_code", [period],
        ).fetchall()
    return [f"{carrier}: {cents} cents" for carrier, cents in rows]


with DAG(
    dag_id="sc_freight_accrual_workbook",
    schedule="0 6 * * *",
    start_date=pendulum.datetime(2024, 2, 4, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["supply", "mart", "freight", "finance"],
    doc_md=__doc__,
    on_failure_callback=notify("supply", "freight accrual"),
) as dag:
    wait_for_costs = FileSensor(
        task_id="wait_for_costs",
        filepath=paths.under_root(paths.SHIPPING_COST_GLOB),
        poke_interval=600,
        timeout=60 * 60 * 4,
        mode="reschedule",
    )

    workbook = PythonOperator(
        task_id="load_workbook", python_callable=load_workbook,
        op_kwargs={"ds": "{{ ds }}"})
    accrual = PythonOperator(
        task_id="build_accrual", python_callable=build_accrual,
        op_kwargs={"ds": "{{ ds }}"})
    covered = PythonOperator(
        task_id="check_carriers_covered", python_callable=check_carriers_covered,
        op_kwargs={"ds": "{{ ds }}"})
    report = PythonOperator(
        task_id="report_accrual", python_callable=report_accrual,
        op_kwargs={"ds": "{{ ds }}"})

    wait_for_costs >> workbook >> accrual >> covered >> report
