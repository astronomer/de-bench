"""Receivables aging: what the trade book owes, and how late it is.

Five bands — current, 1-30, 31-60, 61-90, over 90 — by entity and by trade
account, rebuilt every morning into `marts.ar_aging_daily`.

Age is measured from the invoice due date to the business date, in whole
calendar days, and the bands are closed at both ends: an invoice 30 days past
due is in the 1-30 band and an invoice 31 days past due is in the next one. The
boundary is written out because the collections team reads the band names and
two different readings of "30 days" put a hundred thousand cents in the wrong
column.

An invoice under dispute stays in its band and stays in the total. §REV-10: a
disputed amount stays recognised while the dispute is open, and a provision is
not netted against revenue.

Owned by finance-analytics.
"""

from __future__ import annotations

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.sensors.python import PythonSensor
from airflow.sdk import DAG

from include.lib import warehouse
from include.lib.notify import notify
from projects.finance.lib import warehouse_checks

#: The day this run owns. See `CONVENTIONS.md`.
TARGET_DS = "{{ macros.ds_add(ds, -1) }}"

TABLE = "marts.ar_aging_daily"
SOURCE = "marts.fct_ar_invoices"
ROLLUP = "marts.account_rollup"

#: The bands, by days past due. The upper bound is inclusive; `None` is the
#: open-ended one at the end.
BANDS = (("current", None, 0), ("1_30", 1, 30), ("31_60", 31, 60),
         ("61_90", 61, 90), ("over_90", 91, None))

DEFAULT_ARGS = {
    "owner": "finance",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
    "on_failure_callback": notify("finance", "the aging did not rebuild"),
}


def band_sql() -> str:
    """The CASE that puts an invoice in a band, built from `BANDS`.

    Built from the tuple rather than written twice, so the band names in the
    table and the band names collections talk about cannot drift apart.
    """
    whens = []
    for name, low, high in BANDS:
        if low is None:
            whens.append(f"WHEN days_past_due <= {high} THEN '{name}'")
        elif high is None:
            whens.append(f"WHEN days_past_due >= {low} THEN '{name}'")
        else:
            whens.append(
                f"WHEN days_past_due BETWEEN {low} AND {high} THEN '{name}'"
            )
    return "CASE " + " ".join(whens) + " END"


def build(target_ds: str) -> int:
    """Replace the day's aging. Returns the rows written."""
    with warehouse.connect() as con:
        rows = con.execute(
            f"""WITH aged AS (
                    SELECT entity, customer_id, outstanding_cents,
                           date_diff('day', due_date, DATE ?) AS days_past_due
                    FROM {warehouse.qualify(SOURCE)}
                    WHERE outstanding_cents > 0
                      AND invoice_date <= DATE ?
                )
                SELECT entity, customer_id, {band_sql()} AS band,
                       sum(outstanding_cents)::BIGINT AS outstanding_cents,
                       count(*)::BIGINT               AS invoice_count
                FROM aged
                GROUP BY entity, customer_id, band
                ORDER BY entity, customer_id, band""",
            [target_ds, target_ds],
        ).fetchall()
        return warehouse.delete_insert(
            TABLE, "ds", target_ds, [(target_ds, *row) for row in rows],
            columns=["ds", "entity", "customer_id", "band", "outstanding_cents",
                     "invoice_count"],
            con=con,
        )


def assert_bands(target_ds: str) -> list[str]:
    """Every band the code knows about is spelled the way the table holds it.

    A band that appears in the table and not in `BANDS` means the CASE fell
    through, which puts real money in a null band that nothing sums.
    """
    known = {name for name, _, _ in BANDS}
    with warehouse.connect(read_only=True) as con:
        found = {row[0] for row in con.execute(
            f"SELECT DISTINCT band FROM {warehouse.qualify(TABLE)} WHERE ds = ?",
            [target_ds],
        ).fetchall()}
    if None in found or not found <= known:
        raise ValueError(f"unknown aging bands for {target_ds}: {sorted(map(str, found - known))}")
    return sorted(found)


with DAG(
    dag_id="fin_ar_aging",
    schedule="0 7 * * *",
    start_date=pendulum.datetime(2024, 2, 4, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["finance", "mart", "receivables"],
    doc_md=__doc__,
) as dag:
    wait_for_invoices = PythonSensor(
        task_id="wait_for_invoices",
        python_callable=warehouse_checks.has_partition,
        op_kwargs={"table": ROLLUP, "partition_col": "fiscal_month",
                   "partition_value": "{{ macros.ds_format(macros.ds_add(ds, -1), '%Y-%m-%d', '%Y-%m') }}"},
        poke_interval=300,
        timeout=60 * 60 * 2,
        mode="reschedule",
        doc_md="The account rollup is what says which accounts are in the book "
               "this month. Two hours, then say so.",
    )

    rebuild = PythonOperator(
        task_id="build",
        python_callable=build,
        op_kwargs={"target_ds": TARGET_DS},
        pool="warehouse_write",
    )

    bands = PythonOperator(
        task_id="assert_bands",
        python_callable=assert_bands,
        op_kwargs={"target_ds": TARGET_DS},
    )

    cents = PythonOperator(
        task_id="check_cents",
        python_callable=warehouse_checks.assert_integer_cents,
        op_kwargs={"table": TABLE, "columns": ["outstanding_cents"]},
    )

    rows = PythonOperator(
        task_id="check_rows",
        python_callable=warehouse_checks.assert_row_count,
        op_kwargs={"table": TABLE, "partition_col": "ds",
                   "partition_value": TARGET_DS, "at_least": 1},
    )

    publish = PythonOperator(
        task_id="publish",
        python_callable=warehouse_checks.publish_partition,
        op_kwargs={"table": TABLE, "partition_col": "ds",
                   "partition_value": TARGET_DS, "name": "ar_aging_daily",
                   "order_by": ["entity", "customer_id", "band"]},
    )

    wait_for_invoices >> rebuild >> [bands, cents, rows] >> publish
