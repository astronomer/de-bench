"""Restate the FY2024 prior-year comparatives from the store day book.

The comp mart carries no prior year for FY2024. Order detail begins on
2024-02-04 and FY2023 survives only as `raw.pos_sales_daily`, the store day
summary the retention policy left behind, so the year-in-review has nothing to
put in its comparative column.

This rebuilds `marts.comp_prior_year_weekly` whole: one row per FY2024 fiscal
week per market, holding what the estate took in the comparable week of FY2023.
It is triggered by hand rather than scheduled — the year is closed and the book
behind it does not change — and it rebuilds the whole year every time, week by
week, so a rerun leaves one copy.

Owned by finance-analytics. FIN-431.
"""

from __future__ import annotations

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from include.lib.notify import notify
from projects.finance.lib import comp_history, warehouse_checks

TABLE = comp_history.TABLE
FISCAL_YEAR = comp_history.FISCAL_YEAR

DEFAULT_ARGS = {
    "owner": "finance",
    "retries": 1,
    "retry_delay": pendulum.duration(minutes=5),
    "on_failure_callback": notify("finance", "the FY2024 comparatives did not rebuild"),
}

with DAG(
    dag_id="fin_comp_restate_fy2024",
    schedule=None,
    start_date=pendulum.datetime(2026, 6, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["finance", "comp", "restatement"],
    doc_md=__doc__,
) as dag:
    build = PythonOperator(
        task_id="build",
        python_callable=comp_history.build_year,
        op_kwargs={"fiscal_year": FISCAL_YEAR, "table": TABLE},
        pool="warehouse_write",
        doc_md="Rebuild the year out of the store day book, a week at a time. "
               "The write is a delete-insert on `week_start`, so a second run "
               "leaves one copy of the year.",
    )

    check_cents = PythonOperator(
        task_id="check_cents",
        python_callable=warehouse_checks.assert_integer_cents,
        op_kwargs={"table": TABLE, "columns": ["takings_cents_ly"]},
        doc_md="Money is an integer number of cents. A float here is a defect.",
    )

    check_weeks = PythonOperator(
        task_id="check_weeks",
        python_callable=comp_history.assert_year_complete,
        op_kwargs={"fiscal_year": FISCAL_YEAR, "table": TABLE},
        doc_md="Every fiscal week of the year landed a row. A missing week "
               "reads as a flat year rather than as an absent one.",
    )

    build >> [check_cents, check_weeks]
