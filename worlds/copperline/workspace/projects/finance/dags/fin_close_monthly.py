"""The month-end close pack, and the tie that has to hold before it ships.

Runs on the 1st. It closes the fiscal month that has just ended: builds the
recognition models, checks the daily rows are behind the monthly figure, ties
every entity to `raw.finance_ledger`, and writes
`exports/close/<fiscal_month>.csv` for the controller's office.

**The month is not final on the 1st.** §REV-8 closes a month on the 5th
business day of the following month, and the file is due on the 6th. What runs
today is the draft the close team works from, and the tie is the thing they
work: a break found on the 1st is a day's work and a break found on the 5th is
a late close.

`marts.revenue_recognized_daily` and `marts.revenue_recognized_monthly` are the
two models this is built from. Neither is built yet — `contracts/finance-close.md`
names them both and `projects/finance/README.md` has the open item — so the run
fails at the first check every month and the close is assembled by hand.

Owned by finance-analytics. C-2 in `docs/report-registry.md`.
"""

from __future__ import annotations

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from include.lib import workspace_root
from include.lib.notify import notify
from projects.finance.lib import close, ledger, warehouse_checks

DBT_ROOT = workspace_root() / "dbt" / "copperline_analytics"

#: The fiscal month this run closes, and the day the month is final.
FISCAL_MONTH = "{{ task_instance.xcom_pull(task_ids='resolve_month') }}"

DAILY = "marts.revenue_recognized_daily"
MONTHLY = "marts.revenue_recognized_monthly"
LEDGER = "raw.finance_ledger"
BREAKS = "ops.tie_breaks"

DEFAULT_ARGS = {
    "owner": "finance",
    "retries": 1,
    "retry_delay": pendulum.duration(minutes=15),
    "on_failure_callback": notify("finance", "the close pack did not build"),
}

with DAG(
    dag_id="fin_close_monthly",
    schedule="0 7 1 * *",
    start_date=pendulum.datetime(2024, 2, 4, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["finance", "close", "ledger"],
    doc_md=__doc__,
) as dag:
    resolve_month = PythonOperator(
        task_id="resolve_month",
        python_callable=close.closing_month,
        op_kwargs={"run_ds": "{{ ds }}"},
        doc_md="Which fiscal month this run closes. Read from the shipped 4-5-4 "
               "calendar, never computed: a fiscal month is not a calendar "
               "month, whatever a particular year makes it look like.",
    )

    build_recognition = BashOperator(
        task_id="build_recognition",
        bash_command=(
            "/opt/dbt-venv/bin/dbt run --target prod --profiles-dir . "
            "--select tag:recognition"
        ),
        cwd=str(DBT_ROOT),
        pool="warehouse_write",
        doc_md="Build the recognition models for the month. One opaque "
               "invocation on purpose: the close wants all of them or none of "
               "them, and a half-built month is worse than an unbuilt one.",
    )

    check_daily = PythonOperator(
        task_id="check_daily",
        python_callable=close.recognised_daily,
        op_kwargs={"fiscal_month": FISCAL_MONTH, "table": DAILY},
        doc_md="The daily rows the monthly figure is summed from. A month with "
               "a monthly figure and nothing behind it is a figure nobody can "
               "take apart when the tie fails.",
    )

    check_cents = PythonOperator(
        task_id="check_cents",
        python_callable=warehouse_checks.assert_integer_cents,
        op_kwargs={"table": MONTHLY, "columns": ["recognized_cents"]},
        doc_md="Money is an integer number of cents, and the tie is to the "
               "cent, so a float here cannot tie by construction.",
    )

    monthly = PythonOperator(
        task_id="monthly",
        python_callable=close.recognised_monthly,
        op_kwargs={"fiscal_month": FISCAL_MONTH, "table": MONTHLY},
        doc_md="The month's figure per entity, five rows, zeros included. An "
               "entity that lands nothing shows as a zero rather than as an "
               "absence.",
    )

    tie = PythonOperator(
        task_id="tie",
        python_callable=ledger.tie_to_ledger,
        op_kwargs={"table": MONTHLY, "column": "recognized_cents",
                   "ledger": LEDGER, "fiscal_month": FISCAL_MONTH},
        doc_md="Every entity against the ledger, to the cent, with no "
               "tolerance. The daily tie has a floor for the pager; the close "
               "does not.",
    )

    write_breaks = PythonOperator(
        task_id="write_breaks",
        python_callable=ledger.write_breaks,
        op_kwargs={"rows": tie.output, "table": BREAKS,
                   "fiscal_month": FISCAL_MONTH},
        pool="warehouse_write",
        doc_md="The month's tie, per entity, whether or not anything broke.",
    )

    write_close_file = PythonOperator(
        task_id="write_close_file",
        python_callable=close.write_close_file,
        op_kwargs={"rows": monthly.output, "ties": tie.output,
                   "fiscal_month": FISCAL_MONTH},
        doc_md="`exports/close/<fiscal_month>.csv`, with the ledger and the "
               "difference beside each figure. The controller's office reads "
               "the difference column first.",
    )

    verify_close_file = PythonOperator(
        task_id="verify_close_file",
        python_callable=close.verify_close_file,
        op_kwargs={"path": write_close_file.output, "fiscal_month": FISCAL_MONTH},
        doc_md="Five entity lines and the right month on every one. It has "
               "caught both ways this file has gone wrong: a month written into "
               "the previous month's file, and an entity dropped by a join.",
    )

    resolve_month >> build_recognition
    build_recognition >> [check_daily, check_cents] >> monthly >> tie
    tie >> write_breaks >> write_close_file >> verify_close_file
