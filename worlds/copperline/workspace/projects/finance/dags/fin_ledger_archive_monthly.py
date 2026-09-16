"""Freeze last month's ledger once the month has closed.

Runs on the 2nd, at 02:00, after the close DAG has had its first pass. It takes
the month's ledger rows and the published monthly figures, writes them to
`ops.ledger_archive` with the day they were frozen, and leaves them there.

The archive is what an auditor reads and what a restatement is measured
against. §REV-8 makes the closed month final; this is the copy that proves what
"final" was, because the marts behind it are rebuilt every night and a rebuilt
figure is not evidence of what was published.

`raw.finance_ledger` is exempt from retention under §RET-5 and keeps seven
years. So does this.

Owned by finance-analytics.
"""

from __future__ import annotations

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from include.lib import warehouse
from include.lib.notify import notify
from projects.finance.lib import close

TABLE = "ops.ledger_archive"
LEDGER = "raw.finance_ledger"
MONTHLY = "marts.revenue_recognized_monthly"

DEFAULT_ARGS = {
    "owner": "finance",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=15),
    "on_failure_callback": notify("finance", "last month's ledger was not frozen"),
}


def month_to_freeze(run_ds: str) -> str:
    """The fiscal month the run's previous day fell in.

    The DAG fires on the 2nd, so that is last month. Read from the shipped
    4-5-4 calendar rather than computed: a fiscal month is not a calendar
    month, and on the years it looks like one that is a coincidence.
    """
    return close.closing_month(run_ds)


def freeze(fiscal_month: str, frozen_on: str) -> int:
    """Copy the month's ledger and published figures into the archive.

    Returns the rows written. Written once: a month already in the archive is
    left as it is, because a frozen figure that can be refrozen is not frozen.
    """
    target = warehouse.qualify(TABLE)
    with warehouse.connect() as con:
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {warehouse.qualify('ops')}")
        con.execute(
            f"""CREATE TABLE IF NOT EXISTS {target} (
                fiscal_month VARCHAR NOT NULL, entity VARCHAR NOT NULL,
                ledger_cents BIGINT, published_cents BIGINT,
                frozen_on VARCHAR NOT NULL)"""
        )
        already = con.execute(
            f"SELECT count(*) FROM {target} WHERE fiscal_month = ?", [fiscal_month]
        ).fetchone()[0]
        if already:
            raise ValueError(
                f"{fiscal_month} is already in the archive, frozen on "
                + str(con.execute(
                    f"SELECT min(frozen_on) FROM {target} WHERE fiscal_month = ?",
                    [fiscal_month],
                ).fetchone()[0])
            )
        return int(con.execute(
            f"""INSERT INTO {target}
                SELECT l.fiscal_month, l.entity,
                       sum(l.amount_cents)::BIGINT,
                       max(m.recognized_cents)::BIGINT,
                       ?
                FROM {warehouse.qualify(LEDGER)} l
                LEFT JOIN {warehouse.qualify(MONTHLY)} m
                       ON m.fiscal_month = l.fiscal_month AND m.entity = l.entity
                WHERE l.fiscal_month = ?
                GROUP BY l.fiscal_month, l.entity""",
            [frozen_on, fiscal_month],
        ).fetchall()[0][0])


def verify(fiscal_month: str) -> int:
    """The archive holds a row for every entity the ledger covered."""
    with warehouse.connect(read_only=True) as con:
        archived, covered = con.execute(
            f"""SELECT (SELECT count(*) FROM {warehouse.qualify(TABLE)}
                        WHERE fiscal_month = ?),
                       (SELECT count(DISTINCT entity) FROM {warehouse.qualify(LEDGER)}
                        WHERE fiscal_month = ?)""",
            [fiscal_month, fiscal_month],
        ).fetchone()
    if archived != covered:
        raise ValueError(
            f"{fiscal_month}: {archived} archived row(s) and the ledger covers "
            f"{covered} entity(ies)"
        )
    return int(archived)


with DAG(
    dag_id="fin_ledger_archive_monthly",
    schedule="0 2 2 * *",
    start_date=pendulum.datetime(2024, 2, 4, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["finance", "ledger", "archive"],
    doc_md=__doc__,
) as dag:
    month = PythonOperator(
        task_id="month_to_freeze",
        python_callable=month_to_freeze,
        op_kwargs={"run_ds": "{{ ds }}"},
    )

    frozen = PythonOperator(
        task_id="freeze",
        python_callable=freeze,
        op_kwargs={"fiscal_month": month.output, "frozen_on": "{{ ds }}"},
        pool="warehouse_write",
    )

    checked = PythonOperator(
        task_id="verify",
        python_callable=verify,
        op_kwargs={"fiscal_month": month.output},
    )

    month >> frozen >> checked
