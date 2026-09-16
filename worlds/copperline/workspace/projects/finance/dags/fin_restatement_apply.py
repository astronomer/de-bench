"""Apply a restatement to a month that has already closed.

Triggered by hand, with a fiscal month, an entity, an amount in cents and a
reason. It is the only sanctioned way to touch a closed month, and it does not
change one: §REV-8 says a closed month is final, so the amount books on the 1st
day of the earliest open fiscal month, carrying its own reason, and the closed
month is left as it was published.

Everything it does is written to `ops.restatements`, which is the record the
controller's office reads at the next close and the auditor reads at year end.
A restatement with no reason is refused.

The memo that started the practice is `docs/memos/fy26-cost-restatement.md`.
`docs/change-management.md` says who signs one off.

Owned by finance-analytics.
"""

from __future__ import annotations

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, Param

from include.lib import warehouse
from include.lib.notify import notify
from projects.finance.lib import close

TABLE = "ops.restatements"
MONTHLY = "marts.revenue_recognized_monthly"
DAILY = "marts.revenue_recognized_daily"

DEFAULT_ARGS = {
    "owner": "finance",
    "retries": 0,
    "on_failure_callback": notify("finance", "a restatement did not apply"),
}


def resolve(params: dict, run_ds: str) -> dict:
    """Work out which month the amount actually books into.

    The requested month is where the amount belongs. The booking month is
    where it is allowed to go: the requested month if it is still open, and
    otherwise the earliest open month, which is the one the run fires in.
    """
    requested = str(params["fiscal_month"])
    with warehouse.connect(read_only=True) as con:
        is_closed = con.execute(
            f"SELECT coalesce(bool_or(is_closed), false) FROM "
            f"{warehouse.qualify(MONTHLY)} WHERE fiscal_month = ?",
            [requested],
        ).fetchone()[0]
    booking = requested if not is_closed else close.closing_month(run_ds)
    return {
        "requested_month": requested,
        "booking_month": booking,
        "requested_month_closed": bool(is_closed),
        "entity": str(params["entity"]),
        "amount_cents": int(params["amount_cents"]),
        "reason": str(params["reason"]),
        "requested_by": str(params["requested_by"]),
    }


def refuse_without_reason(request: dict) -> dict:
    """A restatement carries a reason, an entity we bill through, and cents.

    The three checks are here rather than in the param schema because the
    message matters: whoever triggered this is in the middle of a close and a
    schema error tells them nothing about what to type instead.
    """
    if not request["reason"].strip():
        raise ValueError("a restatement needs a reason; it is read at the next close")
    if request["entity"] not in close.ENTITIES:
        raise ValueError(
            f"{request['entity']} is not one of {', '.join(close.ENTITIES)}"
        )
    if request["amount_cents"] == 0:
        raise ValueError("a restatement of zero cents is not a restatement")
    return request


def record(request: dict, run_id: str) -> int:
    """Write the restatement to `ops.restatements`. Returns the rows written.

    The record comes before the booking. A restatement that was recorded and
    not booked is a conversation; one that was booked and not recorded is a
    number nobody can explain a year later.
    """
    target = warehouse.qualify(TABLE)
    with warehouse.connect() as con:
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {warehouse.qualify('ops')}")
        con.execute(
            f"""CREATE TABLE IF NOT EXISTS {target} (
                run_id VARCHAR, requested_month VARCHAR, booking_month VARCHAR,
                requested_month_closed BOOLEAN, entity VARCHAR,
                amount_cents BIGINT, reason VARCHAR, requested_by VARCHAR)"""
        )
        con.execute(
            f"INSERT INTO {target} VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [run_id, request["requested_month"], request["booking_month"],
             request["requested_month_closed"], request["entity"],
             request["amount_cents"], request["reason"], request["requested_by"]],
        )
    return 1


def book(request: dict) -> dict:
    """Book the amount into the booking month, as one dated row.

    It is one row on the 1st of the booking month, with its own reason, and it
    does not touch the closed month's rows. That is §REV-8's instruction, word
    for word, and it is why a restated figure and a published figure can differ
    and both be right.
    """
    day = _first_day(request["booking_month"])
    with warehouse.connect() as con:
        con.execute(
            f"""INSERT INTO {warehouse.qualify(DAILY)}
                (ds, fiscal_month, entity, recognized_cents, source)
                VALUES (?, ?, ?, ?, ?)""",
            [day, request["booking_month"], request["entity"],
             request["amount_cents"], f"restatement: {request['reason']}"],
        )
    return {"booked_on": day, **request}


def month_to_retie(request: dict) -> str:
    """The month whose tie has moved, for whoever reads the log.

    `fin_ledger_tie` runs at 08:00 and will pick it up. It is not run here:
    the tie holds the warehouse writer, and a restatement is usually applied in
    the middle of a close.
    """
    return request["booking_month"]


def _first_day(fiscal_month: str) -> str:
    """The 1st day of a fiscal month, from `raw.fiscal_calendar`."""
    with warehouse.connect(read_only=True) as con:
        row = con.execute(
            f"""SELECT min(cal_date) FROM {warehouse.qualify('raw.fiscal_calendar')}
                WHERE fiscal_year || '-P' || lpad(fiscal_period::VARCHAR, 2, '0') = ?""",
            [fiscal_month],
        ).fetchone()
    if not row or row[0] is None:
        raise ValueError(f"{fiscal_month} is not a fiscal month the calendar holds")
    return str(row[0])


with DAG(
    dag_id="fin_restatement_apply",
    schedule=None,
    start_date=pendulum.datetime(2024, 2, 4, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params={
        "fiscal_month": Param(type="string",
                              description="the month the amount belongs to, FY2026-P04"),
        "entity": Param(type="string", enum=list(close.ENTITIES),
                        description="the legal entity"),
        "amount_cents": Param(type="integer",
                              description="the amount, in cents. Negative is a reduction."),
        "reason": Param(type="string",
                        description="why. It is read at the next close and at year end."),
        "requested_by": Param(type="string", description="who signed it off"),
    },
    tags=["finance", "close", "restatement"],
    doc_md=__doc__,
) as dag:
    resolved = PythonOperator(
        task_id="resolve",
        python_callable=resolve,
        op_kwargs={"run_ds": "{{ ds }}"},
    )

    checked = PythonOperator(
        task_id="refuse_without_reason",
        python_callable=refuse_without_reason,
        op_kwargs={"request": resolved.output},
    )

    recorded = PythonOperator(
        task_id="record",
        python_callable=record,
        op_kwargs={"request": checked.output},
        pool="warehouse_write",
    )

    booked = PythonOperator(
        task_id="book",
        python_callable=book,
        op_kwargs={"request": checked.output},
        pool="warehouse_write",
    )

    tied = PythonOperator(
        task_id="retie",
        python_callable=month_to_retie,
        op_kwargs={"request": checked.output},
    )

    resolved >> checked >> recorded >> booked >> tied
