"""Marketplace remittance files into the settlement and payout tables.

Copperline pays sellers weekly and the remittance is written from the operator's
side, which is what every sign in it turns on. The principal is money Copperline
owes the seller, so it signs negative; commission and the fulfilment fee sign
positive; a refund signs negative against commission and lands in the period of
the refund, never in the period of the original order — including inside a
closed month.

The listing pages the same way the order listing does, and `total` is null on
every page.

Produces `raw.marketplace_settlements` and `raw.marketplace_payouts`. Read by
`payment_settlement_weekly`, `fin_cash_recon_daily` and the seller-ops summary.

Owned by commerce (J. Mwangi).
"""

from __future__ import annotations

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from include.lib import warehouse
from include.lib.loaders import JsonApiToWarehouseOperator
from include.lib.notify import notify
from projects.commerce.lib import sql

DEFAULT_ARGS = {
    "owner": "commerce",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=10),
}

SETTLEMENT_COLUMNS = {
    "settlement_id": "VARCHAR",
    "payout_id": "VARCHAR",
    "marketplace_order_id": "VARCHAR",
    "order_id": "BIGINT",
    "amount_cents": "BIGINT",
    "posted_at": "TIMESTAMP",
    "payout_date": "DATE",
    "loaded_at": "TIMESTAMP",
}

PAYOUT_COLUMNS = {
    "payout_id": "VARCHAR",
    "payout_date": "DATE",
    "principal_cents": "BIGINT",
    "commission_cents": "BIGINT",
    "fee_cents": "BIGINT",
    "net_paid_cents": "BIGINT",
}


def check_line_types(ds: str) -> int:
    """Every settlement line names a line type the summary knows how to sum.

    A type nobody recognises is dropped by every reader downstream without
    saying so, and the payout then does not tie.
    """
    with warehouse.connect(read_only=True) as con:
        unknown = con.execute(sql.read("marketplace_unknown_line_types"), [ds]).fetchall()
    if unknown:
        names = ", ".join(str(row[0]) for row in unknown)
        raise ValueError(f"{ds}: settlement lines carry unknown types: {names}")
    return 0


def check_signs(ds: str) -> int:
    """Principal signs negative and commission signs positive.

    A feed that flips a sign is loud in the payout tie and silent everywhere
    else, so it is checked here where the rows land.
    """
    with warehouse.connect(read_only=True) as con:
        wrong = int(con.execute(sql.read("marketplace_sign_breaks"), [ds]).fetchone()[0])
    if wrong:
        raise ValueError(
            f"{ds}: {wrong} settlement lines sign against the operator's "
            "convention. The principal is money out."
        )
    return wrong


def tie_lines_to_payout(ds: str) -> int:
    """The day's lines against the payout header, per payout, cent exact."""
    with warehouse.connect(read_only=True) as con:
        breaks = con.execute(sql.read("marketplace_payout_tie"), [ds]).fetchall()
    if breaks:
        names = ", ".join(str(row[0]) for row in breaks[:5])
        raise ValueError(
            f"{ds}: {len(breaks)} payouts do not tie to their lines: {names}"
        )
    return 0


def record_intake(ds: str) -> int:
    """The day's loaded counts, into `ops.intake_log`."""
    with warehouse.connect() as con:
        con.execute("CREATE SCHEMA IF NOT EXISTS ops")
        con.execute(sql.read("intake_log_ddl"))
        lines, payouts = con.execute(
            sql.read("marketplace_settlement_counts"), [ds, ds],
        ).fetchone()
        warehouse.delete_insert(
            "ops.intake_log", "ds", ds,
            [{"ds": ds, "source": "marketplace_settlements",
              "row_count": int(lines or 0)},
             {"ds": ds, "source": "marketplace_payouts",
              "row_count": int(payouts or 0)}],
            con=con,
        )
    return int(lines or 0)


with DAG(
    dag_id="marketplace_settlement_intake",
    schedule="0 7 * * *",
    start_date=pendulum.datetime(2024, 2, 4, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["commerce", "raw", "intake", "marketplace"],
    doc_md=__doc__,
    on_failure_callback=notify("commerce", "marketplace settlement intake"),
) as dag:
    load_lines = JsonApiToWarehouseOperator(
        task_id="load_settlement_lines",
        endpoint="marketplace/settlements",
        params={"payout_date": "{{ ds }}"},
        table="raw.marketplace_settlements",
        mode="replace",
        partition_col="payout_date",
        columns=SETTLEMENT_COLUMNS,
    )

    load_payouts = JsonApiToWarehouseOperator(
        task_id="load_payouts",
        endpoint="marketplace/payouts",
        params={"payout_date": "{{ ds }}"},
        table="raw.marketplace_payouts",
        mode="replace",
        partition_col="payout_date",
        columns=PAYOUT_COLUMNS,
    )

    types = PythonOperator(
        task_id="check_line_types", python_callable=check_line_types,
        op_kwargs={"ds": "{{ ds }}"})
    signs = PythonOperator(
        task_id="check_signs", python_callable=check_signs,
        op_kwargs={"ds": "{{ ds }}"})
    tie = PythonOperator(
        task_id="tie_lines_to_payout", python_callable=tie_lines_to_payout,
        op_kwargs={"ds": "{{ ds }}"})
    log_intake = PythonOperator(
        task_id="record_intake", python_callable=record_intake,
        op_kwargs={"ds": "{{ ds }}"})

    [load_lines, load_payouts] >> types >> signs >> tie >> log_intake
