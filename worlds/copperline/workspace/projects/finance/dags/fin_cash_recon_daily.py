"""The bank file against what the processors say they settled.

Three numbers have to agree every morning: what the bank received, what the
processor's settlement feed says it sent, and what the weekly settlement mart
holds. This lands the bank file, compares the three, and writes the exceptions
to `ops.cash_exceptions`.

Two rules from `docs/reconciliation-policy.md` do the deciding, and neither is
this DAG's to change.

- **§R-1**: on payment state, the processor wins. The bank says money moved;
  the processor says what it was for.
- **§R-4**: age is measured on the processor's `event_time`, never on our
  `loaded_at`. A disagreement whose event is less than 48 hours old is
  `pending_sync` and is not an exception, however long ago we loaded it — and
  a disagreement whose event is 48 hours old or more is an exception, however
  recently we loaded it. Forty-eight hours is inside.

Owned by finance-analytics.
"""

from __future__ import annotations

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.sdk import DAG

from include.lib import warehouse, workspace_root
from include.lib.loaders import CsvToWarehouseOperator
from include.lib.notify import notify

#: The day this run reconciles. See `CONVENTIONS.md`.
TARGET_DS = "{{ macros.ds_add(ds, -1) }}"

BANK_FILE = "landing/bank/dt={{ macros.ds_add(ds, -1) }}/statement.csv"
BANK_TABLE = "raw.bank_statement_lines"
SETTLEMENTS = "raw.pay_meridian_settlements"
EXCEPTIONS = "ops.cash_exceptions"

#: §R-4. Inclusive: exactly 48 hours old is an exception.
PENDING_SYNC_HOURS = 48

DEFAULT_ARGS = {
    "owner": "finance",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=10),
    "on_failure_callback": notify("finance", "cash reconciliation did not run"),
}


def compare(target_ds: str) -> list[dict]:
    """One row per settlement reference the three sources disagree about.

    The join is on `payment_intent_id`, which is the durable key. `order_ref`
    is Copperline's reference, the processor recycles it across retry attempts
    inside a 24-hour window, and a join on it matches about 96% of rows and is
    wrong — `docs/billing-integration.md` §B-6.
    """
    with warehouse.connect(read_only=True) as con:
        rows = con.execute(
            f"""SELECT s.payment_intent_id,
                       s.net_amount_cents                       AS processor_cents,
                       b.amount_cents                           AS bank_cents,
                       coalesce(s.net_amount_cents, 0) - coalesce(b.amount_cents, 0)
                                                                AS difference_cents,
                       date_diff('hour', s.event_time, DATE ? + INTERVAL 1 DAY)
                                                                AS event_age_hours
                FROM {warehouse.qualify(SETTLEMENTS)} s
                FULL OUTER JOIN {warehouse.qualify(BANK_TABLE)} b
                             ON b.payment_intent_id = s.payment_intent_id
                            AND b.ds = DATE ?
                WHERE s.settled_at::DATE = DATE ?
                  AND coalesce(s.net_amount_cents, 0) <> coalesce(b.amount_cents, 0)""",
            [target_ds, target_ds, target_ds],
        ).fetchall()
    return [
        {"ds": target_ds, "payment_intent_id": intent,
         "processor_cents": processor, "bank_cents": bank,
         "difference_cents": difference,
         "exception_kind": "pending_sync" if (age or 0) < PENDING_SYNC_HOURS
                           else "amount_mismatch"}
        for intent, processor, bank, difference, age in rows
    ]


def write_exceptions(rows: list[dict], target_ds: str) -> int:
    """Replace the day's exceptions. Returns the rows written."""
    return warehouse.delete_insert(
        EXCEPTIONS, "ds", target_ds, rows,
        columns=["ds", "payment_intent_id", "processor_cents", "bank_cents",
                 "difference_cents", "exception_kind"],
    )


def report(rows: list[dict]) -> int:
    """Fail on a real mismatch. `pending_sync` is not one.

    A settlement whose processor event is younger than 48 hours has not
    finished arriving, and treating it as a break is how a reconciliation
    becomes something people mute.
    """
    real = [row for row in rows if row["exception_kind"] != "pending_sync"]
    if real:
        total = sum(abs(row["difference_cents"] or 0) for row in real)
        raise ValueError(
            f"{len(real)} settlement(s) disagree with the bank, {total} cents apart"
        )
    return len(rows)


with DAG(
    dag_id="fin_cash_recon_daily",
    schedule="30 8 * * *",
    start_date=pendulum.datetime(2024, 2, 4, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["finance", "cash", "reconciliation"],
    doc_md=__doc__,
) as dag:
    wait_for_bank_file = FileSensor(
        task_id="wait_for_bank_file",
        filepath=str(workspace_root() / BANK_FILE),
        poke_interval=300,
        timeout=60 * 60 * 4,
        mode="reschedule",
        doc_md="The bank drops the statement between 06:00 and 08:00 and has "
               "been as late as 09:30 twice. Four hours, then say so.",
    )

    land_bank_file = CsvToWarehouseOperator(
        task_id="land_bank_file",
        source=BANK_FILE,
        table=BANK_TABLE,
        mode="replace",
        partition_col="ds",
        partition_value=TARGET_DS,
        pool="warehouse_write",
        doc_md="Replace the day's statement lines. Both arguments together: "
               "`mode=replace` on its own falls back to append, and a statement "
               "landed twice reconciles against itself.",
    )

    differences = PythonOperator(
        task_id="compare",
        python_callable=compare,
        op_kwargs={"target_ds": TARGET_DS},
    )

    write = PythonOperator(
        task_id="write_exceptions",
        python_callable=write_exceptions,
        op_kwargs={"rows": differences.output, "target_ds": TARGET_DS},
        pool="warehouse_write",
    )

    summarise = PythonOperator(
        task_id="report",
        python_callable=report,
        op_kwargs={"rows": differences.output},
    )

    wait_for_bank_file >> land_bank_file >> differences >> write >> summarise
