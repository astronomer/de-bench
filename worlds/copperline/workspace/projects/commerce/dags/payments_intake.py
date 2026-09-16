"""Processor settlement files into the two payment tables, hourly.

Copperline takes card money through two processors and the two feeds do not look
alike. Meridian Pay sends hourly JSON lines into
`landing/meridian/dt=<ds>/hr=<hh>/events.jsonl`, partitioned on when the file
arrived rather than on when the payment happened. Halcyon, the processor
Meridian replaced, sent one CSV per merchant account per night into
`landing/halcyon/dt=<ds>/`, and stopped sending at the end of the overlap
quarter — so its half of this DAG lands nothing for a date after that and that
is correct, not a gap.

`docs/billing-integration.md` is the description of both feeds, what each field
means, and which side is authoritative over the overlap.

Produces `raw.pay_meridian_settlements` and `raw.pay_halcyon_settlements`. Read
by `payments_recon_daily`, `psp_bridge_build` and finance's cash reconciliation.

Owned by commerce (J. Mwangi).
"""

from __future__ import annotations

import pendulum
from airflow.providers.standard.operators.python import (
    PythonOperator,
    ShortCircuitOperator,
)
from airflow.sdk import DAG

from include.lib import landing_dir, warehouse
from include.lib.loaders import CsvToWarehouseOperator, JsonApiToWarehouseOperator
from include.lib.notify import notify
from projects.commerce.lib import sql

DEFAULT_ARGS = {
    "owner": "commerce",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
}

#: Halcyon's amount arrives as a two-decimal string and its timestamp as
#: merchant-local text with no zone. Both are typed as text on the way in, on
#: purpose: the parsing is a modelling decision and `stg_pay_halcyon` makes it,
#: not the loader.
HALCYON_COLUMNS = {
    "txn_id": "VARCHAR",
    "amount": "VARCHAR",
    "currency": "VARCHAR",
    "txn_datetime": "VARCHAR",
    "settled_on": "DATE",
    "file_date": "DATE",
}

MERIDIAN_COLUMNS = {
    "event_id": "VARCHAR",
    "amount_cents": "BIGINT",
    "event_time_utc": "TIMESTAMP",
    "loaded_at": "TIMESTAMP",
    "settlement_date": "DATE",
    "restates_event_id": "VARCHAR",
    "attempt_no": "INTEGER",
    "deleted_at": "TIMESTAMP",
}


def halcyon_is_still_sending(ds: str) -> bool:
    """Whether Halcyon delivered anything for the file date.

    The feed stopped at the end of the overlap quarter. A missing directory
    after that is the feed being finished, so the load is short-circuited rather
    than failed — a red square every hour for a processor that no longer exists
    is noise the rotation learns to ignore.
    """
    return (landing_dir("halcyon") / f"dt={ds}").is_dir()


def check_meridian_events_unique(ds: str) -> int:
    """`event_id` is Meridian's idempotency key and it is unique per event.

    A repeat means the same hour's file was taken twice, which the delete-insert
    should already have prevented; if it did not, the recon totals double and
    nothing else says so.
    """
    with warehouse.connect(read_only=True) as con:
        repeats = con.execute(sql.read("meridian_duplicate_events"), [ds]).fetchall()
    if repeats:
        names = ", ".join(str(row[0]) for row in repeats[:5])
        raise ValueError(f"{ds}: Meridian event ids repeat: {names}")
    return 0


def record_intake(ds: str) -> int:
    """Both feeds' loaded counts for the day, into `ops.intake_log`."""
    with warehouse.connect() as con:
        con.execute("CREATE SCHEMA IF NOT EXISTS ops")
        con.execute(sql.read("intake_log_ddl"))
        rows = con.execute(sql.read("payments_day_counts"), [ds, ds]).fetchone()
        warehouse.delete_insert(
            "ops.intake_log", "ds", ds,
            [{"ds": ds, "source": "meridian", "row_count": int(rows[0] or 0)},
             {"ds": ds, "source": "halcyon", "row_count": int(rows[1] or 0)}],
            con=con,
        )
    return int(rows[0] or 0) + int(rows[1] or 0)


with DAG(
    dag_id="payments_intake",
    schedule="40 * * * *",
    start_date=pendulum.datetime(2024, 2, 4, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["commerce", "raw", "intake", "payments"],
    doc_md=__doc__,
    on_failure_callback=notify("commerce", "payments intake"),
) as dag:
    load_meridian = JsonApiToWarehouseOperator(
        task_id="load_meridian_events",
        source="landing/meridian/dt={{ ds }}/hr=*/events.jsonl",
        table="raw.pay_meridian_settlements",
        mode="replace",
        partition_col="settlement_date",
        columns=MERIDIAN_COLUMNS,
    )

    meridian_unique = PythonOperator(
        task_id="check_meridian_events_unique",
        python_callable=check_meridian_events_unique,
        op_kwargs={"ds": "{{ ds }}"},
    )

    halcyon_sending = ShortCircuitOperator(
        task_id="check_halcyon_still_sending",
        python_callable=halcyon_is_still_sending,
        op_kwargs={"ds": "{{ ds }}"},
        ignore_downstream_trigger_rules=False,
    )

    load_halcyon = CsvToWarehouseOperator(
        task_id="load_halcyon_settlements",
        source="landing/halcyon/dt={{ ds }}/settlement_*.csv",
        table="raw.pay_halcyon_settlements",
        mode="replace",
        partition_col="file_date",
        columns=HALCYON_COLUMNS,
    )

    log_intake = PythonOperator(
        task_id="record_intake",
        python_callable=record_intake,
        op_kwargs={"ds": "{{ ds }}"},
        trigger_rule="none_failed_min_one_success",
    )

    load_meridian >> meridian_unique >> log_intake
    halcyon_sending >> load_halcyon >> log_intake
