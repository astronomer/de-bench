"""Store close batches into `raw.pos_sales_header` and the batch manifest.

One CSV per trading store per night, under `landing/pos/dt=<ds>/`, named for the
store. 268 stores, so a complete night is 268 files. This runs hourly and takes
whatever has landed; `store_pos_late_catchup` runs at 04:00 and takes the
stragglers. `docs/runbooks/pos-ingestion.md` is the operational description of
both.

**The store id is in the file name, not in the file.** The store server writes
its own columns and its own name, so the loader reads the name for the store and
each file is loaded on its own rather than through one glob. That is also why a
file renamed on the way in lands under the wrong store, which the runbook's open
items record.

A reload replaces the store-day. It does not append. If a store-day's totals
double after a reload, the load path appended and that is a defect worth
raising, not data to clean up.

Produces `raw.pos_sales_header` and `raw.pos_batch_manifest`. Read by the store
sales marts, the comp figures and the till reconciliation.

Owned by commerce (J. Mwangi).
"""

from __future__ import annotations

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from include.lib import warehouse
from include.lib.notify import notify
from projects.commerce.lib import pos, sql

DEFAULT_ARGS = {
    "owner": "commerce",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
}


def list_arrived(ds: str) -> int:
    """How many store files are on disk for the day."""
    return len(pos.store_files(ds))


def load_stores(ds: str) -> int:
    """Load every store file that has landed, one store-day at a time.

    Each file is one store's day, so each load deletes that store-day and
    replaces it. A file that lands after this run is taken by the next hour's
    run, or by the catch-up at 04:00, and neither disturbs a store already in.
    """
    loaded = 0
    with warehouse.connect() as con:
        for store, path in pos.store_files(ds):
            try:
                con.execute("BEGIN TRANSACTION")
                con.execute(sql.read("pos_delete_store_day"), [ds, store])
                loaded += int(con.execute(
                    sql.read("pos_insert_store_day"), [path, store, ds],
                ).fetchone()[0])
                con.execute("COMMIT")
            except Exception:
                con.execute("ROLLBACK")
                raise
    return loaded


def build_manifest(ds: str) -> int:
    """One manifest row per store-day loaded, with the counts the till
    reconciliation works off."""
    with warehouse.connect() as con:
        return int(con.execute(sql.read("pos_manifest_build"), [ds]).fetchone()[0])


def check_store_days(ds: str) -> int:
    """Every loaded store-day appears once.

    Two manifest rows for one store-day means a file was taken twice under two
    names, which is the one shape a reload cannot repair by itself.
    """
    with warehouse.connect(read_only=True) as con:
        doubled = con.execute(sql.read("pos_doubled_store_days"), [ds]).fetchall()
    if doubled:
        names = ", ".join(str(row[0]) for row in doubled[:5])
        raise ValueError(f"{ds}: these stores hold two batches for the day: {names}")
    return 0


def check_till_totals(ds: str) -> int:
    """The till reconciliation, store by store, per POS-2.

    The register's declared count against the count we loaded. A difference of
    a few cents in the money is float rounding at the register and is written
    off; a difference of whole transactions is a partial file, and the fix is a
    reload of that store's file rather than a repair of its rows.

    Returns the number of stores that reconcile.
    """
    with warehouse.connect(read_only=True) as con:
        partial = con.execute(sql.read("pos_partial_files"), [ds]).fetchall()
        clean = int(con.execute(sql.read("pos_clean_stores"), [ds]).fetchone()[0])
    if partial:
        names = ", ".join(f"{row[0]} ({row[1]} short)" for row in partial[:5])
        raise ValueError(
            f"{ds}: {len(partial)} stores sent a partial file: {names}. Ask "
            "store systems to resend and reload the store-day."
        )
    return clean


def record_intake(ds: str) -> int:
    """The day's store count and row count, into `ops.intake_log`."""
    with warehouse.connect() as con:
        con.execute("CREATE SCHEMA IF NOT EXISTS ops")
        con.execute(sql.read("intake_log_ddl"))
        rows = int(con.execute(sql.read("pos_day_count"), [ds]).fetchone()[0])
        warehouse.delete_insert(
            "ops.intake_log", "ds", ds,
            [{"ds": ds, "source": "pos_batches", "row_count": rows}], con=con,
        )
    return rows


with DAG(
    dag_id="store_pos_intake",
    schedule="30 * * * *",
    start_date=pendulum.datetime(2024, 2, 4, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["commerce", "raw", "intake", "pos"],
    doc_md=__doc__,
    on_failure_callback=notify("commerce", "store POS intake"),
) as dag:
    arrived = PythonOperator(
        task_id="list_arrived_files", python_callable=list_arrived,
        op_kwargs={"ds": "{{ ds }}"})
    load = PythonOperator(
        task_id="load_store_batches", python_callable=load_stores,
        op_kwargs={"ds": "{{ ds }}"})
    manifest = PythonOperator(
        task_id="build_batch_manifest", python_callable=build_manifest,
        op_kwargs={"ds": "{{ ds }}"})
    doubled = PythonOperator(
        task_id="check_store_days", python_callable=check_store_days,
        op_kwargs={"ds": "{{ ds }}"})
    tills = PythonOperator(
        task_id="check_till_totals", python_callable=check_till_totals,
        op_kwargs={"ds": "{{ ds }}"})
    log_intake = PythonOperator(
        task_id="record_intake", python_callable=record_intake,
        op_kwargs={"ds": "{{ ds }}"})

    arrived >> load >> manifest >> doubled >> tills >> log_intake
