"""Stores that missed the hourly window, picked up at 04:00.

A store's batch file can arrive up to three days after its business date, and
when it does every row in it arrives at once — the rows themselves carry no lag
of their own. So this looks back over a window of business dates rather than
over the run's own day, and reloads the store-days whose files have turned up
since the last look.

`docs/late-data-policy.md` LD-1 says a late row is never dropped and LD-2 says a
day that has already been published is rebuilt rather than appended to. Both are
what this DAG is for. `docs/runbooks/pos-ingestion.md` POS-1 says the rest: a
store that has not sent by 04:00 is late, not missing, and a store that has sent
nothing for three business days is escalated rather than backfilled.

Produces the late store-days of `raw.pos_sales_header` and their manifest rows.

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
    "retry_delay": pendulum.duration(minutes=10),
}

#: The measured file lag on this feed, from POS-1. It is a file-level number
#: and not a row-level one, and the two are different: no POS row is ever late,
#: the file that carries it is. Re-measure it if the register software changes;
#: do not copy it into a model whose source delivers differently.
LOOKBACK_DAYS = 3


def business_dates(ds: str) -> list[str]:
    """The business dates this run may still be waiting on, oldest first."""
    day = pendulum.parse(ds).date()
    return [day.subtract(days=n).isoformat() for n in range(LOOKBACK_DAYS, 0, -1)]


def find_late_stores(ds: str) -> list[tuple[str, str]]:
    """(business date, store) for every file on disk that is not yet loaded."""
    late = []
    with warehouse.connect(read_only=True) as con:
        for day in business_dates(ds):
            loaded = {
                row[0] for row in
                con.execute(sql.read("pos_loaded_stores"), [day]).fetchall()
            }
            late += [(day, store) for store, _ in pos.store_files(day)
                     if store not in loaded]
    return late


def report_late(ds: str) -> int:
    """How many store-days are waiting, for the morning question."""
    return len(find_late_stores(ds))


def load_late_stores(ds: str) -> int:
    """Reload each late store-day from its file.

    The write replaces the store-day, so a day that was already published comes
    back rebuilt rather than doubled.
    """
    loaded = 0
    with warehouse.connect() as con:
        for day, store in find_late_stores(ds):
            path = pos.store_file(day, store)
            if path is None:
                continue
            try:
                con.execute("BEGIN TRANSACTION")
                con.execute(sql.read("pos_delete_store_day"), [day, store])
                loaded += int(con.execute(
                    sql.read("pos_insert_store_day"), [path, store, day],
                ).fetchone()[0])
                con.execute(sql.read("pos_manifest_build_store"), [day, store])
                con.execute("COMMIT")
            except Exception:
                con.execute("ROLLBACK")
                raise
    return loaded


def escalate_silent_stores(ds: str) -> int:
    """Stores that have sent nothing for three business days.

    POS-1 escalates these to store systems rather than backfilling them, and it
    says to check the market calendar first: a store in a market with
    `feed_expected = false` sent nothing because it was shut.
    """
    with warehouse.connect() as con:
        con.execute("CREATE SCHEMA IF NOT EXISTS ops")
        con.execute(sql.read("pos_escalations_ddl"))
        silent = con.execute(
            sql.read("pos_silent_stores"), [ds, LOOKBACK_DAYS],
        ).fetchall()
        warehouse.delete_insert(
            "ops.pos_escalations", "ds", ds,
            [{"ds": ds, "store_id": row[0], "last_seen": row[1]} for row in silent],
            columns=["ds", "store_id", "last_seen"], con=con,
        )
    return len(silent)


with DAG(
    dag_id="store_pos_late_catchup",
    schedule="0 4 * * *",
    start_date=pendulum.datetime(2024, 2, 4, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["commerce", "raw", "pos", "late-data"],
    doc_md=__doc__,
    on_failure_callback=notify("commerce", "POS late catch-up"),
) as dag:
    late = PythonOperator(
        task_id="report_late_stores", python_callable=report_late,
        op_kwargs={"ds": "{{ ds }}"})
    load = PythonOperator(
        task_id="load_late_stores", python_callable=load_late_stores,
        op_kwargs={"ds": "{{ ds }}"})
    escalate = PythonOperator(
        task_id="escalate_silent_stores", python_callable=escalate_silent_stores,
        op_kwargs={"ds": "{{ ds }}"})

    late >> load >> escalate
