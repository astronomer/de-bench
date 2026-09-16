"""Receipts, picks and transfers into `raw.wms_movements`, hourly.

Corvid posts movements asynchronously through the night, so the file grows after
it first appears. The read is incremental on `loaded_at` and bounded by the
run's own interval, which is what `include.lib.watermark` is for: `since()`
gives the floor and the run's own `data_interval_end` gives the ceiling.

**On an hourly schedule the day is not the window.** `ds` is the same string for
twenty-four consecutive runs, so a read keyed on it re-reads the whole day every
hour and a write scoped to it deletes the sibling hours that already landed.
This DAG reads and writes on the interval and never on `ds`.

**The vocabulary is Corvid's, not the application's.** A sale is a `pick`, a
shrink is what a `cycle_count` found, a return goes out again as an `rtv`.
Nothing translates them on the way in; `int_scan_normalized` and the movement
models do that downstream.

Produces `raw.wms_movements`. Read by `int_inventory_position_daily`,
`sc_dc_transfer_daily` and `sc_shrink_weekly`.

Owned by supply-chain (K. Duffy).
"""

from __future__ import annotations

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from include.lib import warehouse, watermark
from include.lib.notify import notify
from projects.supply.lib import paths

DEFAULT_ARGS = {
    "owner": "supply",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
}

#: The stream this DAG's watermark is filed under.
STREAM = "wms_movements"

LOAD = """
-- Movements the file carries whose load time falls in this run's window. The
-- bounds are the interval's, half open at the top, so two consecutive runs
-- cannot both take the same row and neither can miss one.
INSERT OR REPLACE INTO copperline.raw.wms_movements BY NAME
SELECT * FROM read_csv(?, header = true, union_by_name = true)
WHERE loaded_at >= ? AND loaded_at < ?
"""

VOCABULARY = """
-- Movement types the downstream models do not know how to read. A new Corvid
-- word arrives with a release and is silently dropped by every model that
-- filters on the ones it knows, so it is caught here where the rows land.
SELECT DISTINCT movement_type
FROM copperline.raw.wms_movements
WHERE loaded_at >= ? AND loaded_at < ?
  AND movement_type NOT IN ('pick', 'receipt', 'rtv', 'pack', 'cycle_count',
                            'adjust', 'transfer')
ORDER BY movement_type
"""


def load_window(**context) -> int:
    """Take the movements whose load time falls in this run's window."""
    since = watermark.since(STREAM, context)
    until = context["data_interval_end"]
    path = paths.wms_movements(context["ds"])
    if not path.exists():
        return 0
    with warehouse.connect() as con:
        return int(con.execute(LOAD, [str(path), since, until]).fetchone()[0])


def check_vocabulary(**context) -> int:
    """Movement types nothing downstream knows how to read."""
    since = watermark.since(STREAM, context)
    with warehouse.connect(read_only=True) as con:
        unknown = con.execute(
            VOCABULARY, [since, context["data_interval_end"]],
        ).fetchall()
    if unknown:
        names = ", ".join(str(row[0]) for row in unknown)
        raise ValueError(
            f"Corvid sent movement types nothing reads: {names}. Add them to the "
            "normalisation before the position is built from them."
        )
    return 0


def check_locations(**context) -> int:
    """Movements that name neither a source nor a destination.

    A movement with no location at either end cannot be replayed onto a
    position, and the replay silently drops it rather than failing.
    """
    since = watermark.since(STREAM, context)
    with warehouse.connect(read_only=True) as con:
        stranded = int(con.execute(
            f"SELECT count(*) FROM {warehouse.qualify('raw.wms_movements')} "
            "WHERE loaded_at >= ? AND loaded_at < ? "
            "AND from_location IS NULL AND to_location IS NULL",
            [since, context["data_interval_end"]],
        ).fetchone()[0])
    if stranded:
        raise ValueError(f"{stranded} movements name no location at either end")
    return stranded


def record_intake(**context) -> int:
    """The window's row count, into `ops.intake_log`.

    Keyed on the interval rather than on the day, because twenty-four runs share
    a day and a log keyed on the day would hold one of them.
    """
    since = watermark.since(STREAM, context)
    with warehouse.connect() as con:
        con.execute("CREATE SCHEMA IF NOT EXISTS ops")
        con.execute(
            f"CREATE TABLE IF NOT EXISTS {warehouse.qualify('ops.intake_windows')} "
            "(stream VARCHAR, window_start TIMESTAMP, window_end TIMESTAMP, "
            "row_count BIGINT)"
        )
        rows = int(con.execute(
            f"SELECT count(*) FROM {warehouse.qualify('raw.wms_movements')} "
            "WHERE loaded_at >= ? AND loaded_at < ?",
            [since, context["data_interval_end"]],
        ).fetchone()[0])
        con.execute(
            f"DELETE FROM {warehouse.qualify('ops.intake_windows')} "
            "WHERE stream = ? AND window_start = ?", [STREAM, since],
        )
        con.execute(
            f"INSERT INTO {warehouse.qualify('ops.intake_windows')} VALUES (?, ?, ?, ?)",
            [STREAM, since, context["data_interval_end"], rows],
        )
    return rows


def advance_mark(**context) -> str:
    """Move the mark to the end of this run's window.

    Last, and only on success. A run that reads with `since()` and never
    advances re-reads the same window forever, and the counts stay plausible.
    """
    return watermark.advance(STREAM, context).isoformat()


with DAG(
    dag_id="sc_inventory_movements_hourly",
    schedule="5 * * * *",
    start_date=pendulum.datetime(2024, 2, 4, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["supply", "raw", "intake", "inventory"],
    doc_md=__doc__,
    on_failure_callback=notify("supply", "WMS movement intake"),
) as dag:
    load = PythonOperator(task_id="load_window", python_callable=load_window)
    vocabulary = PythonOperator(
        task_id="check_vocabulary", python_callable=check_vocabulary)
    locations = PythonOperator(
        task_id="check_locations", python_callable=check_locations)
    counts = PythonOperator(
        task_id="record_intake", python_callable=record_intake)
    mark = PythonOperator(task_id="advance_watermark", python_callable=advance_mark)

    load >> vocabulary >> locations >> counts >> mark
