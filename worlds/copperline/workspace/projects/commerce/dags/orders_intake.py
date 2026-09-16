"""Web and store orders into `raw.orders`, hourly.

The OMS cuts one file a night into `landing/oms/dt=<ds>/orders.csv`, and this
DAG picks it up every hour so that a file which lands at 02:14 is in the
warehouse by 03:20 rather than the next morning. A run that finds the day's file
already loaded replaces the same partition with the same rows, which is why it
may run twenty-four times a day without the day's counts moving.

The whole order spine is downstream: `order_lines_daily`, `orders_enrich_daily`,
`order_economics_daily`, and every mart commerce publishes.

Owned by commerce (J. Mwangi). When this stalls, the flash is late.
"""

from __future__ import annotations

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.sdk import DAG

from include.lib import landing_dir, warehouse, watermark
from include.lib.loaders import CsvToWarehouseOperator
from include.lib.notify import notify
from projects.commerce.lib import sql

DEFAULT_ARGS = {
    "owner": "commerce",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
}

#: The columns the CSV sniffer guesses badly, named so the guess cannot drift
#: with a DuckDB minor. The nullable three are era columns: `event_time_utc` is
#: empty on store rows before the UTC cutover, and the currency pair before the
#: market cutover.
ORDER_COLUMNS = {
    "order_id": "VARCHAR",
    "local_order_date": "DATE",
    "event_time_utc": "TIMESTAMP",
    "event_time_local": "TIMESTAMP",
    "currency_code": "VARCHAR",
    "fx_rate_ppm": "BIGINT",
    "grand_total_cents": "BIGINT",
    "deleted_at": "TIMESTAMP",
    "updated_at": "TIMESTAMP",
    "loaded_at": "TIMESTAMP",
}


def file_line_count(ds: str) -> int:
    """Data lines in the day's cut, header excluded."""
    path = landing_dir("oms") / f"dt={ds}" / "orders.csv"
    with path.open(encoding="utf-8") as handle:
        return max(0, sum(1 for _ in handle) - 1)


def check_row_count(ds: str) -> int:
    """The file's line count against what the load took.

    The two agree or the file was truncated in transit, and a truncated day is
    reloaded rather than enriched. Returns the loaded count.
    """
    with warehouse.connect(read_only=True) as con:
        loaded = int(con.execute(
            f"SELECT count(*) FROM {warehouse.qualify('raw.orders')} "
            "WHERE local_order_date = ?", [ds],
        ).fetchone()[0])
    sent = file_line_count(ds)
    if loaded != sent:
        raise ValueError(
            f"{ds}: the OMS cut carries {sent} rows and raw.orders holds "
            f"{loaded}. Reload the day rather than working from a short file."
        )
    return loaded


def check_order_ids_unique(ds: str) -> int:
    """One row per order, for the day. Returns the number of orders."""
    with warehouse.connect(read_only=True) as con:
        rows = con.execute(sql.read("orders_duplicate_ids"), [ds]).fetchall()
    if rows:
        names = ", ".join(str(row[0]) for row in rows[:5])
        raise ValueError(f"{ds}: order ids repeat in the cut: {names}")
    with warehouse.connect(read_only=True) as con:
        return int(con.execute(
            f"SELECT count(*) FROM {warehouse.qualify('raw.orders')} "
            "WHERE local_order_date = ?", [ds],
        ).fetchone()[0])


def record_intake(ds: str) -> int:
    """Replace the day's row in `ops.intake_log` with what is there now."""
    with warehouse.connect() as con:
        con.execute("CREATE SCHEMA IF NOT EXISTS ops")
        con.execute(sql.read("intake_log_ddl"))
        rows = int(con.execute(
            f"SELECT count(*) FROM {warehouse.qualify('raw.orders')} "
            "WHERE local_order_date = ?", [ds],
        ).fetchone()[0])
        warehouse.delete_insert(
            "ops.intake_log", "ds", ds,
            [{"ds": ds, "source": "oms_orders", "row_count": rows}],
            con=con,
        )
    return rows


def advance_orders_watermark(**context) -> str:
    """Move the `oms_orders` mark to the end of this run's interval, so the
    incremental readers downstream know how far the feed has been taken."""
    return watermark.advance("oms_orders", context).isoformat()


with DAG(
    dag_id="orders_intake",
    schedule="20 * * * *",
    start_date=pendulum.datetime(2024, 2, 4, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["commerce", "raw", "intake"],
    doc_md=__doc__,
    on_failure_callback=notify("commerce", "orders intake"),
) as dag:
    wait_for_cut = FileSensor(
        task_id="wait_for_oms_cut",
        filepath=str(landing_dir("oms") / "dt={{ ds }}" / "orders.csv"),
        poke_interval=300,
        timeout=60 * 50,
        mode="reschedule",
    )

    load_orders = CsvToWarehouseOperator(
        task_id="load_orders",
        source="landing/oms/dt={{ ds }}/orders.csv",
        table="raw.orders",
        mode="replace",
        partition_col="local_order_date",
        columns=ORDER_COLUMNS,
    )

    count_check = PythonOperator(
        task_id="check_row_count",
        python_callable=check_row_count,
        op_kwargs={"ds": "{{ ds }}"},
    )

    id_check = PythonOperator(
        task_id="check_order_ids_unique",
        python_callable=check_order_ids_unique,
        op_kwargs={"ds": "{{ ds }}"},
    )

    log_intake = PythonOperator(
        task_id="record_intake",
        python_callable=record_intake,
        op_kwargs={"ds": "{{ ds }}"},
    )

    move_mark = PythonOperator(
        task_id="advance_watermark",
        python_callable=advance_orders_watermark,
    )

    wait_for_cut >> load_orders >> count_check >> id_check >> log_intake >> move_mark
