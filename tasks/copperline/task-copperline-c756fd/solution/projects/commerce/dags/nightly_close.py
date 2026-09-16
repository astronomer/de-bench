"""The commerce nightly close: ingest, enrich, price, publish, export.

Thirty-four tasks in one file, and it should not be one file. It was three —
`nightly_ingest`, `nightly_price` and `nightly_publish` — until the FY2025 close,
when the three were merged in a week to get one dependency chain instead of two
sensors. The merge worked, the close landed, and nobody has split it back.

What that costs, every day: the ingest half takes about nine minutes and the
finance half takes about forty seconds, and the finance half is the one people
rerun. Rerunning it means clearing the whole run, which re-reads four channels
of files nobody has changed. One schedule, one retry policy, one owner e-mail
for two jobs that fail for completely different reasons.

Produces `staging.orders_enriched` for the close date, `marts.order_economics`,
and the three export packs the morning distribution sends. Read by finance's
close pack and by the merch dashboards.

Owned by commerce (J. Mwangi).
"""

from __future__ import annotations

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.sdk import DAG

from include.lib import landing_dir, warehouse
from include.lib.notify import notify
from projects.commerce.lib import sql

DEFAULT_ARGS = {
    "owner": "commerce",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=10),
}

#: The four channels the close ingests, and the file each one waits on. The
#: loop below is over this committed list and runs at parse; commerce does not
#: map, so the four are four squares in the graph and an auditor can see which
#: one was red.
CHANNELS = (
    ("store", "pos", "dt={{ ds }}/store_S-0101.csv"),
    ("web", "oms", "dt={{ ds }}/orders.csv"),
    ("marketplace", "marketplace", "dt={{ ds }}/orders.csv"),
    ("trade", "ar", "dt={{ ds }}/invoices.csv"),
)

#: The three packs the morning distribution sends. Each has a statement of its
#: own under `projects/commerce/sql/close_export_<name>.sql`.
EXPORTS = ("gl_extract", "merch_pack", "store_pack")

#: Where the packs land, under `include/data/`.
EXPORT_LAYER = "exports"


def run(statement: str, ds: str, *extra: object) -> int:
    """Run one commerce statement over one close date."""
    with warehouse.connect() as con:
        row = con.execute(sql.read(statement), [ds, *extra]).fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def load_channel(channel: str, ds: str) -> int:
    """Replace the close date's rows for one channel."""
    return run(f"close_load_{channel}", ds)


def check_channel(channel: str, ds: str) -> int:
    """The channel's loaded count against what its manifest claims.

    A channel that is short is a reload, not a repair, and the close does not
    price a short day.
    """
    with warehouse.connect(read_only=True) as con:
        claimed, loaded = con.execute(
            sql.read("close_channel_counts"), [ds, channel],
        ).fetchone()
    if claimed is not None and claimed != loaded:
        raise ValueError(
            f"{ds} {channel}: the source claims {claimed} rows and {loaded} "
            "landed. Reload the channel before the close prices it."
        )
    return int(loaded or 0)


def build_step(statement: str, ds: str) -> int:
    """One enrichment, pricing or economics step."""
    return run(statement, ds)


def check_economics_grain(ds: str) -> int:
    """One row per order in the day the close has built, per the mart's contract.

    Held against the build rather than against `marts.order_economics`: the
    publish is three steps below this one, so reading the mart here graded either
    nothing (a date never closed before) or the run before this one.
    """
    with warehouse.connect(read_only=True) as con:
        rows, orders = con.execute(sql.read("close_economics_grain"), [ds, ds]).fetchone()
    if rows != orders:
        raise ValueError(
            f"{ds}: the close built {rows} rows for {orders} orders; the grain "
            "in contracts/order_economics.yml is one row per order"
        )
    return int(rows)


def tie_to_orders(ds: str) -> int:
    """Booked cents in the day the close has built, against the order table it
    came from.

    Cent-exact. A difference here is the pipeline's until somebody shows it is
    the OMS's, and finance reads this number at close. Held against the build,
    for the same reason the grain check is: the close does not publish a break,
    so the tie has to run before the publish and there is nothing in the mart to
    hold at that point.
    """
    with warehouse.connect(read_only=True) as con:
        booked, ordered = con.execute(sql.read("close_revenue_tie"), [ds, ds]).fetchone()
    if booked != ordered:
        raise ValueError(
            f"{ds}: the close books {booked} cents and raw.orders "
            f"totals {ordered}. The close does not publish a break."
        )
    return int(booked or 0)


#: The mart's columns, in the order `close_publish_economics` selects them.
#: `contracts/order_economics.yml` is the source.
ECONOMICS_COLUMNS = (
    "order_id", "order_date", "channel",
    "booked_cents", "net_sales_cents", "merch_margin_cents",
)


def publish_economics(ds: str) -> int:
    """Replace the close date's partition of `marts.order_economics`.

    A bare `INSERT` put the day in a second time on every rerun, which is what
    `CONVENTIONS.md`, "Writing to the warehouse", rule 2 calls a defect: the
    merch dashboards reconcile a total at order grain and a doubled day still
    renders. `delete_insert` owns this date and touches no other.
    """
    with warehouse.connect() as con:
        rows = con.execute(sql.read("close_publish_economics"), [ds]).fetchall()
        return warehouse.delete_insert(
            "marts.order_economics", "order_date", ds, rows,
            columns=list(ECONOMICS_COLUMNS), con=con,
        )


def export_pack(name: str, ds: str) -> str:
    """Publish one export pack as a partition file."""
    with warehouse.connect(read_only=True) as con:
        result = con.execute(sql.read(f"close_export_{name}"), [ds])
        header = [description[0] for description in result.description]
        rows = result.fetchall()
    return str(warehouse.write_partition(EXPORT_LAYER, name, ds, header, rows))


def record_close(ds: str) -> int:
    """The close's own row in `ops.close_log`, for the morning question."""
    with warehouse.connect() as con:
        con.execute("CREATE SCHEMA IF NOT EXISTS ops")
        con.execute(sql.read("close_log_ddl"))
        rows = int(con.execute(sql.read("close_economics_count"), [ds]).fetchone()[0])
        warehouse.delete_insert(
            "ops.close_log", "ds", ds,
            [{"ds": ds, "orders": rows, "state": "complete"}], con=con,
        )
    return rows


def check_close_complete(ds: str) -> int:
    """Every export the morning distribution expects is on disk for the date."""
    missing = [
        name for name in EXPORTS
        if not warehouse.partition_path(EXPORT_LAYER, name, ds).exists()
    ]
    if missing:
        raise ValueError(f"{ds}: the close did not write {', '.join(missing)}")
    return len(EXPORTS)


def announce_close(ds: str) -> str:
    """The line the morning distribution and the flash both wait to see."""
    return f"commerce close complete for {ds}"


with DAG(
    dag_id="nightly_close",
    schedule="0 2 * * *",
    start_date=pendulum.datetime(2024, 2, 4, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["commerce", "close", "mart"],
    doc_md=__doc__,
    on_failure_callback=notify("commerce", "nightly close"),
) as dag:
    checks = []
    for _channel, _source, _leaf in CHANNELS:
        _wait = FileSensor(
            task_id=f"wait_{_channel}_files",
            filepath=str(landing_dir(_source) / _leaf),
            poke_interval=300,
            timeout=60 * 90,
            mode="reschedule",
        )
        _load = PythonOperator(
            task_id=f"load_{_channel}_orders",
            python_callable=load_channel,
            op_kwargs={"channel": _channel, "ds": "{{ ds }}"},
        )
        _check = PythonOperator(
            task_id=f"check_{_channel}_counts",
            python_callable=check_channel,
            op_kwargs={"channel": _channel, "ds": "{{ ds }}"},
        )
        _wait >> _load >> _check
        checks.append(_check)

    enrich_base = PythonOperator(
        task_id="build_enrich_base", python_callable=build_step,
        op_kwargs={"statement": "enrich_base", "ds": "{{ ds }}"})
    enrich_customer = PythonOperator(
        task_id="join_customer", python_callable=build_step,
        op_kwargs={"statement": "enrich_customer", "ds": "{{ ds }}"})
    enrich_product = PythonOperator(
        task_id="join_product", python_callable=build_step,
        op_kwargs={"statement": "enrich_product", "ds": "{{ ds }}"})
    enrich_channel = PythonOperator(
        task_id="join_channel", python_callable=build_step,
        op_kwargs={"statement": "enrich_channel", "ds": "{{ ds }}"})
    enrich_tender = PythonOperator(
        task_id="join_tender", python_callable=build_step,
        op_kwargs={"statement": "enrich_tender", "ds": "{{ ds }}"})
    enrich_publish = PythonOperator(
        task_id="publish_enriched", python_callable=build_step,
        op_kwargs={"statement": "enrich_publish", "ds": "{{ ds }}"})

    price_book = PythonOperator(
        task_id="load_price_book", python_callable=build_step,
        op_kwargs={"statement": "close_price_book", "ds": "{{ ds }}"})
    list_price = PythonOperator(
        task_id="apply_list_price", python_callable=build_step,
        op_kwargs={"statement": "close_apply_list_price", "ds": "{{ ds }}"})
    promotions = PythonOperator(
        task_id="apply_promotions", python_callable=build_step,
        op_kwargs={"statement": "close_apply_promotions", "ds": "{{ ds }}"})
    gift_cards = PythonOperator(
        task_id="apply_gift_cards", python_callable=build_step,
        op_kwargs={"statement": "close_apply_gift_cards", "ds": "{{ ds }}"})
    pricing_tie = PythonOperator(
        task_id="check_pricing_ties", python_callable=build_step,
        op_kwargs={"statement": "close_pricing_tie", "ds": "{{ ds }}"})

    line_economics = PythonOperator(
        task_id="build_line_economics", python_callable=build_step,
        op_kwargs={"statement": "close_line_economics", "ds": "{{ ds }}"})
    order_economics = PythonOperator(
        task_id="build_order_economics", python_callable=build_step,
        op_kwargs={"statement": "close_order_economics", "ds": "{{ ds }}"})
    economics_grain = PythonOperator(
        task_id="check_economics_grain", python_callable=check_economics_grain,
        op_kwargs={"ds": "{{ ds }}"})
    revenue_tie = PythonOperator(
        task_id="tie_to_orders", python_callable=tie_to_orders,
        op_kwargs={"ds": "{{ ds }}"})
    publish = PythonOperator(
        task_id="publish_economics", python_callable=publish_economics,
        op_kwargs={"ds": "{{ ds }}"})

    exports = []
    for _name in EXPORTS:
        _export = PythonOperator(
            task_id=f"export_{_name}",
            python_callable=export_pack,
            op_kwargs={"name": _name, "ds": "{{ ds }}"},
        )
        publish >> _export
        exports.append(_export)

    close_log = PythonOperator(
        task_id="record_close_log", python_callable=record_close,
        op_kwargs={"ds": "{{ ds }}"})
    close_check = PythonOperator(
        task_id="check_close_complete", python_callable=check_close_complete,
        op_kwargs={"ds": "{{ ds }}"})
    announce = PythonOperator(
        task_id="notify_close_done", python_callable=announce_close,
        op_kwargs={"ds": "{{ ds }}"})

    checks >> enrich_base
    enrich_base >> [enrich_customer, enrich_product, enrich_channel, enrich_tender]
    [enrich_customer, enrich_product, enrich_channel, enrich_tender] >> enrich_publish
    enrich_publish >> price_book >> list_price >> promotions >> gift_cards >> pricing_tie
    pricing_tie >> line_economics >> order_economics >> economics_grain
    economics_grain >> revenue_tie >> publish
    exports >> close_log >> close_check >> announce
