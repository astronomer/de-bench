"""Order lines into `raw.order_lines`, one day at a time.

The OMS cuts lines beside the headers, into `landing/oms/dt=<ds>/order_lines.csv`.
Lines carry no date of their own — the day a line belongs to is its order's
`local_order_date` — so the day this run owns is the set of orders the header
table holds for it, and the write deletes and replaces exactly that set. That is
what makes a second run of a day a no-op rather than a doubling.

The store channel's line detail arrives here too, under
`source_system = 'pos_replay'`. `docs/runbooks/pos-ingestion.md` says where that
comes from and why it looks different from a web line.

Produces `raw.order_lines`. Read by `int_order_lines_discounted`,
`int_order_lines_costed`, and everything built on them.

Owned by commerce (J. Mwangi).
"""

from __future__ import annotations

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from include.lib import landing_dir, warehouse
from include.lib.notify import notify
from projects.commerce.lib import sql

DEFAULT_ARGS = {
    "owner": "commerce",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=10),
}


def source_file(ds: str) -> str:
    """The day's line cut, as an absolute path."""
    return str(landing_dir("oms") / f"dt={ds}" / "order_lines.csv")


def check_headers_loaded(ds: str) -> int:
    """The day's headers, which the lines are keyed against.

    Lines loaded before their headers are all orphans, and the orphan check
    below cannot tell that from a genuinely short header cut. Returns the
    header count.
    """
    with warehouse.connect(read_only=True) as con:
        rows = int(con.execute(
            f"SELECT count(*) FROM {warehouse.qualify('raw.orders')} "
            "WHERE local_order_date = ?", [ds],
        ).fetchone()[0])
    if not rows:
        raise ValueError(
            f"{ds}: raw.orders holds no rows for the day, so the lines have "
            "nothing to hang on. Let orders_intake take the day first."
        )
    return rows


def load_order_lines(ds: str) -> int:
    """Replace the day's lines with the ones in the day's file.

    The delete is scoped to the orders that belong to `ds` and the insert reads
    the same day's file, both inside one transaction, so a run that dies halfway
    leaves the day as it found it.
    """
    with warehouse.connect() as con:
        try:
            con.execute("BEGIN TRANSACTION")
            con.execute(sql.read("order_lines_delete_day"), [ds])
            written = int(con.execute(
                sql.read("order_lines_insert_day"), [source_file(ds)],
            ).fetchone()[0])
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise
    return written


def check_orphan_lines(ds: str) -> int:
    """Lines whose order is not in the day's headers. Zero, or the cut is short."""
    with warehouse.connect(read_only=True) as con:
        orphans = int(con.execute(sql.read("order_lines_orphans"), [ds]).fetchone()[0])
    if orphans:
        raise ValueError(
            f"{ds}: {orphans} lines name an order raw.orders does not hold. "
            "Reload the day's OMS cut; do not delete the lines."
        )
    return orphans


def check_line_totals(ds: str) -> int:
    """Every line's total against its own arithmetic.

    `line_total_cents` is what the OMS computed and the other columns are what
    it computed it from. They tie to the cent or the file is saying two
    different things and neither is safe to price.
    """
    with warehouse.connect(read_only=True) as con:
        bad = con.execute(sql.read("order_lines_total_breaks"), [ds]).fetchall()
    if bad:
        names = ", ".join(str(row[0]) for row in bad[:5])
        raise ValueError(f"{ds}: line totals do not tie on {len(bad)} lines: {names}")
    return 0


def record_intake(ds: str) -> int:
    """Replace the day's row in `ops.intake_log` with what is there now."""
    with warehouse.connect() as con:
        con.execute("CREATE SCHEMA IF NOT EXISTS ops")
        con.execute(sql.read("intake_log_ddl"))
        rows = int(con.execute(sql.read("order_lines_day_count"), [ds]).fetchone()[0])
        warehouse.delete_insert(
            "ops.intake_log", "ds", ds,
            [{"ds": ds, "source": "oms_order_lines", "row_count": rows}],
            con=con,
        )
    return rows


with DAG(
    dag_id="order_lines_daily",
    schedule="0 2 * * *",
    start_date=pendulum.datetime(2024, 2, 4, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["commerce", "raw"],
    doc_md=__doc__,
    on_failure_callback=notify("commerce", "order lines"),
) as dag:
    headers = PythonOperator(
        task_id="check_headers_loaded",
        python_callable=check_headers_loaded,
        op_kwargs={"ds": "{{ ds }}"},
    )

    load = PythonOperator(
        task_id="load_order_lines",
        python_callable=load_order_lines,
        op_kwargs={"ds": "{{ ds }}"},
    )

    orphans = PythonOperator(
        task_id="check_orphan_lines",
        python_callable=check_orphan_lines,
        op_kwargs={"ds": "{{ ds }}"},
    )

    totals = PythonOperator(
        task_id="check_line_totals",
        python_callable=check_line_totals,
        op_kwargs={"ds": "{{ ds }}"},
    )

    log_intake = PythonOperator(
        task_id="record_intake",
        python_callable=record_intake,
        op_kwargs={"ds": "{{ ds }}"},
    )

    headers >> load >> orphans >> totals >> log_intake
