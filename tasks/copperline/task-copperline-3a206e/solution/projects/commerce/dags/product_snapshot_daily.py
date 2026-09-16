"""The product snapshots that build `dim_product`.

Two dbt snapshots run here: `snap_product_price`, which tracks the ticket price
of a variant, and `snap_product_attrs`, which tracks the merchandising
attributes. `dim_product` is built from the pair, and it is the conformed
dimension commerce never handed over to the platform team — the policy in
`projects/platform/README.md` says platform builds conformed dims once, and this
is the exception nobody closed.

The PIM change feed arrives out of order with no bound, so the snapshot orders
on the source's own change stamp rather than on arrival. Ties break on the
originating system, in the order the SCD contract states.

`build_product_history` then rebuilds `marts.dim_product` from the change feed
itself. The snapshot pair versions the newest row per SKU from one night to the
next, so the history it holds starts on the night it first ran; the feed holds
every change since Palette was turned on, with the date each one took effect.
`close_price_book`, `enrich_product` and the grain check below all read the
table as spans, so the spans are what the nightly has to leave behind.

Produces the two snapshot tables and `dim_product`. Read by every mart that
names a product, the merch dashboards, and `fin_gl_export`.

Owned by commerce (J. Mwangi).
"""

from __future__ import annotations

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from include.lib import warehouse, workspace_root
from include.lib.notify import notify
from projects.commerce.lib import sql

DEFAULT_ARGS = {
    "owner": "commerce",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
}

#: The dbt project and the interpreter that runs it. The virtualenv is baked
#: into the image; `dbt/README.md` says why it is not the ambient Python.
DBT = "/opt/dbt-venv/bin/dbt"
DBT_PROJECT = workspace_root() / "dbt" / "copperline_analytics"


def build_product_history() -> int:
    """Rebuild the product history from Palette's change feed.

    Whole every night rather than a day at a time: the catalogue is under two
    thousand SKUs, a late change rewrites spans it landed behind, and a rebuild
    that reads the whole feed cannot leave a day half-applied. Returns the span
    count.
    """
    with warehouse.connect() as con:
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {warehouse.qualify('marts')}")
        con.execute(sql.read("product_history_build"))
        rows = con.execute(
            f"SELECT count(*) FROM {warehouse.qualify('marts.dim_product')}"
        ).fetchone()[0]
    return int(rows)


def check_snapshot_grain(ds: str) -> int:
    """One open row per variant in each snapshot.

    Two open rows means a change landed twice under different stamps, and every
    join to the dimension then fans out. Returns the open row count.
    """
    with warehouse.connect(read_only=True) as con:
        doubled, open_rows = con.execute(sql.read("snapshot_open_rows"), [ds]).fetchone()
    if doubled:
        raise ValueError(
            f"{ds}: {doubled} variants carry more than one open snapshot row. "
            "Every join to dim_product multiplies by that."
        )
    return int(open_rows or 0)


with DAG(
    dag_id="product_snapshot_daily",
    schedule="0 1 * * *",
    start_date=pendulum.datetime(2024, 2, 4, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["commerce", "snapshot", "dim"],
    doc_md=__doc__,
    on_failure_callback=notify("commerce", "product snapshots"),
) as dag:
    snapshot_price = BashOperator(
        task_id="snapshot_product_price",
        bash_command=f"{DBT} snapshot --select snap_product_price",
        cwd=str(DBT_PROJECT),
    )

    snapshot_attrs = BashOperator(
        task_id="snapshot_product_attrs",
        bash_command=f"{DBT} snapshot --select snap_product_attrs",
        cwd=str(DBT_PROJECT),
    )

    build_dim = BashOperator(
        task_id="build_dim_product",
        bash_command=f"{DBT} run --select dim_product",
        cwd=str(DBT_PROJECT),
    )

    history = PythonOperator(
        task_id="build_product_history",
        python_callable=build_product_history,
    )

    grain = PythonOperator(
        task_id="check_snapshot_grain",
        python_callable=check_snapshot_grain,
        op_kwargs={"ds": "{{ ds }}"},
    )

    [snapshot_price, snapshot_attrs] >> build_dim >> history >> grain
