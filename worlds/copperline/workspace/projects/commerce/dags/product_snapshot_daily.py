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

    grain = PythonOperator(
        task_id="check_snapshot_grain",
        python_callable=check_snapshot_grain,
        op_kwargs={"ds": "{{ ds }}"},
    )

    [snapshot_price, snapshot_attrs] >> build_dim >> grain
