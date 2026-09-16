"""`marts.order_economics` at order grain, one day at a time.

One row per order: what was booked, what it netted after returns and discounts,
and the merchandise margin behind it. `contracts/order_economics.yml` fixes the
grain and the columns, and `contracts/merch-dashboards.md` is the prose contract
the dashboards read it under.

The margin column is the reason this is a mart rather than a view. Net revenue
and landed cost are also finance's numbers, so both come from the shared `int`
models rather than being rebuilt here — `docs/lineage.md` names the readers, and
there are more of them than the dbt graph shows.

Produces `marts.order_economics`. Read by the merch dashboards, `fin_margin_daily`,
`fin_ledger_tie`, `fin_gl_export`, `models/finance/category_margin.sql` and
`commerce_export_partner`.

Owned by commerce (J. Mwangi).
"""

from __future__ import annotations

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from include.lib import contracts, warehouse
from include.lib.notify import notify
from projects.commerce.lib import sql

DEFAULT_ARGS = {
    "owner": "commerce",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=10),
}


def build_lines(ds: str) -> int:
    """Discounted and costed lines for the day, from the shared `int` models."""
    with warehouse.connect() as con:
        return int(con.execute(sql.read("economics_lines"), [ds]).fetchone()[0])


def roll_to_order(ds: str) -> int:
    """Collapse the lines to one row per order."""
    with warehouse.connect() as con:
        return int(con.execute(sql.read("economics_roll"), [ds]).fetchone()[0])


def apply_returns(ds: str) -> int:
    """Net matched returns off the order, per `int_net_sales_lines`.

    A return is netted against the order it came from, on the order's own date,
    not on the date the return arrived. That is what keeps a day's net revenue
    stable once the day is published.
    """
    with warehouse.connect() as con:
        return int(con.execute(sql.read("economics_returns"), [ds]).fetchone()[0])


def publish(ds: str) -> int:
    """Replace the day's partition of `marts.order_economics`."""
    with warehouse.connect() as con:
        return int(con.execute(sql.read("economics_publish"), [ds]).fetchone()[0])


def check_contract(ds: str) -> int:
    """The mart against `contracts/order_economics.yml`.

    The contract is machine-readable and `plat_contracts_enforce` checks the
    whole table every morning. This checks the day this run wrote, so a break
    is attributed to the run that made it.
    """
    violations = contracts.check("order_economics")
    if violations:
        lines = "; ".join(f"{v.rule} on {v.column or 'the grain'}" for v in violations[:5])
        raise ValueError(f"{ds}: marts.order_economics breaks its contract: {lines}")
    return 0


def tie_to_orders(ds: str) -> int:
    """Booked cents against `raw.orders` for the day, cent exact."""
    with warehouse.connect(read_only=True) as con:
        booked, ordered = con.execute(sql.read("economics_tie"), [ds, ds]).fetchone()
    if booked != ordered:
        raise ValueError(
            f"{ds}: the mart books {booked} cents and the order table totals "
            f"{ordered}. Every reader downstream takes this as revenue."
        )
    return int(booked or 0)


with DAG(
    dag_id="order_economics_daily",
    schedule="30 4 * * *",
    start_date=pendulum.datetime(2024, 2, 4, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["commerce", "mart", "economics"],
    doc_md=__doc__,
    on_failure_callback=notify("commerce", "order economics"),
) as dag:
    lines = PythonOperator(
        task_id="build_lines", python_callable=build_lines,
        op_kwargs={"ds": "{{ ds }}"})
    roll = PythonOperator(
        task_id="roll_to_order", python_callable=roll_to_order,
        op_kwargs={"ds": "{{ ds }}"})
    returns = PythonOperator(
        task_id="apply_returns", python_callable=apply_returns,
        op_kwargs={"ds": "{{ ds }}"})
    write = PythonOperator(
        task_id="publish_economics", python_callable=publish,
        op_kwargs={"ds": "{{ ds }}"})
    contract = PythonOperator(
        task_id="check_contract", python_callable=check_contract,
        op_kwargs={"ds": "{{ ds }}"})
    tie = PythonOperator(
        task_id="tie_to_orders", python_callable=tie_to_orders,
        op_kwargs={"ds": "{{ ds }}"})

    lines >> roll >> returns >> write >> contract >> tie
