"""The enriched order spine: `staging.orders_enriched`, one day at a time.

Joins the day's orders to the customer book, the product book and the channel
dimension, and lands one row per order with the attributes every downstream
model expects to find already resolved. Three joins are era-aware and the
docstrings on the steps say which era each one turns on.

Produces `staging.orders_enriched`. Read by `order_economics_daily`,
`channel_daily_intake`, the finance margin build, and the merch dashboards.

Owned by commerce (J. Mwangi). The 03:00 slot is after the OMS cut and before
the 04:00 dbt build, and moving it moves both.
"""

from __future__ import annotations

import pendulum
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from include.lib import warehouse
from include.lib.notify import notify
from projects.commerce.lib import sql

DEFAULT_ARGS = {
    "owner": "commerce",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=10),
}


def run(statement: str, ds: str, *extra: object) -> int:
    """Run one commerce statement for a day and return its first column.

    Every step below is one statement over one day's rows, so they all take the
    same shape. `warehouse.connect()` is the only way into the warehouse this
    project has, and it qualifies the names it is given.
    """
    with warehouse.connect() as con:
        row = con.execute(sql.read(statement), [ds, *extra]).fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def build_base(ds: str) -> int:
    """The day's orders, less the test rows, into the enrichment base.

    Soft-deleted orders stay: `docs/reconciliation-policy.md` R-6 says a row the
    source deleted is a row that still has to be accounted for.
    """
    return run("enrich_base", ds)


def join_customer(ds: str) -> int:
    """Resolve `customer_ref` to a customer key.

    Trade orders carry a reference in one of two formats and the crosswalk is
    keyed by format, not by date. `docs/runbooks/customer-id-migration.md` is
    the description of the two.
    """
    return run("enrich_customer", ds)


def join_product(ds: str) -> int:
    """Attach the product attributes in force on the order date, from the
    snapshot pair `product_snapshot_daily` maintains."""
    return run("enrich_product", ds)


def join_channel(ds: str) -> int:
    """Attach the channel and market attributes. Four channels: store, web,
    marketplace and trade."""
    return run("enrich_channel", ds)


def join_tender(ds: str) -> int:
    """Roll the day's payment attempts up to one tender state per order.

    The processor is the authority on payment state, per
    `docs/reconciliation-policy.md` R-1, so this reads the settlement feeds and
    not the order's own status.
    """
    return run("enrich_tender", ds)


def publish(ds: str) -> int:
    """Replace the day's partition of `staging.orders_enriched`."""
    return run("enrich_publish", ds)


def check_grain(ds: str) -> int:
    """One row per order, and every order in. Both directions, because a join
    that fans out and a join that drops rows look the same in a total."""
    with warehouse.connect(read_only=True) as con:
        orders, enriched, repeated = con.execute(
            sql.read("enrich_grain_check"), [ds, ds, ds],
        ).fetchone()
    if repeated:
        raise ValueError(f"{ds}: {repeated} orders appear more than once after the joins")
    if orders != enriched:
        raise ValueError(
            f"{ds}: {orders} orders went in and {enriched} came out; a join is "
            "dropping rows rather than leaving the attribute null"
        )
    return int(enriched)


with DAG(
    dag_id="orders_enrich_daily",
    schedule="0 3 * * *",
    start_date=pendulum.datetime(2024, 2, 4, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["commerce", "staging"],
    doc_md=__doc__,
    on_failure_callback=notify("commerce", "order enrichment"),
) as dag:
    base = PythonOperator(
        task_id="build_base",
        python_callable=build_base,
        op_kwargs={"ds": "{{ ds }}"},
    )

    customer = PythonOperator(
        task_id="join_customer",
        python_callable=join_customer,
        op_kwargs={"ds": "{{ ds }}"},
    )

    product = PythonOperator(
        task_id="join_product",
        python_callable=join_product,
        op_kwargs={"ds": "{{ ds }}"},
    )

    channel = PythonOperator(
        task_id="join_channel",
        python_callable=join_channel,
        op_kwargs={"ds": "{{ ds }}"},
    )

    tender = PythonOperator(
        task_id="join_tender",
        python_callable=join_tender,
        op_kwargs={"ds": "{{ ds }}"},
    )

    joined = EmptyOperator(task_id="joins_complete")

    write = PythonOperator(
        task_id="publish_enriched",
        python_callable=publish,
        op_kwargs={"ds": "{{ ds }}"},
    )

    grain = PythonOperator(
        task_id="check_grain",
        python_callable=check_grain,
        op_kwargs={"ds": "{{ ds }}"},
    )

    base >> [customer, product, channel, tender] >> joined >> write >> grain
