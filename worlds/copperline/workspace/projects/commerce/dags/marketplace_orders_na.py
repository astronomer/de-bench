"""North American marketplace orders into `raw.marketplace_orders`.

Copperline runs its own marketplace and takes a commission on what sellers ship.
This is the operator's own order listing for the North American region, read
hourly off the marketplace API. `marketplace_orders_eu` is the same DAG for
Europe: same shape, same retry policy, same staging table, same merge key, same
checks. A third region is a copy of either one with `REGION` changed.

**The listing pages and it does not say how far.** Every page carries at most
fifty rows and a `next_page_token`, `total` is always null, and the token is
absent only on the last page. `JsonApiToWarehouseOperator` follows the token to
the end; anything that calls the API by hand pages the same way, and a
first-page answer looks exactly like a complete one.

**The grain is a key, not a date.** An order is amended after it is placed, so
the same `marketplace_order_id` comes back on a later listing with different
figures. The stage is emptied and refilled by the run that owns it, and the
merge keys on the order id, which is what makes a second run of an hour leave
one row rather than two.

The recognition date on a marketplace order is `ship_confirmed_at`, not
`placed_at` — the seller confirms one to six days after the order, and a month
boundary sits between the two on a few per cent of rows every month.
`docs/finance-policy.md` REV-14 owns that, and `gmv_cents` is the seller's order
value and never Copperline's revenue.

Produces the North American rows of `raw.marketplace_orders`. Read by
`marketplace_settlement_intake`, `marts.gmv_daily` and the board pack.

Owned by commerce (J. Mwangi).
"""

from __future__ import annotations

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from include.lib import warehouse
from include.lib.loaders import JsonApiToWarehouseOperator
from include.lib.notify import notify
from projects.commerce.lib import sql

DEFAULT_ARGS = {
    "owner": "commerce",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
}

#: The region this sibling owns. `marketplace_orders_eu` sets "eu" and changes
#: nothing else.
REGION = "na"

#: The table this run fills and then merges from. One per region, so the two
#: siblings never touch each other's rows.
STAGE = f"staging.mkt_orders_{REGION}"

#: The columns the JSON sniffer guesses badly. Both timestamps matter and every
#: money column is integer cents on the wire.
ORDER_COLUMNS = {
    "marketplace_order_id": "VARCHAR",
    "order_id": "BIGINT",
    "gmv_cents": "BIGINT",
    "commission_cents": "BIGINT",
    "fulfilment_fee_cents": "BIGINT",
    "commission_rate_bps": "INTEGER",
    "placed_at": "TIMESTAMP",
    "ship_confirmed_at": "TIMESTAMP",
    "loaded_at": "TIMESTAMP",
}


def reset_stage() -> str:
    """Empty this region's stage before the listing fills it.

    The load appends, which is right into a table the run has just taken over
    and wrong into one it shares. This is what makes it right.
    """
    with warehouse.connect() as con:
        con.execute("CREATE SCHEMA IF NOT EXISTS staging")
        con.execute(f"DROP TABLE IF EXISTS {warehouse.qualify(STAGE)}")
    return STAGE


def merge_orders(region: str) -> int:
    """Merge the stage into `raw.marketplace_orders` on the order id."""
    with warehouse.connect() as con:
        return int(con.execute(
            sql.read("marketplace_orders_merge"), [f"staging.mkt_orders_{region}"],
        ).fetchone()[0])


def check_page_completeness(region: str, ds: str) -> int:
    """The load against the feed that does report a count.

    The listing reports no total, so this compares against the settlement feed,
    which does: every confirmed order in a paid week carries settlement lines. A
    short load shows up as orders the settlement feed names and this table does
    not have.
    """
    with warehouse.connect(read_only=True) as con:
        missing = int(con.execute(
            sql.read("marketplace_orders_missing"), [ds, region],
        ).fetchone()[0])
    if missing:
        raise ValueError(
            f"{ds} {region}: the settlement feed names {missing} orders this "
            "load does not have. Follow next_page_token to the last page."
        )
    return missing


def record_intake(region: str, ds: str) -> int:
    """The region's loaded count for the day, into `ops.intake_log`."""
    with warehouse.connect() as con:
        con.execute("CREATE SCHEMA IF NOT EXISTS ops")
        con.execute(sql.read("intake_log_ddl"))
        rows = int(con.execute(
            sql.read("marketplace_orders_day_count"), [ds, region],
        ).fetchone()[0])
        warehouse.delete_insert(
            "ops.intake_log", "ds", ds,
            [{"ds": ds, "source": f"marketplace_orders_{region}", "row_count": rows}],
            con=con,
        )
    return rows


with DAG(
    dag_id=f"marketplace_orders_{REGION}",
    schedule="10 * * * *",
    start_date=pendulum.datetime(2024, 2, 4, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["commerce", "raw", "intake", "marketplace", REGION],
    doc_md=__doc__,
    on_failure_callback=notify("commerce", f"marketplace orders {REGION}"),
) as dag:
    stage = PythonOperator(
        task_id="reset_stage",
        python_callable=reset_stage,
    )

    load = JsonApiToWarehouseOperator(
        task_id="load_orders",
        endpoint="marketplace/orders",
        params={"region": REGION, "since": "{{ ds }}"},
        table=STAGE,
        columns=ORDER_COLUMNS,
    )

    merge = PythonOperator(
        task_id="merge_orders",
        python_callable=merge_orders,
        op_kwargs={"region": REGION},
    )

    pages = PythonOperator(
        task_id="check_page_completeness",
        python_callable=check_page_completeness,
        op_kwargs={"region": REGION, "ds": "{{ ds }}"},
    )

    log_intake = PythonOperator(
        task_id="record_intake",
        python_callable=record_intake,
        op_kwargs={"region": REGION, "ds": "{{ ds }}"},
    )

    stage >> load >> merge >> pages >> log_intake
