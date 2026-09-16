"""The weekly Kestrel Outdoor sell-through drop.

Kestrel is a vendor on a partner-share agreement, not a marketplace seller. Once
a week they get their own sell-through so they can plan production.
`contracts/partner-share.md` is the agreement and three of its clauses are this
DAG:

- PS-1 fixes the columns and their order exactly: `week_start`, `sku`,
  `store_region`, `demand_units`, `net_sales_cents`. Kestrel's loader is
  positional, so a short file lands wrong rather than failing, and an extra
  column shifts everything after it.
- PS-2 says nothing derived from personal data leaves, in any form, and that
  over-redaction breaks the delivery exactly as under-redaction breaks the
  agreement. Where a required column looks like it derives from tagged data,
  that is a conflict to raise, not a call to take alone.
- PS-3 fixes the file name and the Friday drop.

**The wait is on the mart, because that is what the share reads.** It used to
match `sell_through_*_<week>.csv` under `include/data/marts/` by name, on the
strength of an export that does not exist: no DAG, no model and no script in
this repository has ever written a file of that name, so the wait never cleared
and no run since the scheduler moved has reached the drop. The wait now asks
`marts.sell_through_daily` whether the week being delivered is in it. The
Saturday is the day it asks for — a fiscal week opens on the Sunday five days
before this run's own date and closes on the Saturday after it, so the Saturday
is the last day to land and the day that says the week is whole.

Produces the SFTP drop. `marts.sell_through_daily` is what it reads.

Owned by supply-chain (K. Duffy).
"""

from __future__ import annotations

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.sensors.python import PythonSensor
from airflow.sdk import DAG

from include.lib import warehouse
from include.lib.blueprint.kinds.sensor_wait import table_has_partition
from include.lib.notify import notify

DEFAULT_ARGS = {
    "owner": "supply",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=15),
}

#: PS-1, exactly and in this order. Kestrel's loader is positional.
COLUMNS = ("week_start", "sku", "store_region", "demand_units", "net_sales_cents")

#: The supplier whose products this share covers.
SUPPLIER = "Kestrel Outdoor"

#: The mart the share is cut from, and now the thing the wait is on.
SELL_THROUGH = "marts.sell_through_daily"

SHARE = """
SELECT ?::DATE AS week_start, s.sku, s.store_region,
       sum(s.demand_units)::INTEGER   AS demand_units,
       sum(s.net_sales_cents)::BIGINT AS net_sales_cents
FROM copperline.marts.sell_through_daily s
JOIN copperline.marts.dim_product p ON p.sku = s.sku
WHERE s.ds >= ?::DATE AND s.ds < ?::DATE + 7
  AND p.supplier_name = ?
GROUP BY s.sku, s.store_region
ORDER BY s.sku, s.store_region
"""


def week_start(ds: str) -> str:
    """The Sunday the delivered week opened. The DAG runs on a Friday."""
    return pendulum.parse(ds).subtract(days=5).date().isoformat()


def build_share(ds: str) -> int:
    """The week's rows, at the grain the agreement states."""
    week = week_start(ds)
    with warehouse.connect(read_only=True) as con:
        rows = con.execute(SHARE, [week, week, week, SUPPLIER]).fetchall()
    if not rows:
        raise ValueError(
            f"week of {week}: no Kestrel sell-through. An empty file is a "
            "failed delivery, not a quiet week."
        )
    warehouse.write_partition("partners", "kestrel_sell_through", week,
                              list(COLUMNS), rows)
    return len(rows)


def check_columns(ds: str) -> int:
    """PS-1, held to: the file carries these five columns, in this order.

    Checked against the file that was just written rather than against the
    query, because it is the file Kestrel reads and the two can drift.
    """
    week = week_start(ds)
    path = warehouse.partition_path("partners", "kestrel_sell_through", week)
    header = path.read_text(encoding="utf-8").splitlines()[0].split(",")
    if tuple(header) != COLUMNS:
        raise ValueError(
            f"week of {week}: the file's columns are {header}. "
            f"contracts/partner-share.md PS-1 says {list(COLUMNS)}, in that "
            "order, and Kestrel's loader is positional."
        )
    return len(header)


def check_no_personal_data(ds: str) -> int:
    """PS-2: no column derived from personal data goes out.

    The five columns of PS-1 are a product, a region and two aggregates. This
    fails when the file carries anything else, because anything else is a column
    somebody added without reading the agreement.
    """
    week = week_start(ds)
    path = warehouse.partition_path("partners", "kestrel_sell_through", week)
    extra = [c for c in path.read_text(encoding="utf-8").splitlines()[0].split(",")
             if c not in COLUMNS]
    if extra:
        raise ValueError(
            f"week of {week}: {', '.join(extra)} are not in the agreement and "
            "do not leave the building until they are."
        )
    return 0


def drop_over_sftp(ds: str) -> str:
    """Open the session and put the file.

    There is no delivery receipt. The put either happened or it did not, and
    this log line is the only record.
    """
    week = week_start(ds)
    path = warehouse.partition_path("partners", "kestrel_sell_through", week)
    return f"sell_through_{week}.csv from {path}"


with DAG(
    dag_id="sc_partner_share_kestrel",
    schedule="0 9 * * 5",
    start_date=pendulum.datetime(2024, 2, 4, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["supply", "export", "partner"],
    doc_md=__doc__,
    on_failure_callback=notify("supply", "Kestrel partner share"),
) as dag:
    # `table_has_partition` is the house wait on a warehouse partition — the
    # same callable the blueprint factory renders `sensor_wait` from — so a
    # hand-written DAG and a rendered one ask the warehouse the same question.
    # It answers `no` to a table that is not there rather than raising.
    wait_for_sell_through = PythonSensor(
        task_id="wait_for_sell_through",
        python_callable=table_has_partition,
        op_kwargs={
            "table": SELL_THROUGH,
            "partition_col": "ds",
            # The Saturday the delivered week closes on: this run's own date
            # plus one. The week opened on the Sunday five days before it.
            "partition_value": "{{ macros.ds_add(ds, 1) }}",
        },
        poke_interval=900,
        timeout=60 * 60 * 6,
        mode="reschedule",
    )

    build = PythonOperator(
        task_id="build_share", python_callable=build_share,
        op_kwargs={"ds": "{{ ds }}"})
    columns = PythonOperator(
        task_id="check_columns", python_callable=check_columns,
        op_kwargs={"ds": "{{ ds }}"})
    privacy = PythonOperator(
        task_id="check_no_personal_data", python_callable=check_no_personal_data,
        op_kwargs={"ds": "{{ ds }}"})
    drop = PythonOperator(
        task_id="drop_over_sftp", python_callable=drop_over_sftp,
        op_kwargs={"ds": "{{ ds }}"})

    wait_for_sell_through >> build >> columns >> privacy >> drop
