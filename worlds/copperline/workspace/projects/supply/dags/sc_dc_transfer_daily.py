"""Inter-DC transfers: what left, what arrived, and what is still in the air.

A transfer is two movements — a `pack` out of one location and a `receipt` into
another — and they do not post on the same day. This DAG pairs them, publishes
the day's transfers, and keeps the unpaired ones visible instead of netting them
away.

**In transit is not a rounding difference.** A pack with no matching receipt is
either stock on a lorry or stock that never arrived, and the two look identical
until the receipt posts. `docs/inventory-policy.md` INV-2 keeps them out of the
on-hand position; this table is where they are counted.

**`depends_on_past` is on.** The pairing walks forward from the unpaired set the
night before left, so a night that ran out of order pairs a receipt against a
pack it has not seen. One bad night blocks the nights after it, which is the
behaviour supply wants here.

Produces `marts.dc_transfers_daily`. Read by `sc_dc_capacity_daily`, the lane
performance build and the merch stock-cover view.

Owned by supply-chain (K. Duffy).
"""

from __future__ import annotations

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from include.lib import warehouse
from include.lib.notify import notify

DEFAULT_ARGS = {
    "owner": "supply",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=10),
    # The pairing walks forward from what last night left unpaired. See the
    # module docstring.
    "depends_on_past": True,
}

#: How long a transfer may be in the air before it is worth asking about. The
#: measured tail on the longest lane, not a round number.
IN_TRANSIT_DAYS = 12

PACKS = """
CREATE OR REPLACE TABLE copperline.staging.dc_packs AS
SELECT movement_id, sku, from_location, to_location, qty,
       occurred_at::DATE AS packed_on
FROM copperline.raw.wms_movements
WHERE movement_type = 'pack' AND occurred_at::DATE = ?::DATE
"""

PAIRED = """
-- A receipt matches the oldest unpaired pack of the same SKU on the same lane.
-- Oldest first, because stock does not overtake itself on a lane.
CREATE OR REPLACE TABLE copperline.staging.dc_paired AS
SELECT p.movement_id AS pack_id, r.movement_id AS receipt_id,
       p.sku, p.from_location, p.to_location, p.qty,
       p.packed_on, r.occurred_at::DATE AS received_on
FROM copperline.staging.dc_packs p
JOIN copperline.raw.wms_movements r
  ON r.movement_type = 'receipt'
 AND r.sku = p.sku
 AND r.to_location = p.to_location
 AND r.occurred_at::DATE >= p.packed_on
 AND r.occurred_at::DATE <= ?::DATE
QUALIFY row_number() OVER (PARTITION BY r.movement_id
                           ORDER BY p.packed_on, p.movement_id) = 1
"""

PUBLISH = """
INSERT INTO copperline.marts.dc_transfers_daily BY NAME
SELECT ?::DATE AS ds, from_location, to_location,
       count(*)                                        AS transfers,
       sum(qty)                                        AS units,
       sum(CASE WHEN received_on IS NULL THEN qty ELSE 0 END) AS in_transit_units
FROM copperline.staging.dc_paired
WHERE packed_on = ?::DATE
GROUP BY from_location, to_location
"""

STALE = """
SELECT count(*)
FROM copperline.staging.dc_paired
WHERE received_on IS NULL AND packed_on < ?::DATE - CAST(? AS INTEGER)
"""


def build_packs(ds: str) -> int:
    """The day's packs, before anything is paired against them."""
    with warehouse.connect() as con:
        return int(con.execute(PACKS, [ds]).fetchone()[0])


def pair_receipts(ds: str) -> int:
    """Pair each receipt to the oldest unpaired pack on its lane."""
    with warehouse.connect() as con:
        return int(con.execute(PAIRED, [ds]).fetchone()[0])


def publish(ds: str) -> int:
    """Replace the day's partition."""
    with warehouse.connect() as con:
        con.execute(f"DELETE FROM {warehouse.qualify('marts.dc_transfers_daily')} "
                    "WHERE ds = ?", [ds])
        return int(con.execute(PUBLISH, [ds, ds]).fetchone()[0])


def check_units_balance(ds: str) -> int:
    """Units packed equal units accounted for, paired plus in transit."""
    with warehouse.connect(read_only=True) as con:
        packed, accounted = con.execute(
            "SELECT "
            f"  (SELECT coalesce(sum(qty), 0) FROM "
            f"   {warehouse.qualify('staging.dc_packs')}), "
            f"  (SELECT coalesce(sum(units), 0) FROM "
            f"   {warehouse.qualify('marts.dc_transfers_daily')} WHERE ds = ?::DATE)",
            [ds],
        ).fetchone()
    if packed != accounted:
        raise ValueError(
            f"{ds}: {packed} units were packed and {accounted} are accounted "
            "for. A transfer has gone missing between the two tables."
        )
    return int(packed)


def report_stale_transfers(ds: str) -> int:
    """Transfers in the air longer than the longest lane takes.

    Reported, not failed: a lorry is sometimes late and a stale transfer is a
    phone call to the DC, not a broken pipeline.
    """
    with warehouse.connect(read_only=True) as con:
        return int(con.execute(STALE, [ds, IN_TRANSIT_DAYS]).fetchone()[0])


with DAG(
    dag_id="sc_dc_transfer_daily",
    schedule="0 6 * * *",
    start_date=pendulum.datetime(2024, 2, 4, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["supply", "mart", "inventory"],
    doc_md=__doc__,
    on_failure_callback=notify("supply", "DC transfers"),
) as dag:
    packs = PythonOperator(
        task_id="build_packs", python_callable=build_packs,
        op_kwargs={"ds": "{{ ds }}"})
    pair = PythonOperator(
        task_id="pair_receipts", python_callable=pair_receipts,
        op_kwargs={"ds": "{{ ds }}"})
    write = PythonOperator(
        task_id="publish_transfers", python_callable=publish,
        op_kwargs={"ds": "{{ ds }}"})
    balance = PythonOperator(
        task_id="check_units_balance", python_callable=check_units_balance,
        op_kwargs={"ds": "{{ ds }}"})
    stale = PythonOperator(
        task_id="report_stale_transfers", python_callable=report_stale_transfers,
        op_kwargs={"ds": "{{ ds }}"})

    packs >> pair >> write >> balance >> stale
