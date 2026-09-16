"""On-hand by SKU and location, from the Corvid WMS snapshot.

Two counts arrive. The nightly file is the A-class count, the fast movers Corvid
recounts every night. Once a week a whole-network file lands beside it and
carries the long tail. This DAG takes whichever are there and publishes one
position per SKU per location per day.

**The snapshot and the movement log disagree, and that is upstream's doing.**
Corvid writes the snapshot from its own count and movements post
asynchronously, so replaying the movements to a date does not reproduce the
snapshot. `docs/inventory-policy.md` INV-2 says which one the warehouse
believes: the position comes from `int_inventory_position_daily` and every
downstream team reads the position, never the movements.

**The valuation columns change meaning at the FY2026 boundary.** Before it,
`unit_cost_cents` is populated and the other two are null; from it,
`retail_value_cents` and `cost_complement_bps` are populated and
`unit_cost_cents` is null. `docs/inventory-policy.md` INV-1 is the only place
that says what the change was. This DAG lands both shapes and does not choose.

Produces `raw.inventory_snapshots` and `marts.inventory_position`. Read by the
replenishment feed, the merch dashboards, `sc_backorder_daily` and
`sc_dc_capacity_daily`.

Owned by supply-chain (K. Duffy).
"""

from __future__ import annotations

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from include.lib import contracts, warehouse
from include.lib.loaders import CsvToWarehouseOperator
from include.lib.notify import notify
from projects.supply.lib import paths

DEFAULT_ARGS = {
    "owner": "supply",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=10),
}

SNAPSHOT_COLUMNS = {
    "snapshot_date": "DATE",
    "sku": "VARCHAR",
    "location_id": "VARCHAR",
    "dept_code": "VARCHAR",
    "on_hand_units": "DECIMAL(12,3)",
    "reserved_units": "DECIMAL(12,3)",
    "in_transit_units": "DECIMAL(12,3)",
    "unit_cost_cents": "BIGINT",
    "retail_value_cents": "BIGINT",
    "cost_complement_bps": "INTEGER",
    "last_counted_at": "TIMESTAMP",
    "loaded_at": "TIMESTAMP",
}

NETWORK_SNAPSHOT = """
-- The weekly whole-network count, where it landed. It carries the long tail of
-- SKUs the nightly A-class file leaves out, so a SKU that appears in both takes
-- the nightly count and a SKU that appears only here takes this one.
INSERT OR REPLACE INTO copperline.raw.inventory_snapshots BY NAME
SELECT * FROM read_csv(?, header = true, union_by_name = true)
"""

POSITION = """
-- One position per SKU per location for the day, with the demand signal beside
-- it. `demand_units` is units ordered, with returns NOT deducted:
-- contracts/replenishment.md RP-1 and docs/semantic-definitions.md both say so,
-- and netting returns understates demand for the SKUs that sell most.
INSERT INTO copperline.marts.inventory_position BY NAME
SELECT s.snapshot_date AS ds, s.sku, s.location_id,
       s.on_hand_units::INTEGER  AS on_hand_units,
       coalesce(d.demand_units, 0)::INTEGER AS demand_units
FROM copperline.raw.inventory_snapshots s
LEFT JOIN (
    SELECT l.sku, o.store_id AS location_id, sum(l.qty) AS demand_units
    FROM copperline.raw.order_lines l
    JOIN copperline.raw.orders o ON o.order_id = l.order_id
    WHERE o.local_order_date = ?::DATE
    GROUP BY l.sku, o.store_id
) d ON d.sku = s.sku AND d.location_id = s.location_id
WHERE s.snapshot_date = ?::DATE
"""

COVERAGE = """
-- SKU-locations the movement log knows about that the snapshot does not carry.
-- The two disagree by design, but a SKU that moved and is in no count at all is
-- a count that missed a bin rather than the usual drift.
SELECT count(*)
FROM (
    SELECT DISTINCT m.sku, m.to_location AS location_id
    FROM copperline.raw.wms_movements m
    WHERE m.occurred_at::DATE = ?::DATE AND m.to_location IS NOT NULL
    EXCEPT
    SELECT sku, location_id FROM copperline.raw.inventory_snapshots
     WHERE snapshot_date = ?::DATE
)
"""


VALUATION = """
-- Stock value for the day, by the method in force on it. The boundary is the
-- first day of FY2026 and it is stated in docs/inventory-policy.md INV-1, not
-- in the data: the snapshot announces the change by which columns go null and
-- names nothing.
INSERT INTO copperline.marts.fct_inventory_valuation BY NAME
SELECT s.snapshot_date AS ds, s.dept_code, s.location_id,
       sum(s.on_hand_units)                                    AS on_hand_units,
       CASE WHEN s.snapshot_date < DATE '2026-02-01'
            THEN round(sum(s.on_hand_units * s.unit_cost_cents))
            ELSE round(sum(s.retail_value_cents)
                       * max(c.cost_complement_bps) / 10000.0)
       END                                                     AS cost_cents
FROM raw.inventory_snapshots s
LEFT JOIN copperline.raw.dept_cost_complement c
       ON c.dept_code = s.dept_code
      AND c.effective_from <= s.snapshot_date
WHERE s.snapshot_date = ?::DATE
GROUP BY s.snapshot_date, s.dept_code, s.location_id
"""


def load_network_snapshot(ds: str) -> int:
    """Take the weekly whole-network count, on the night it lands."""
    path = paths.wms_network_snapshot(ds)
    if not path.exists():
        return 0
    with warehouse.connect() as con:
        return int(con.execute(NETWORK_SNAPSHOT, [str(path)]).fetchone()[0])


def build_position(ds: str) -> int:
    """Replace the day's partition of `marts.inventory_position`."""
    with warehouse.connect() as con:
        con.execute(f"DELETE FROM {warehouse.qualify('marts.inventory_position')} "
                    "WHERE ds = ?", [ds])
        return int(con.execute(POSITION, [ds, ds]).fetchone()[0])


def check_coverage(ds: str) -> int:
    """SKU-locations that moved and are in no count. Reported, not failed:
    the counts and the movements disagree by design and a bin missed by one
    night's count is caught by the weekly network file."""
    with warehouse.connect(read_only=True) as con:
        return int(con.execute(COVERAGE, [ds, ds]).fetchone()[0])


def build_valuation(ds: str) -> int:
    """Value the day's stock by the method in force on that day.

    Before 2026-02-01 that is weighted-average cost, per SKU. From it, it is the
    retail method at DEPARTMENT grain — the complement is a department ratio and
    there is no per-SKU cost behind it, so a per-SKU margin from FY2026 data is
    computing something the method does not define. INV-3 rounds once, per
    department, after the complement is applied.

    The two methods do not agree and INV-4 says nothing is restated across the
    boundary. A year-on-year comparison over it compares two measures.
    """
    with warehouse.connect() as con:
        con.execute(f"DELETE FROM {warehouse.qualify('marts.fct_inventory_valuation')} "
                    "WHERE ds = ?", [ds])
        return int(con.execute(VALUATION, [ds]).fetchone()[0])


def check_contract(ds: str) -> int:
    """`marts.inventory_position` against `contracts/inventory_position.yml`.

    The contract's `non_negative` rule is the one that matters here: Ironwood
    raises purchase orders off this feed, and a negative position asks it to buy
    stock the building already has.
    """
    violations = contracts.check("inventory_position")
    if violations:
        lines = "; ".join(f"{v.rule} on {v.column or 'the grain'}" for v in violations[:5])
        raise ValueError(f"{ds}: marts.inventory_position breaks its contract: {lines}")
    return 0


with DAG(
    dag_id="sc_inventory_snapshot_daily",
    schedule="0 2 * * *",
    start_date=pendulum.datetime(2024, 2, 4, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["supply", "raw", "mart", "inventory"],
    doc_md=__doc__,
    on_failure_callback=notify("supply", "inventory snapshot"),
) as dag:
    load_aclass = CsvToWarehouseOperator(
        task_id="load_aclass_snapshot",
        source=paths.WMS_SNAPSHOT_TEMPLATE,
        table="raw.inventory_snapshots",
        mode="replace",
        partition_col="snapshot_date",
        columns=SNAPSHOT_COLUMNS,
    )

    load_network = PythonOperator(
        task_id="load_network_snapshot", python_callable=load_network_snapshot,
        op_kwargs={"ds": "{{ ds }}"})
    position = PythonOperator(
        task_id="build_position", python_callable=build_position,
        op_kwargs={"ds": "{{ ds }}"})
    coverage = PythonOperator(
        task_id="check_coverage", python_callable=check_coverage,
        op_kwargs={"ds": "{{ ds }}"})
    contract = PythonOperator(
        task_id="check_contract", python_callable=check_contract,
        op_kwargs={"ds": "{{ ds }}"})
    valuation = PythonOperator(
        task_id="build_valuation", python_callable=build_valuation,
        op_kwargs={"ds": "{{ ds }}"})

    load_aclass >> load_network >> position >> coverage >> contract >> valuation
