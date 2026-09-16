"""S6a — Corvid WMS: inventory snapshots, movements, and the E8 complement.

Two files a day into `landing/wms/dt=<ds>/`, plus the weekly network snapshot
the long tail of SKUs gets. The warehouse tables carry the whole range; the
landing tree carries a window (see `extracts/_landing.py`).

**E8, 2026-02-01.** Copperline valued stock at weighted-average cost until the
first day of FY2026 and by the retail method after it. The snapshot announces
the change and does not explain it:

* `unit_cost_cents` is populated before the change and NULL from it;
* `retail_value_cents` and `cost_complement_bps` are NULL before and
  populated from it.

The two methods do not agree and are not meant to. The complement is set per
department per fiscal period, so `dept_cost_complement` is the only place the
FY2026 rule is written down at all — and applying weighted-average cost to a
FY2026 department understates its stock value by between 6 and 9 per cent,
because that is the spread this module builds the retail prices to. Which
method the warehouse should believe is in `docs/inventory-policy.md` and
nowhere in the data.

**The snapshot and the movement log disagree, and that is upstream's doing.**
Corvid writes the snapshot from its own count; movements post asynchronously.
Replaying the movements to a date does not reproduce the snapshot. This module
carries that disagreement through rather than reconciling it.

**Vocabulary follows the sender.** Corvid's movement words are not the
application's: an application `sale` is a WMS `pick`, a `shrink` is what a
`cycle_count` found, a `return` goes out again as `rtv`. Nothing translates
them on the way in, which is extract convention 5.
"""

from __future__ import annotations

from .. import streams
from ..config import Context
from . import _landing

# The five departments chapter 02 fixes, and the cost complement each one
# runs at from E8, in basis points of retail. A complement of 6,400 means
# stock costs 64% of what it retails for.
DEPARTMENTS = ("HDW", "BLD", "GDN", "OUT", "HOM")
DEPT_COMPLEMENT_BPS = {"HDW": 6300, "BLD": 6850, "GDN": 5950,
                       "OUT": 6120, "HOM": 6480}
# How far the retail method's answer sits above the weighted-average one, per
# department, in basis points. Spec chapter 03 section 11: 6 to 9 per cent.
DEPT_UPLIFT_BPS = {"HDW": 640, "BLD": 880, "GDN": 720, "OUT": 610, "HOM": 830}

APPROVERS = ("m.okafor", "r.delacroix", "s.whitfield", "j.tanaka", "p.oyelaran")

# The Corvid words for the application's movement kinds.
MOVEMENT_WORD = {
    "sale": "pick", "receipt": "receipt", "return": "rtv",
    "transfer_out": "pack", "transfer_in": "receipt",
    "adjustment": "adjust", "shrink": "cycle_count",
}

# The A-class snapshot is 2,880 rows a day and the network snapshot ten times
# that, so the full 400-day RET-2 window would be hundreds of megabytes of
# CSV in every trial. Twenty-one days plus the key dates is the same tree at a
# size setup can pay for. See extracts/_landing.py.
LANDING_DAYS = 21

E8_AT = "2026-02-01"


def _h(ctx: Context, stream: str, *parts: str) -> str:
    return streams.draw(ctx.seed, f"'{stream}'", *parts)


def build(ctx: Context) -> None:
    from ._boundary import ensure_keys_or_stub

    ensure_keys_or_stub(ctx)  # movement order refs come from _util.order_keys
    _complement(ctx)
    _snapshots(ctx)
    _movements(ctx)
    _files(ctx)


def _case(mapping: dict, column: str, default: str) -> str:
    arms = " ".join(f"WHEN '{key}' THEN {value!r}" if isinstance(value, str)
                    else f"WHEN '{key}' THEN {value}"
                    for key, value in mapping.items())
    return f"(CASE {column} {arms} ELSE {default} END)"


# --- raw.dept_cost_complement ---------------------------------------------

def _complement(ctx: Context) -> None:
    """The FY2026 retail-method complement, department by fiscal period. It is
    the only statement of the new rule's numbers anywhere in the fixtures."""
    from ..upstream._refs import fiscal_periods

    ctx.sql("""
        CREATE OR REPLACE TABLE raw.dept_cost_complement (
            dept_code VARCHAR NOT NULL,
            fiscal_period VARCHAR NOT NULL,
            cost_complement_bps INTEGER NOT NULL,
            approved_by VARCHAR NOT NULL,
            effective_from DATE NOT NULL,
            PRIMARY KEY (dept_code, fiscal_period)
        )
    """)
    rows = []
    periods = [p for p in fiscal_periods(2026, 2026)]
    for dept in DEPARTMENTS:
        base = DEPT_COMPLEMENT_BPS[dept]
        for n, (period_id, start, _end) in enumerate(periods):
            # The complement drifts a little period to period, the way a
            # buying team's markup does. It never crosses a department.
            drift = ((n * 37 + len(dept) * 11) % 21) - 10
            rows.append((dept, f"FY2026-P{period_id % 100:02d}", base + drift,
                         APPROVERS[n % len(APPROVERS)], start))
    ctx.con.executemany(
        "INSERT INTO raw.dept_cost_complement VALUES (?,?,?,?,?)", rows)


# --- raw.inventory_snapshots ----------------------------------------------

def _snapshots(ctx: Context) -> None:
    """One row per (snapshot date, sku, location). The A-class SKUs snapshot
    daily over the trailing ninety days, the rest weekly; both populations sit
    in one table, which is what the upstream balance table already holds."""
    ctx.sql("""
        CREATE OR REPLACE TABLE raw.inventory_snapshots (
            snapshot_date DATE NOT NULL,
            sku VARCHAR NOT NULL,
            location_id VARCHAR NOT NULL,
            dept_code VARCHAR NOT NULL,
            on_hand_units DECIMAL(12,3) NOT NULL,
            reserved_units DECIMAL(12,3) NOT NULL,
            in_transit_units DECIMAL(12,3) NOT NULL,
            unit_cost_cents BIGINT,
            retail_value_cents BIGINT,
            cost_complement_bps INTEGER,
            last_counted_at TIMESTAMP,
            loaded_at TIMESTAMP NOT NULL,
            PRIMARY KEY (snapshot_date, sku, location_id)
        )
    """)
    cost = _h(ctx, "raw.inventory_snapshots.cost", "v.variant_id")
    counted = _h(ctx, "raw.inventory_snapshots.counted", "b.balance_id")
    lag = _h(ctx, "raw.inventory_snapshots.lag", "b.as_of_datetime::DATE")
    complement = _case(DEPT_COMPLEMENT_BPS, "c.dept_code", 6400)
    uplift = _case(DEPT_UPLIFT_BPS, "c.dept_code", 700)
    # A standard cost per SKU, in cents, stable for the whole range. Retail is
    # set from it so that (retail x complement) is the cost plus the
    # department's uplift — which is the 6 to 9 per cent the two methods
    # differ by, and the whole of the valuation question.
    unit_cost = f"(210 + ({cost} % 22000))::BIGINT"
    unit_retail = (f"(({unit_cost} * (10000 + {uplift})) / {complement})::BIGINT")
    ctx.sql(f"""
        INSERT INTO raw.inventory_snapshots
        SELECT b.as_of_datetime::DATE,
               v.sku,
               coalesce(w.warehouse_code, 'S-' || lpad(b.store_id::VARCHAR, 4, '0')),
               c.dept_code,
               b.qty_on_hand, b.qty_reserved, b.qty_in_transit,
               CASE WHEN b.as_of_datetime::DATE < DATE '{E8_AT}'
                    THEN {unit_cost} END,
               CASE WHEN b.as_of_datetime::DATE >= DATE '{E8_AT}'
                    THEN (b.qty_on_hand * {unit_retail})::BIGINT END,
               CASE WHEN b.as_of_datetime::DATE >= DATE '{E8_AT}'
                    THEN {complement} END,
               b.as_of_datetime - INTERVAL 1 DAY * (1 + ({counted}) % 89)
                   - INTERVAL 1 HOUR * (({counted} >> 20) % 9),
               b.as_of_datetime::DATE::TIMESTAMP + INTERVAL 1 DAY
                   + INTERVAL 4 HOUR + INTERVAL 1 MINUTE * (({lag}) % 55)
        FROM sim_inventory.inventory_balances b
        JOIN sim_product.product_variants v ON v.variant_id = b.variant_id
        JOIN sim_product.products p ON p.product_id = v.product_id
        JOIN sim_product.categories c ON c.category_id = p.category_id
        LEFT JOIN sim_inventory.warehouses w ON w.warehouse_id = b.warehouse_id
        -- The weekly network snapshot and the daily A-class one can both run
        -- on the same date for the same SKU at the same site. Corvid writes
        -- the count once; keep one row, so the snapshot has a key.
        QUALIFY row_number() OVER (
            PARTITION BY b.as_of_datetime::DATE, v.sku,
                         coalesce(w.warehouse_code,
                                  'S-' || lpad(b.store_id::VARCHAR, 4, '0'))
            ORDER BY b.balance_id) = 1
        ORDER BY 1, 3, 2
    """)


# --- raw.wms_movements -----------------------------------------------------

def _movements(ctx: Context) -> None:
    ctx.sql("""
        CREATE OR REPLACE TABLE raw.wms_movements (
            movement_id VARCHAR NOT NULL PRIMARY KEY,
            occurred_at TIMESTAMP NOT NULL,
            sku VARCHAR NOT NULL,
            from_location VARCHAR,
            to_location VARCHAR,
            qty DECIMAL(12,3) NOT NULL,
            movement_type VARCHAR NOT NULL,
            reference_type VARCHAR NOT NULL,
            reference_id VARCHAR NOT NULL,
            operator_id VARCHAR NOT NULL,
            loaded_at TIMESTAMP NOT NULL
        )
    """)
    word = _case(MOVEMENT_WORD, "m.movement_type", "'adjust'")
    lag = _h(ctx, "raw.wms_movements.lag", "m.movement_id")
    # An order_line reference resolves to the shipped order plus its line
    # slot: the sim cites the day-block line identity, and the boundary maps
    # its parent order through _util.order_keys like every other module.
    reference = (
        "CASE m.reference_type "
        "WHEN 'order_line' THEN ok.raw_order_id || '/L-' || "
        "lpad(((m.reference_id - 1000000000) % 16)::VARCHAR, 2, '0') "
        "WHEN 'receipt_line' THEN 'RCP-' || lpad(m.reference_id::VARCHAR, 8, '0') "
        "WHEN 'transfer_line' THEN 'TR-' || m.reference_id::VARCHAR "
        "ELSE 'CNT-' || lpad(m.reference_id::VARCHAR, 8, '0') END"
    )
    location = (
        "CASE WHEN {wh} IS NOT NULL THEN {code} "
        "WHEN {st} IS NOT NULL THEN 'S-' || lpad({st}::VARCHAR, 4, '0') END"
    )
    ctx.sql(f"""
        INSERT INTO raw.wms_movements
        SELECT 'MV-' || lpad(m.movement_id::VARCHAR, 12, '0'),
               m.moved_at,
               v.sku,
               {location.format(wh='m.from_warehouse_id', code='wf.warehouse_code',
                                st='m.from_store_id')},
               {location.format(wh='m.to_warehouse_id', code='wt.warehouse_code',
                                st='m.to_store_id')},
               m.qty,
               {word},
               m.reference_type,
               {reference},
               'OP-' || lpad(m.employee_id::VARCHAR, 6, '0'),
               -- The movement log lands with the next morning's file, and it
               -- draws from the default tail rather than the POS one: a WMS
               -- movement really can arrive a day or two after it happened.
               m.moved_at::DATE::TIMESTAMP + INTERVAL 1 DAY
                   + INTERVAL 1 DAY * {streams.lag_days(lag, ctx.cfg['late_tail']['default'])}
                   + INTERVAL 4 HOUR + INTERVAL 1 MINUTE * (({lag} >> 24) % 50)
        FROM sim_inventory.inventory_movements m
        JOIN sim_product.product_variants v ON v.variant_id = m.variant_id
        LEFT JOIN _util.order_keys ok
          ON m.reference_type = 'order_line'
         AND ok.order_id = 9000000 + (m.reference_id - 1000000000) // 16
        LEFT JOIN sim_inventory.warehouses wf ON wf.warehouse_id = m.from_warehouse_id
        LEFT JOIN sim_inventory.warehouses wt ON wt.warehouse_id = m.to_warehouse_id
        ORDER BY m.moved_at, m.movement_id
    """)


# --- the landing tree ------------------------------------------------------

def _files(ctx: Context) -> None:
    """`landing/wms/dt=<ds>/`: the A-class snapshot and the movement log every
    day, and the network snapshot on the day the weekly one ran."""
    root = ctx.landing_dir("wms")
    for day in _landing.window(ctx, LANDING_DAYS):
        directory = root / f"dt={day}"
        weekly = (day - ctx.start).days % 7 == 0
        _landing.copy_csv(ctx, directory / "movements.csv", f"""
            SELECT movement_id, occurred_at, sku, from_location, to_location, qty,
                   movement_type, reference_type, reference_id, operator_id
            FROM raw.wms_movements
            WHERE occurred_at::DATE = DATE '{day}'
            ORDER BY movement_id
        """)
        _landing.copy_csv(ctx, directory / "snapshot_aclass.csv", f"""
            SELECT snapshot_date, sku, location_id, dept_code, on_hand_units,
                   reserved_units, in_transit_units, unit_cost_cents,
                   retail_value_cents, cost_complement_bps, last_counted_at
            FROM raw.inventory_snapshots
            WHERE snapshot_date = DATE '{day}'
              AND location_id NOT LIKE 'S-%'
            ORDER BY location_id, sku
        """)
        if weekly:
            _landing.copy_csv(ctx, directory / "snapshot_network.csv", f"""
                SELECT snapshot_date, sku, location_id, dept_code, on_hand_units,
                       reserved_units, in_transit_units, unit_cost_cents,
                       retail_value_cents, cost_complement_bps, last_counted_at
                FROM raw.inventory_snapshots
                WHERE snapshot_date = DATE '{day}'
                ORDER BY location_id, sku
            """)
