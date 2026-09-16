"""`sim_inventory` — nodes, stock, and every reason stock moved.

The two tables that matter downstream are `inventory_balances` and
`inventory_movements`, and **they disagree on purpose**. Corvid writes the
snapshot from its own count; movements post asynchronously against orders,
receipts, transfers and counts. Neither is wrong and neither is derived from
the other here, which is the point: replaying the movements to a date will
not reproduce the snapshot, and only the inventory policy says which one the
warehouse should believe.

Volumes follow spec chapter 03 S6a: about 1,300 stocked SKUs across the
network snapshot weekly, the 240 A-class SKUs snapshot daily for the trailing
ninety days, and stores hold a short weekly history of what sits on their own
shelves — roughly 2.3M balance rows at the shipped profile, 1.2M movements.

`reorder_policies` doubles as the stocked-SKU registry: a SKU is stocked at a
site exactly when it has a policy there, so nothing else needs a list. The
valuation change of E8 does not appear here. The upstream holds one cost
method — a `unit_cost` on the movement — and the extract is where the retail
method and its department complement arrive.
"""

from __future__ import annotations

from ..config import Context
from .. import streams
from . import _refs
from .product import SAMPLE_VARIANT

# 12 nodes; the spec's sample DC is 7. Cost centers follow the finance
# formula for warehouses, 5400 + warehouse_id.
WAREHOUSES = [
    (1, "DC-POR", "Portland OR Distribution Center", "dc", 22000),
    (2, "DC-SAC", "Sacramento CA Distribution Center", "dc", 19500),
    (3, "DC-DAL", "Dallas TX Distribution Center", "dc", 24000),
    (4, "DC-ATL", "Atlanta GA Distribution Center", "dc", 21000),
    (5, "DC-CHI", "Joliet IL Distribution Center", "dc", 20500),
    (6, "HUB-DEN", "Denver CO Forward Hub", "forward_hub", 6400),
    (7, "DC-EDI", "Edison NJ Distribution Center", "dc", 18000),
    (8, "HUB-SEA", "Kent WA Forward Hub", "forward_hub", 5900),
    (9, "DC-LEE", "Leeds Distribution Center", "dc", 14000),
    (10, "HUB-DUB", "Dublin Forward Hub", "forward_hub", 4800),
    (11, "DC-HAM", "Hamburg Distribution Center", "dc", 15500),
    (12, "RET-COL", "Columbus OH Returns Center", "returns", 7200),
]
SAMPLE_WAREHOUSE = 7

STOCKED_SKUS = 1_300          # SKUs with a policy at a warehouse
A_CLASS_SKUS = 240            # of those, the ones that snapshot daily
STORE_SKUS = 40               # SKUs a store keeps its own history for
A_CLASS_DAYS = 90             # the trailing window the daily snapshot covers
STORE_WEEKS = 13              # how far back a store's weekly history runs

MOVEMENTS_TOTAL = 1_200_000
TRANSFERS_TOTAL = 6_000
COUNTS_TOTAL = 4_300

# The stocked registry, numbered so a draw can pick one in a hash join.
_STOCKED = """
    (SELECT variant_id,
            row_number() OVER (ORDER BY variant_id) - 1 AS n,
            count(*) OVER () AS total
     FROM (SELECT DISTINCT variant_id FROM sim_inventory.reorder_policies))
"""


def build(ctx: Context) -> None:
    ctx.sql("CREATE SCHEMA IF NOT EXISTS sim_inventory")

    districts = _refs.districts_by_country(ctx)
    country_ids = _refs.country_ids(ctx)
    homes = {9: "GB", 10: "IE", 11: "DE"}
    addresses = _refs.address_ids(ctx, "sim_inventory.warehouses", len(WAREHOUSES))
    ctx.sql("""
        CREATE OR REPLACE TABLE sim_inventory.warehouses (
            warehouse_id INTEGER PRIMARY KEY,
            warehouse_code VARCHAR NOT NULL,
            name VARCHAR NOT NULL,
            warehouse_type VARCHAR NOT NULL,
            address_id BIGINT NOT NULL,
            region_id INTEGER NOT NULL,
            manager_employee_id INTEGER NOT NULL,
            cost_center_id INTEGER NOT NULL,
            capacity_pallets INTEGER NOT NULL,
            is_active BOOLEAN NOT NULL
        )
    """)
    rows = []
    for n, (wid, code, name, wtype, capacity) in enumerate(WAREHOUSES):
        country_id = country_ids[homes.get(wid, "US")]
        pool = districts[country_id]
        draw = _refs.py_draw(ctx.seed, "sim_inventory.warehouses", wid)
        rows.append((wid, code, name, wtype, addresses[n], pool[draw % len(pool)],
                     2800 + draw % 200, 5400 + wid, capacity, True))
    ctx.con.executemany(
        "INSERT INTO sim_inventory.warehouses VALUES (?,?,?,?,?,?,?,?,?,?)", rows)

    # The stocked registry. The sample SKU ranks first so it is stocked
    # everywhere and A-class at every profile.
    rank = (f"CASE WHEN variant_id = {SAMPLE_VARIANT} THEN 0 ELSE 1 END, "
            f"hash({ctx.seed}, 'sim_inventory.stocked', variant_id)")
    # One stream per site kind: a policy at a warehouse and a policy at a
    # store are different rows about the same SKU, and neither may move the
    # other when the estate or the network changes.
    pol = streams.draw(ctx.seed, "'sim_inventory.reorder_policies.wh'",
                       "s.variant_id", "w.warehouse_id")
    pol_store = streams.draw(ctx.seed, "'sim_inventory.reorder_policies.store'",
                             "s.variant_id", "st.store_id")
    ctx.sql("""
        CREATE OR REPLACE TABLE sim_inventory.reorder_policies (
            policy_id BIGINT PRIMARY KEY,
            variant_id BIGINT NOT NULL,
            warehouse_id INTEGER,
            store_id INTEGER,
            min_qty DECIMAL(14,3) NOT NULL,
            max_qty DECIMAL(14,3) NOT NULL,
            reorder_point DECIMAL(14,3) NOT NULL,
            safety_stock DECIMAL(14,3) NOT NULL,
            lead_time_days INTEGER NOT NULL,
            preferred_supplier_id INTEGER NOT NULL
        )
    """)
    ctx.sql(f"""
        CREATE OR REPLACE TEMP VIEW _stocked_rank AS
        SELECT variant_id, row_number() OVER (ORDER BY {rank}) AS rk
        FROM sim_product.product_variants
    """)
    ctx.sql(f"""
        INSERT INTO sim_inventory.reorder_policies
        SELECT 770000 + row_number() OVER (ORDER BY s.variant_id, w.warehouse_id),
               s.variant_id, w.warehouse_id, NULL,
               (60 + {pol} % 180)::DECIMAL(14,3),
               (900 + ({pol} >> 12) % 1400)::DECIMAL(14,3),
               (200 + ({pol} >> 24) % 400)::DECIMAL(14,3),
               (40 + ({pol} >> 36) % 120)::DECIMAL(14,3),
               7 + ({pol} >> 48) % 35,
               101 + ({pol} >> 52) % 260
        FROM (SELECT variant_id FROM _stocked_rank WHERE rk <= {STOCKED_SKUS}) s
        CROSS JOIN sim_inventory.warehouses w
    """)
    ctx.sql(f"""
        INSERT INTO sim_inventory.reorder_policies
        SELECT 2770000 + row_number() OVER (ORDER BY st.store_id, s.variant_id),
               s.variant_id, NULL, st.store_id,
               (6 + {pol_store} % 24)::DECIMAL(14,3),
               (90 + ({pol_store} >> 12) % 240)::DECIMAL(14,3),
               (20 + ({pol_store} >> 24) % 60)::DECIMAL(14,3),
               (4 + ({pol_store} >> 36) % 20)::DECIMAL(14,3),
               3 + ({pol_store} >> 48) % 14,
               101 + ({pol_store} >> 52) % 260
        FROM sim_store.stores st
        CROSS JOIN generate_series(1, {STORE_SKUS}) AS g(i)
        JOIN _stocked_rank s
          ON s.rk = 1 + (hash({ctx.seed}, 'store_assortment', st.store_id, g.i)
                         >> 1) % {STOCKED_SKUS}
    """)

    # Balances. Three populations, one table: the weekly network snapshot,
    # the daily A-class snapshot over the trailing ninety days, and the
    # store's own short weekly history. Ids sit in disjoint bands so the
    # three inserts can never collide.
    bal = streams.draw(ctx.seed, "'sim_inventory.inventory_balances'",
                       "d.ds", "s.variant_id", "site.site_id")
    ctx.sql("""
        CREATE OR REPLACE TABLE sim_inventory.inventory_balances (
            balance_id BIGINT PRIMARY KEY,
            variant_id BIGINT NOT NULL,
            warehouse_id INTEGER,
            store_id INTEGER,
            zone VARCHAR,
            bin VARCHAR,
            qty_on_hand DECIMAL(14,3) NOT NULL,
            qty_reserved DECIMAL(14,3) NOT NULL,
            qty_available DECIMAL(14,3) NOT NULL,
            qty_in_transit DECIMAL(14,3) NOT NULL,
            as_of_datetime TIMESTAMP NOT NULL
        )
    """)
    # The columns after the site are the same for all three populations, so
    # the quantity draw is written once. Reserved never exceeds on hand, and
    # available is the difference, because a WMS would not publish otherwise.
    on_hand = f"(({bal} >> 4) % 1500)"
    reserved = f"(({bal} >> 20) % 60)"
    zone = f"['A', 'B', 'C', 'D'][1 + ({bal} >> 32) % 4]"
    body = f"""
               {zone},
               {zone} || '-' ||
                   lpad((1 + ({bal} >> 36) % 40)::VARCHAR, 2, '0') || '-' ||
                   lpad((1 + ({bal} >> 44) % 12)::VARCHAR, 2, '0'),
               {on_hand}::DECIMAL(14,3),
               least({reserved}, {on_hand})::DECIMAL(14,3),
               ({on_hand} - least({reserved}, {on_hand}))::DECIMAL(14,3),
               (({bal} >> 48) % 400)::DECIMAL(14,3),
               d.ds::TIMESTAMP + INTERVAL 23 HOUR + INTERVAL 59 MINUTE
    """

    weekly_skus = max(1, ctx.scale(STOCKED_SKUS))
    ctx.sql(f"""
        INSERT INTO sim_inventory.inventory_balances
        SELECT 10000000 + row_number() OVER (ORDER BY d.ds, site.site_id, s.variant_id),
               s.variant_id, site.site_id, NULL,
               {body}
        FROM ({_refs.week_series(ctx)}) d
        CROSS JOIN (SELECT warehouse_id AS site_id FROM sim_inventory.warehouses) site
        CROSS JOIN (SELECT variant_id FROM _stocked_rank WHERE rk <= {weekly_skus}) s
    """)

    a_class = max(1, ctx.scale(A_CLASS_SKUS))
    ctx.sql(f"""
        INSERT INTO sim_inventory.inventory_balances
        SELECT 40000000 + row_number() OVER (ORDER BY d.ds, site.site_id, s.variant_id),
               s.variant_id, site.site_id, NULL,
               {body}
        FROM (SELECT unnest(generate_series(
                  DATE '{ctx.end}' - {A_CLASS_DAYS - 1}, DATE '{ctx.end}',
                  INTERVAL 1 DAY))::DATE AS ds) d
        CROSS JOIN (SELECT warehouse_id AS site_id FROM sim_inventory.warehouses) site
        CROSS JOIN (SELECT variant_id FROM _stocked_rank WHERE rk <= {a_class}) s
        WHERE d.ds <> date_trunc('week', d.ds)::DATE - 1
    """)

    store_skus = max(1, ctx.scale(STORE_SKUS))
    ctx.sql(f"""
        INSERT INTO sim_inventory.inventory_balances
        SELECT 70000000 + row_number() OVER (ORDER BY d.ds, site.site_id, s.variant_id),
               s.variant_id, NULL, site.site_id,
               {body}
        FROM (SELECT unnest(generate_series(
                  DATE '{ctx.end}' - {7 * STORE_WEEKS}, DATE '{ctx.end}',
                  INTERVAL 7 DAY))::DATE AS ds) d
        CROSS JOIN (SELECT store_id AS site_id, open_date, close_date
                    FROM sim_store.stores) site
        CROSS JOIN (SELECT variant_id FROM _stocked_rank WHERE rk <= {store_skus}) s
        WHERE d.ds >= site.open_date
          AND (site.close_date IS NULL OR d.ds <= site.close_date)
    """)

    # Movements. The ledger behind the balance, posted per day, with a
    # polymorphic reference to whichever document caused the move.
    mv = streams.draw(ctx.seed, "'sim_inventory.inventory_movements'", "d.ds", "g.i")
    kind = streams.pick(mv, [("'sale'", 54), ("'receipt'", 12), ("'return'", 6),
                             ("'transfer_out'", 8), ("'transfer_in'", 8),
                             ("'adjustment'", 7), ("'shrink'", 5)])
    ctx.sql("""
        CREATE OR REPLACE TABLE sim_inventory.inventory_movements (
            movement_id BIGINT PRIMARY KEY,
            variant_id BIGINT NOT NULL,
            movement_type VARCHAR NOT NULL,
            qty DECIMAL(14,3) NOT NULL,
            from_warehouse_id INTEGER,
            to_warehouse_id INTEGER,
            from_store_id INTEGER,
            to_store_id INTEGER,
            reference_type VARCHAR NOT NULL,
            reference_id BIGINT NOT NULL,
            unit_cost DECIMAL(18,4) NOT NULL,
            moved_at TIMESTAMP NOT NULL,
            employee_id INTEGER NOT NULL
        )
    """)
    ctx.sql(f"""
        INSERT INTO sim_inventory.inventory_movements
        SELECT 88000000 + row_number() OVER (ORDER BY d.ds, g.i),
               s.variant_id,
               k.movement_type,
               (CASE WHEN k.movement_type IN ('sale', 'shrink', 'transfer_out')
                     THEN -1 ELSE 1 END * (1 + ({mv} >> 8) % 60))::DECIMAL(14,3),
               CASE WHEN k.movement_type IN ('sale', 'transfer_out', 'shrink')
                    THEN 1 + ({mv} >> 16) % 12 END,
               CASE WHEN k.movement_type IN ('receipt', 'transfer_in', 'return')
                    THEN 1 + ({mv} >> 16) % 12 END,
               CASE WHEN k.movement_type = 'transfer_in' THEN 101 + ({mv} >> 24) % 268 END,
               CASE WHEN k.movement_type = 'transfer_out' THEN 101 + ({mv} >> 24) % 268 END,
               CASE k.movement_type WHEN 'receipt' THEN 'receipt_line'
                    WHEN 'transfer_in' THEN 'transfer_line'
                    WHEN 'transfer_out' THEN 'transfer_line'
                    WHEN 'adjustment' THEN 'count_line'
                    WHEN 'shrink' THEN 'count_line' ELSE 'order_line' END,
               -- An order_line movement cites a real line: the shared
               -- day-block identity, drawn as (day, slot, line), never a
               -- band of ids no order ever minted.
               CASE WHEN k.movement_type IN ('transfer_in', 'transfer_out',
                                             'adjustment', 'shrink')
                    THEN 19000000 + ({mv} >> 32) % 8000000
                    ELSE 1000000000
                         + ((d.ds - DATE '{ctx.start}')::BIGINT * 200000
                            + 1 + ({mv} >> 32) % u.orders) * 16
                         + 1 + ({mv} >> 58) % 4 END,
               round(2.10 + ({mv} >> 40) % 22000 / 100.0, 4)::DECIMAL(18,4),
               {streams.minute_time(f"({mv} >> 44)", "d.ds")},
               2000 + ({mv} >> 52) % 1100
        FROM ({_refs.day_series(ctx)}) d
        JOIN _util.days u USING (ds)
        CROSS JOIN generate_series(1, {_refs.per_day(ctx, MOVEMENTS_TOTAL)}) AS g(i)
        JOIN {_STOCKED} s ON s.n = ({mv} >> 56) % s.total
        CROSS JOIN (SELECT {kind} AS movement_type) k
    """)

    # Transfers: stock repositioned across the network, and the units that go
    # missing between shipped and received.
    tr = streams.draw(ctx.seed, "'sim_inventory.stock_transfers'", "d.ds", "g.i")
    tr_out = streams.draw(ctx.seed, "'sim_inventory.stock_transfers'", "t.ds", "t.i")
    ctx.sql("""
        CREATE OR REPLACE TABLE sim_inventory.stock_transfers (
            transfer_id BIGINT PRIMARY KEY,
            transfer_number VARCHAR NOT NULL UNIQUE,
            source_warehouse_id INTEGER NOT NULL,
            dest_warehouse_id INTEGER,
            dest_store_id INTEGER,
            transfer_status VARCHAR NOT NULL,
            requested_at TIMESTAMP NOT NULL,
            shipped_at TIMESTAMP,
            received_at TIMESTAMP,
            shipment_id BIGINT,
            requested_by_employee_id INTEGER NOT NULL
        )
    """)
    ctx.sql(f"""
        INSERT INTO sim_inventory.stock_transfers
        SELECT t.transfer_id,
               'TR-' || t.transfer_id,
               t.source_warehouse_id,
               CASE WHEN t.to_store IS NULL THEN 1 + ({tr_out} >> 20) % 12 END,
               t.to_store,
               t.transfer_status,
               t.requested_at,
               CASE WHEN t.transfer_status <> 'requested'
                    THEN t.requested_at + INTERVAL 1 DAY
                         + INTERVAL 1 HOUR * ((({tr_out} >> 24) % 20)::INTEGER) END,
               CASE WHEN t.transfer_status = 'received'
                    THEN t.requested_at + INTERVAL 2 DAY
                         + INTERVAL 1 HOUR * ((({tr_out} >> 28) % 30)::INTEGER) END,
               CASE WHEN t.transfer_status <> 'requested'
                    THEN 3100000 + ({tr_out} >> 32) % 90000 END,
               3000 + ({tr_out} >> 40) % 900
        FROM (
            SELECT 220000 + row_number() OVER (ORDER BY d.ds, g.i) AS transfer_id,
                   1 + {tr} % 12 AS source_warehouse_id,
                   CASE WHEN ({tr} >> 12) % 100 < 74 THEN 101 + ({tr} >> 16) % 268 END AS to_store,
                   CASE WHEN ({tr} >> 48) % 100 < 82 THEN 'received'
                        WHEN ({tr} >> 48) % 100 < 93 THEN 'in_transit'
                        ELSE 'requested' END AS transfer_status,
                   {streams.minute_time(f"({tr} >> 52)", "d.ds")} AS requested_at,
                   d.ds AS ds, g.i AS i
            FROM ({_refs.day_series(ctx)}) d
            CROSS JOIN generate_series(1, {_refs.per_day(ctx, TRANSFERS_TOTAL)}) AS g(i)
        ) t
    """)

    tl = streams.draw(ctx.seed, "'sim_inventory.stock_transfer_lines'",
                      "t.transfer_id", "g.i")
    ctx.sql("""
        CREATE OR REPLACE TABLE sim_inventory.stock_transfer_lines (
            transfer_line_id BIGINT PRIMARY KEY,
            transfer_id BIGINT NOT NULL,
            variant_id BIGINT NOT NULL,
            qty_requested DECIMAL(14,3) NOT NULL,
            qty_shipped DECIMAL(14,3) NOT NULL,
            qty_received DECIMAL(14,3) NOT NULL,
            qty_damaged DECIMAL(14,3) NOT NULL,
            unit_cost DECIMAL(18,4) NOT NULL
        )
    """)
    ctx.sql(f"""
        INSERT INTO sim_inventory.stock_transfer_lines
        SELECT 660000 + row_number() OVER (ORDER BY t.transfer_id, g.i),
               t.transfer_id,
               s.variant_id,
               (12 + {tl} % 90)::DECIMAL(14,3),
               (12 + {tl} % 90)::DECIMAL(14,3),
               CASE WHEN t.received_at IS NULL THEN 0
                    ELSE 12 + {tl} % 90 - ({tl} >> 12) % 4 END::DECIMAL(14,3),
               CASE WHEN t.received_at IS NULL THEN 0
                    ELSE ({tl} >> 12) % 4 END::DECIMAL(14,3),
               round(2.10 + ({tl} >> 20) % 22000 / 100.0, 4)::DECIMAL(18,4)
        FROM sim_inventory.stock_transfers t
        CROSS JOIN generate_series(1, 3) AS g(i)
        JOIN {_STOCKED} s ON s.n = ({tl} >> 32) % s.total
        WHERE g.i <= 1 + ({tl} >> 44) % 3
    """)

    # Counts and their lines: the gap between what the system believed and
    # what was physically there.
    ct = streams.draw(ctx.seed, "'sim_inventory.stock_counts'", "d.ds", "g.i")
    ctx.sql("""
        CREATE OR REPLACE TABLE sim_inventory.stock_counts (
            count_id BIGINT PRIMARY KEY,
            store_id INTEGER,
            warehouse_id INTEGER,
            count_type VARCHAR NOT NULL,
            started_at TIMESTAMP NOT NULL,
            completed_at TIMESTAMP,
            counted_by_employee_id INTEGER NOT NULL,
            count_status VARCHAR NOT NULL
        )
    """)
    ctx.sql(f"""
        INSERT INTO sim_inventory.stock_counts
        SELECT 14000 + row_number() OVER (ORDER BY d.ds, g.i),
               CASE WHEN {ct} % 100 < 68 THEN 101 + ({ct} >> 8) % 268 END,
               CASE WHEN {ct} % 100 >= 68 THEN 1 + ({ct} >> 8) % 12 END,
               CASE WHEN ({ct} >> 20) % 100 < 88 THEN 'cycle' ELSE 'full' END,
               d.ds::TIMESTAMP + INTERVAL 6 HOUR,
               CASE WHEN ({ct} >> 24) % 100 < 94
                    THEN d.ds::TIMESTAMP + INTERVAL 6 HOUR
                         + INTERVAL 15 MINUTE * (4 + ({ct} >> 28) % 20) END,
               4000 + ({ct} >> 36) % 400,
               CASE WHEN ({ct} >> 24) % 100 < 94 THEN 'completed' ELSE 'in_progress' END
        FROM ({_refs.day_series(ctx)}) d
        CROSS JOIN generate_series(1, {_refs.per_day(ctx, COUNTS_TOTAL)}) AS g(i)
    """)

    cl = streams.draw(ctx.seed, "'sim_inventory.stock_count_lines'", "c.count_id", "g.i")
    ctx.sql("""
        CREATE OR REPLACE TABLE sim_inventory.stock_count_lines (
            count_line_id BIGINT PRIMARY KEY,
            count_id BIGINT NOT NULL,
            variant_id BIGINT NOT NULL,
            system_qty DECIMAL(14,3) NOT NULL,
            counted_qty DECIMAL(14,3) NOT NULL,
            variance_qty DECIMAL(14,3) NOT NULL,
            variance_value DECIMAL(18,4) NOT NULL,
            adjustment_movement_id BIGINT
        )
    """)
    system_qty = f"(4 + {cl} % 120)"
    variance = f"(CASE WHEN ({cl} >> 12) % 100 < 76 THEN 0 ELSE -1 - ({cl} >> 16) % 5 END)"
    unit_cost = f"round(2.10 + ({cl} >> 24) % 22000 / 100.0, 4)"
    ctx.sql(f"""
        INSERT INTO sim_inventory.stock_count_lines
        SELECT 330000 + row_number() OVER (ORDER BY c.count_id, g.i),
               c.count_id,
               s.variant_id,
               {system_qty}::DECIMAL(14,3),
               ({system_qty} + {variance})::DECIMAL(14,3),
               {variance}::DECIMAL(14,3),
               ({variance} * {unit_cost})::DECIMAL(18,4),
               CASE WHEN {variance} <> 0 THEN 88000000 + ({cl} >> 36) % 1000000 END
        FROM sim_inventory.stock_counts c
        CROSS JOIN generate_series(1, 20) AS g(i)
        JOIN {_STOCKED} s ON s.n = ({cl} >> 44) % s.total
        WHERE c.count_status = 'completed'
    """)
    ctx.sql("DROP VIEW IF EXISTS _stocked_rank")
