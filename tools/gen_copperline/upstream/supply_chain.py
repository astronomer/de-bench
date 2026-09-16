"""`sim_supply_chain` — who Copperline buys from, at what cost, and what
actually turned up.

Two properties here are load-bearing and the rest is furniture:

* **Some variants carry two `is_primary_source` rows at once.** Nothing
  upstream forbids it, so it happens: about one sourced SKU in twenty-five has
  a second supplier flagged primary inside the same window. That is the
  duplicate a uniqueness test should catch and a landed-cost model must not
  silently fan out on.
* **Partial receipts are the norm.** A PO is received over several receipts,
  some lines short, some rejected, and there is no scorecard table anywhere —
  on-time-in-full has to be computed against the PO's expected date.

The sample thread is planted whole: PO 660214 buys variant 550231 from
supplier 118 (Lakeshore Outdoor Co) into warehouse 7 for 268,800.0000 across
ten lines of 26,880.0000, and receipt 880412 lands 1,200 of the 2,400 units
with 18 rejected for a seam defect.

Volumes stay deliberately modest — a few thousand rows a table. No extract
reads purchasing at package grain, so the purchasing history exists to make
the fiction whole and to give the finance side something real to match
against, not to carry a fact table's weight.
"""

from __future__ import annotations

import datetime as dt

from ..config import Context
from .. import streams
from . import _refs
from .product import SAMPLE_VARIANT
from .inventory import SAMPLE_WAREHOUSE

SUPPLIER_ID_BASE = 100        # supplier ids 101..360; the sample is 118
SUPPLIERS = 260
SAMPLE_SUPPLIER = 118

PURCHASE_ORDERS = 2_600
PO_LINES_MAX = 4
RECEIPTS = 2_200

# The planted sample documents. Their ids sit outside every drawn band, so a
# volume change can never collide with them.
SAMPLE_PO = 660_214
SAMPLE_PO_LINE = 3_301_127
SAMPLE_RECEIPT = 880_412
SAMPLE_RECEIPT_LINE = 1_102_284
SAMPLE_SHIPMENT_IN = 3_119_004      # the inbound leg, planted in logistics

SUPPLIER_TYPES = ["manufacturer", "distributor", "importer", "co_packer"]
INCOTERMS = ["FOB", "DDP", "EXW", "CIF"]
RISK = ["low", "low", "low", "medium", "high"]
REJECT_REASONS = ["seam_defect", "damaged_carton", "wrong_item",
                  "short_ship", "label_error"]


def build(ctx: Context) -> None:
    ctx.sql("CREATE SCHEMA IF NOT EXISTS sim_supply_chain")

    country_ids = _refs.country_ids(ctx)
    addresses = _refs.address_ids(ctx, "sim_supply_chain.suppliers", SUPPLIERS)
    ctx.sql("""
        CREATE OR REPLACE TABLE sim_supply_chain.suppliers (
            supplier_id INTEGER PRIMARY KEY,
            supplier_code VARCHAR NOT NULL UNIQUE,
            name VARCHAR NOT NULL,
            supplier_type VARCHAR NOT NULL,
            address_id BIGINT NOT NULL,
            country_id INTEGER NOT NULL,
            currency_code VARCHAR NOT NULL,
            net_payment_days INTEGER NOT NULL,
            incoterms VARCHAR NOT NULL,
            onboarded_at DATE NOT NULL,
            status VARCHAR NOT NULL,
            risk_rating VARCHAR NOT NULL
        )
    """)
    homes = [("US", "USD"), ("US", "USD"), ("US", "USD"),
             ("CA", "USD"), ("GB", "GBP"), ("DE", "EUR"), ("MX", "MXN")]
    rows = []
    for n in range(SUPPLIERS):
        supplier_id = SUPPLIER_ID_BASE + 1 + n
        draw = _refs.py_draw(ctx.seed, "sim_supply_chain.suppliers", supplier_id)
        iso2, currency = homes[draw % len(homes)]
        onboarded = dt.date(2016, 1, 4) + dt.timedelta(days=draw % 2_900)
        rows.append((
            supplier_id, f"SUP-{supplier_id}", f"Supplier {supplier_id}",
            SUPPLIER_TYPES[(draw >> 8) % len(SUPPLIER_TYPES)],
            addresses[n], country_ids[iso2], currency,
            [30, 45, 60, 90][(draw >> 16) % 4],
            INCOTERMS[(draw >> 20) % len(INCOTERMS)],
            onboarded,
            "active" if (draw >> 24) % 100 < 92 else "inactive",
            RISK[(draw >> 28) % len(RISK)],
        ))
    rows[SAMPLE_SUPPLIER - SUPPLIER_ID_BASE - 1] = (
        SAMPLE_SUPPLIER, f"SUP-{SAMPLE_SUPPLIER}", "Lakeshore Outdoor Co",
        "manufacturer", addresses[SAMPLE_SUPPLIER - SUPPLIER_ID_BASE - 1],
        country_ids["US"], "USD", 45, "FOB",
        dt.date(2021, 5, 19), "active", "low")
    ctx.con.executemany(
        "INSERT INTO sim_supply_chain.suppliers VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", rows)

    # What each supplier charges for a SKU. Sourced SKUs are the ones with a
    # reorder policy: a SKU nobody stocks is a SKU nobody buys.
    sp = streams.draw(ctx.seed, "'sim_supply_chain.supplier_products'",
                      "s.variant_id", "g.i")
    ctx.sql("""
        CREATE OR REPLACE TABLE sim_supply_chain.supplier_products (
            supplier_product_id BIGINT PRIMARY KEY,
            supplier_id INTEGER NOT NULL,
            variant_id BIGINT NOT NULL,
            supplier_sku VARCHAR NOT NULL,
            unit_cost DECIMAL(18,4) NOT NULL,
            currency_code VARCHAR NOT NULL,
            moq INTEGER NOT NULL,
            case_pack_qty INTEGER NOT NULL,
            lead_time_days INTEGER NOT NULL,
            is_primary_source BOOLEAN NOT NULL,
            valid_from DATE NOT NULL,
            valid_to DATE
        )
    """)
    # g.i = 1 is the primary source. g.i = 2 and 3 are alternates — except
    # for about one SKU in twenty-five, where the second row is flagged
    # primary as well and nothing upstream objects.
    dual = f"(({sp} >> 40) % 25 = 0)"
    ctx.sql(f"""
        INSERT INTO sim_supply_chain.supplier_products
        SELECT 440000 + row_number() OVER (ORDER BY s.variant_id, g.i),
               {SUPPLIER_ID_BASE} + 1 + {sp} % {SUPPLIERS},
               s.variant_id,
               'SP-' || lpad((({sp} >> 8) % 99999)::VARCHAR, 5, '0'),
               round(1.80 + ({sp} >> 12) % 18000 / 100.0, 4)::DECIMAL(18,4),
               'USD',
               [100, 250, 500, 1000][1 + ({sp} >> 28) % 4],
               [6, 12, 24, 48][1 + ({sp} >> 32) % 4],
               7 + ({sp} >> 36) % 42,
               g.i = 1 OR (g.i = 2 AND {dual}),
               DATE '2026-02-01',
               NULL
        FROM (SELECT DISTINCT variant_id FROM sim_inventory.reorder_policies
              WHERE warehouse_id IS NOT NULL) s
        CROSS JOIN generate_series(1, 3) AS g(i)
        WHERE g.i <= 1 + ({sp} >> 44) % 3
    """)
    ctx.sql(f"""
        UPDATE sim_supply_chain.supplier_products SET
            supplier_id = {SAMPLE_SUPPLIER}, supplier_sku = 'LS-CSH-SL-STD',
            unit_cost = 11.2000, currency_code = 'USD', moq = 500,
            case_pack_qty = 24, lead_time_days = 21, is_primary_source = true,
            valid_from = DATE '2026-02-01', valid_to = NULL
        WHERE variant_id = {SAMPLE_VARIANT}
          AND supplier_product_id = (SELECT min(supplier_product_id)
                                     FROM sim_supply_chain.supplier_products
                                     WHERE variant_id = {SAMPLE_VARIANT})
    """)

    # Purchase orders. The commitment; the receipts below are what landed.
    po = streams.draw(ctx.seed, "'sim_supply_chain.purchase_orders'", "d.ds", "g.i")
    po_out = streams.draw(ctx.seed, "'sim_supply_chain.purchase_orders'", "p.ds", "p.i")
    ctx.sql("""
        CREATE OR REPLACE TABLE sim_supply_chain.purchase_orders (
            po_id BIGINT PRIMARY KEY,
            po_number VARCHAR NOT NULL UNIQUE,
            supplier_id INTEGER NOT NULL,
            dest_warehouse_id INTEGER,
            dest_store_id INTEGER,
            buyer_employee_id INTEGER NOT NULL,
            order_date DATE NOT NULL,
            expected_date DATE NOT NULL,
            po_status VARCHAR NOT NULL,
            currency_code VARCHAR NOT NULL,
            total_amount DECIMAL(18,4) NOT NULL,
            cost_center_id INTEGER NOT NULL
        )
    """)
    ctx.sql(f"""
        INSERT INTO sim_supply_chain.purchase_orders
        SELECT p.po_id,
               'PO-' || year(p.order_date)::VARCHAR || '-' || p.po_id,
               p.supplier_id,
               p.dest_warehouse_id,
               NULL,
               2900 + ({po_out} >> 8) % 90,
               p.order_date,
               p.order_date + (14 + ({po_out} >> 16) % 35)::INTEGER,
               CASE WHEN p.order_date < DATE '{ctx.end}' - 60 THEN 'closed'
                    WHEN ({po_out} >> 20) % 100 < 76 THEN 'open'
                    WHEN ({po_out} >> 20) % 100 < 92 THEN 'approved'
                    ELSE 'draft' END,
               'USD',
               (26880.0 + ({po_out} >> 24) % 400000 / 100.0)::DECIMAL(18,4),
               5400 + p.dest_warehouse_id
        FROM (
            SELECT 600000 + row_number() OVER (ORDER BY d.ds, g.i) AS po_id,
                   {SUPPLIER_ID_BASE} + 1 + {po} % {SUPPLIERS} AS supplier_id,
                   1 + ({po} >> 4) % 12 AS dest_warehouse_id,
                   d.ds AS order_date, d.ds AS ds, g.i AS i
            FROM ({_refs.day_series(ctx)}) d
            CROSS JOIN generate_series(1, {_refs.per_day(ctx, PURCHASE_ORDERS)}) AS g(i)
        ) p
    """)

    pl = streams.draw(ctx.seed, "'sim_supply_chain.purchase_order_lines'",
                      "p.po_id", "g.i")
    ctx.sql("""
        CREATE OR REPLACE TABLE sim_supply_chain.purchase_order_lines (
            po_line_id BIGINT PRIMARY KEY,
            po_id BIGINT NOT NULL,
            line_no INTEGER NOT NULL,
            variant_id BIGINT NOT NULL,
            qty_ordered DECIMAL(14,3) NOT NULL,
            qty_received DECIMAL(14,3) NOT NULL,
            qty_cancelled DECIMAL(14,3) NOT NULL,
            unit_cost DECIMAL(18,4) NOT NULL,
            line_total DECIMAL(18,4) NOT NULL,
            expected_date DATE NOT NULL
        )
    """)
    ordered = f"(120 + {pl} % 2400)"
    short = f"(CASE WHEN ({pl} >> 12) % 100 < 34 THEN 1 + ({pl} >> 16) % 120 ELSE 0 END)"
    cost = f"round(1.80 + ({pl} >> 24) % 18000 / 100.0, 4)"
    ctx.sql(f"""
        INSERT INTO sim_supply_chain.purchase_order_lines
        SELECT 3310000 + row_number() OVER (ORDER BY p.po_id, g.i),
               p.po_id, g.i, s.variant_id,
               {ordered}::DECIMAL(14,3),
               CASE WHEN p.po_status IN ('draft', 'approved') THEN 0
                    ELSE {ordered} - {short} END::DECIMAL(14,3),
               CASE WHEN p.po_status = 'closed' THEN {short} ELSE 0 END::DECIMAL(14,3),
               {cost}::DECIMAL(18,4),
               ({ordered} * {cost})::DECIMAL(18,4),
               p.expected_date
        FROM sim_supply_chain.purchase_orders p
        CROSS JOIN generate_series(1, {PO_LINES_MAX}) AS g(i)
        JOIN (SELECT variant_id,
                     row_number() OVER (ORDER BY variant_id) - 1 AS n,
                     count(*) OVER () AS total
              FROM (SELECT DISTINCT variant_id FROM sim_inventory.reorder_policies
                    WHERE warehouse_id IS NOT NULL)) s
          ON s.n = ({pl} >> 36) % s.total
        WHERE g.i <= 1 + ({pl} >> 44) % {PO_LINES_MAX}
    """)

    # Receipts. Several per PO is normal, and a line can arrive short.
    gr = streams.draw(ctx.seed, "'sim_supply_chain.goods_receipts'", "p.po_id", "g.i")
    ctx.sql("""
        CREATE OR REPLACE TABLE sim_supply_chain.goods_receipts (
            receipt_id BIGINT PRIMARY KEY,
            receipt_number VARCHAR NOT NULL UNIQUE,
            po_id BIGINT NOT NULL,
            warehouse_id INTEGER NOT NULL,
            shipment_id BIGINT,
            received_at TIMESTAMP NOT NULL,
            received_by_employee_id INTEGER NOT NULL,
            receipt_status VARCHAR NOT NULL
        )
    """)
    ctx.sql(f"""
        INSERT INTO sim_supply_chain.goods_receipts
        SELECT r.receipt_id, 'GR-' || r.receipt_id, r.po_id, r.warehouse_id,
               r.shipment_id, r.received_at, r.employee_id, r.receipt_status
        FROM (
            SELECT 800000 + row_number() OVER (ORDER BY p.po_id, g.i) AS receipt_id,
                   p.po_id, p.dest_warehouse_id AS warehouse_id,
                   3100000 + ({gr} >> 8) % 90000 AS shipment_id,
                   (p.expected_date + (({gr} >> 16) % 9)::INTEGER)::TIMESTAMP
                       + INTERVAL 1 MINUTE * ((360 + ({gr} >> 24) % 540)::INTEGER)
                       AS received_at,
                   2800 + ({gr} >> 32) % 200 AS employee_id,
                   CASE WHEN ({gr} >> 40) % 100 < 94 THEN 'posted'
                        ELSE 'pending' END AS receipt_status
            FROM sim_supply_chain.purchase_orders p
            CROSS JOIN generate_series(1, 2) AS g(i)
            WHERE p.po_status IN ('closed', 'open')
              AND g.i <= 1 + ({gr} >> 48) % 2
        ) r
        WHERE r.received_at <= TIMESTAMP '{ctx.end} 23:59:00'
    """)

    rl = streams.draw(ctx.seed, "'sim_supply_chain.goods_receipt_lines'",
                      "r.receipt_id", "l.po_line_id")
    ctx.sql("""
        CREATE OR REPLACE TABLE sim_supply_chain.goods_receipt_lines (
            receipt_line_id BIGINT PRIMARY KEY,
            receipt_id BIGINT NOT NULL,
            po_line_id BIGINT NOT NULL,
            variant_id BIGINT NOT NULL,
            qty_received DECIMAL(14,3) NOT NULL,
            qty_rejected DECIMAL(14,3) NOT NULL,
            reject_reason VARCHAR,
            lot_number VARCHAR,
            expiry_date DATE,
            movement_id BIGINT
        )
    """)
    reasons = ", ".join(f"'{r}'" for r in REJECT_REASONS)
    rejected = f"(CASE WHEN ({rl} >> 8) % 100 < 12 THEN 1 + ({rl} >> 12) % 40 ELSE 0 END)"
    ctx.sql(f"""
        INSERT INTO sim_supply_chain.goods_receipt_lines
        SELECT 1110000 + row_number() OVER (ORDER BY r.receipt_id, l.po_line_id),
               r.receipt_id, l.po_line_id, l.variant_id,
               greatest(round(l.qty_received / 2, 0), 1)::DECIMAL(14,3),
               {rejected}::DECIMAL(14,3),
               CASE WHEN {rejected} > 0
                    THEN [{reasons}][1 + ({rl} >> 20) % {len(REJECT_REASONS)}] END,
               'LOT-' || strftime(r.received_at, '%y%m') || '-' ||
                   chr(65 + (({rl} >> 24) % 6)::INTEGER),
               NULL,
               88000000 + ({rl} >> 32) % 1000000
        FROM sim_supply_chain.goods_receipts r
        JOIN sim_supply_chain.purchase_order_lines l ON l.po_id = r.po_id
        WHERE l.qty_received > 0
    """)

    _plant_sample(ctx)


def _plant_sample(ctx: Context) -> None:
    """The sample storyline's purchasing leg, planted whole so a builder can
    read chapter 02's samples straight off the tables. Ten lines of 26,880
    make the header's 268,800 tie."""
    ctx.sql(f"""
        INSERT INTO sim_supply_chain.purchase_orders VALUES
        ({SAMPLE_PO}, 'PO-2026-{SAMPLE_PO}', {SAMPLE_SUPPLIER}, {SAMPLE_WAREHOUSE},
         NULL, 2904, DATE '2026-03-03', DATE '2026-03-24', 'closed', 'USD',
         268800.0000, 5401)
    """)
    ctx.sql(f"""
        INSERT INTO sim_supply_chain.purchase_order_lines
        SELECT {SAMPLE_PO_LINE} + g.i - 1, {SAMPLE_PO}, g.i,
               CASE WHEN g.i = 1 THEN {SAMPLE_VARIANT} ELSE s.variant_id END,
               2400.000, 2280.000, 120.000, 11.2000, 26880.0000, DATE '2026-03-24'
        FROM generate_series(1, 10) AS g(i)
        JOIN (SELECT variant_id,
                     row_number() OVER (ORDER BY variant_id) AS n
              FROM (SELECT DISTINCT variant_id FROM sim_inventory.reorder_policies
                    WHERE warehouse_id IS NOT NULL)) s
          ON s.n = g.i + 40
    """)
    ctx.sql(f"""
        INSERT INTO sim_supply_chain.goods_receipts VALUES
        ({SAMPLE_RECEIPT}, 'GR-{SAMPLE_RECEIPT}', {SAMPLE_PO}, {SAMPLE_WAREHOUSE},
         {SAMPLE_SHIPMENT_IN}, TIMESTAMP '2026-03-26 07:40:00', 2870, 'posted')
    """)
    ctx.sql(f"""
        INSERT INTO sim_supply_chain.goods_receipt_lines VALUES
        ({SAMPLE_RECEIPT_LINE}, {SAMPLE_RECEIPT}, {SAMPLE_PO_LINE}, {SAMPLE_VARIANT},
         1200.000, 18.000, 'seam_defect', 'LOT-2603-A', NULL, 88100412)
    """)
