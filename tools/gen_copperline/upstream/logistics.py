"""`sim_logistics` — how stock physically moved: carriers, fulfilments,
shipments, scans, and the small own-fleet routing nobody reads.

What earns its place here:

* **Split shipments.** One order can have several fulfilments, which is why
  "was this order on time" has no answer until a document defines it. The
  ambiguity is the fixture; do not resolve it in the data.
* **Four mutually exclusive foreign keys on `shipment_lines`.** Which one is
  populated follows the shipment's direction — outbound carries an order
  line, inbound a PO line, a transfer carries a transfer line, a return a
  return line.
* **A carrier scan vocabulary per carrier.** The mapping to a common
  milestone set is authored at the extract, not here.

`delivery_routes` and `route_stops` stay small on purpose: Copperline's own
last-mile fleet is small, no extract reads either table, and they exist so
the fiction is whole.

**How this module reaches the order spine.** `sim_sales` builds after this
one, so nothing here can read an order. It does not need to: an order's
identity is a formula over the day and the order's place in that day, and
both sides compute it the same way. For day `dn` — days counted from
`range.start` — and a slot in `1..orders` for that day, which `_util.days`
carries:

    order_seq      = dn * 200,000 + slot
    order_id       = 9,000,000     + order_seq
    order_line_id  = 1,000,000,000 + order_seq * 16 + line_no
    fulfillment_id = 200,000,000   + order_seq

The last one is the join that matters: sales stamps that value on shipped
non-store order lines, so a fulfilment drawn here and a line written there
meet without either module reading the other. The `8,000,000..8,999,999`
band is reserved for planted and sample rows, which is where the storyline's
order 8840127 sits.
"""

from __future__ import annotations

from ..config import Context
from .. import streams
from . import _refs
from .product import SAMPLE_VARIANT
from .inventory import SAMPLE_WAREHOUSE

# Carrier ids 10..17; the spec's sample parcel carrier is 12.
CARRIERS = [
    (10, "Merriweather Logistics", "MWLG", "ltl", "dispatch@merriweather.example"),
    (11, "Harbor Line Logistics", "HRBL", "ftl", "ops@harborline.example"),
    (12, "Brightline Freight", "BRFR", "parcel", "ops@brightlinefreight.example"),
    (13, "Pallas Parcel", "PLPC", "parcel", "support@pallasparcel.example"),
    (14, "Meridian Post", "MRDP", "postal", "service@meridianpost.example"),
    (15, "Copperline Fleet", "CPLF", "own_fleet", "fleet@copperline.example"),
    (16, "Albion Carriers", "ALBC", "parcel", "uk@albioncarriers.example"),
    (17, "Rheinweg Logistik", "RHWG", "ltl", "de@rheinweg.example"),
]

FULFILLMENTS = 9_000
SHIPMENTS = 9_000
ROUTES = 1_200
TRACKING_PER_SHIPMENT = 4

# The order-identity formula, shared with sales. See the module note.
SLOTS_PER_DAY = 200_000
ORDER_ID_BASE = 9_000_000
ORDER_LINE_ID_BASE = 1_000_000_000
LINES_PER_ORDER = 16
FULFILLMENT_ID_BASE = 200_000_000

SAMPLE_ORDER = 8_840_127
SAMPLE_ORDER_LINE = 19_338_451
SAMPLE_FULFILLMENT = 5_520_118
SAMPLE_SHIPMENT = 3_120_884
SAMPLE_SHIPMENT_IN = 3_119_004      # the inbound leg the sample receipt names
SAMPLE_SHIPMENT_TRANSFER = 3_120_770
SAMPLE_SHIPMENT_LINE = 9_910_228
SAMPLE_TRACKING_EVENT = 60_114_228
SAMPLE_DEST_ADDRESS = 7_712_045

EVENT_CODES = ["PICKED_UP", "IN_TRANSIT", "ARRIVED_FACILITY",
               "OUT_FOR_DELIVERY", "DELIVERED", "EXCEPTION"]
SERVICE_LEVELS = ["ground_2day", "ground_5day", "express_next", "freight_std"]


def build(ctx: Context) -> None:
    ctx.sql("CREATE SCHEMA IF NOT EXISTS sim_logistics")

    ctx.sql("""
        CREATE OR REPLACE TABLE sim_logistics.carriers (
            carrier_id INTEGER PRIMARY KEY,
            name VARCHAR NOT NULL,
            scac_code VARCHAR NOT NULL,
            carrier_type VARCHAR NOT NULL,
            contact_email VARCHAR NOT NULL,
            is_active BOOLEAN NOT NULL
        )
    """)
    ctx.con.executemany(
        "INSERT INTO sim_logistics.carriers VALUES (?,?,?,?,?,?)",
        [(cid, name, scac, ctype, email, True)
         for cid, name, scac, ctype, email in CARRIERS])

    # Fulfilments. An order can carry several, which is the whole of the
    # split-shipment ambiguity; the internal steps run promised, picked,
    # packed, shipped, completed.
    f = streams.draw(ctx.seed, "'sim_logistics.fulfillments'", "d.ds", "g.i")
    ctx.sql("""
        CREATE OR REPLACE TABLE sim_logistics.fulfillments (
            fulfillment_id BIGINT PRIMARY KEY,
            order_id BIGINT NOT NULL,
            fulfillment_type VARCHAR NOT NULL,
            warehouse_id INTEGER,
            store_id INTEGER,
            supplier_id INTEGER,
            assigned_employee_id INTEGER NOT NULL,
            promised_at TIMESTAMP NOT NULL,
            picked_at TIMESTAMP,
            packed_at TIMESTAMP,
            shipped_at TIMESTAMP,
            completed_at TIMESTAMP,
            fulfillment_status VARCHAR NOT NULL
        )
    """)
    f_out = streams.draw(ctx.seed, "'sim_logistics.fulfillments'", "o.ds", "o.i")
    kind = streams.pick(f"({f_out} >> 4)",
                        [("'ship_from_dc'", 62), ("'ship_from_store'", 18),
                         ("'pickup_in_store'", 14), ("'dropship'", 6)])
    picked = f"({streams.minute_time(f'({f_out} >> 12)', 'o.ds')})"
    ctx.sql(f"""
        INSERT INTO sim_logistics.fulfillments
        SELECT {FULFILLMENT_ID_BASE} + o.order_seq,
               {ORDER_ID_BASE} + o.order_seq,
               k.fulfillment_type,
               CASE WHEN k.fulfillment_type = 'ship_from_dc'
                    THEN 1 + ({f_out} >> 20) % 12 END,
               CASE WHEN k.fulfillment_type IN ('ship_from_store', 'pickup_in_store')
                    THEN 101 + ({f_out} >> 24) % 268 END,
               CASE WHEN k.fulfillment_type = 'dropship'
                    THEN 101 + ({f_out} >> 28) % 260 END,
               2000 + ({f_out} >> 32) % 1100,
               o.ds::TIMESTAMP + INTERVAL 1 DAY * (2 + ({f_out} >> 36) % 4)
                   + INTERVAL 23 HOUR + INTERVAL 59 MINUTE,
               {picked},
               {picked} + INTERVAL 1 MINUTE * (20 + ({f_out} >> 40) % 300),
               {picked} + INTERVAL 1 MINUTE * (200 + ({f_out} >> 44) % 700),
               CASE WHEN ({f_out} >> 48) % 100 < 88
                    THEN {picked} + INTERVAL 1 DAY * (1 + ({f_out} >> 52) % 4) END,
               CASE WHEN ({f_out} >> 48) % 100 < 88 THEN 'delivered'
                    WHEN ({f_out} >> 48) % 100 < 96 THEN 'in_transit' ELSE 'picking' END
        FROM (
            -- One order a fulfilment, picked by slot inside its own day. Two
            -- draws landing on the same order keep one row: a fulfilment id
            -- is the order's, so a duplicate would be the same fulfilment.
            SELECT d.ds AS ds, g.i AS i,
                   d.dn * {SLOTS_PER_DAY} + 1 + {f} % d.orders AS order_seq
            FROM _util.days d
            CROSS JOIN generate_series(1, {_refs.per_day(ctx, FULFILLMENTS)}) AS g(i)
            QUALIFY row_number() OVER (PARTITION BY order_seq ORDER BY g.i) = 1
        ) o
        CROSS JOIN (SELECT {kind} AS fulfillment_type) k
    """)

    # Shipments. `freight_cost` is what the WMS recorded at the time; what the
    # carrier later invoices is a different number on a different feed.
    sh = streams.draw(ctx.seed, "'sim_logistics.shipments'", "d.ds", "g.i")
    sh_out = streams.draw(ctx.seed, "'sim_logistics.shipments'", "s.ds", "s.i")
    ctx.sql("""
        CREATE OR REPLACE TABLE sim_logistics.shipments (
            shipment_id BIGINT PRIMARY KEY,
            shipment_number VARCHAR NOT NULL UNIQUE,
            carrier_id INTEGER NOT NULL,
            shipment_direction VARCHAR NOT NULL,
            origin_warehouse_id INTEGER,
            origin_store_id INTEGER,
            origin_supplier_id INTEGER,
            dest_address_id BIGINT,
            dest_warehouse_id INTEGER,
            tracking_number VARCHAR NOT NULL,
            service_level VARCHAR NOT NULL,
            weight_kg DECIMAL(12,3) NOT NULL,
            freight_cost DECIMAL(18,4) NOT NULL,
            currency_code VARCHAR NOT NULL,
            shipped_at TIMESTAMP NOT NULL,
            promised_delivery_at TIMESTAMP NOT NULL,
            delivered_at TIMESTAMP,
            shipment_status VARCHAR NOT NULL
        )
    """)
    direction = streams.pick(f"({sh} >> 4)",
                             [("'outbound'", 74), ("'inbound'", 18), ("'transfer'", 8)])
    services = ", ".join(f"'{s}'" for s in SERVICE_LEVELS)
    shipped = f"({streams.minute_time(f'({sh} >> 12)', 'd.ds')})"
    ctx.sql(f"""
        INSERT INTO sim_logistics.shipments
        SELECT s.shipment_id,
               'SH-' || s.shipment_id,
               [10, 11, 12, 12, 13, 14, 15, 16, 17][1 + ({sh_out} >> 16) % 9],
               s.shipment_direction,
               CASE WHEN s.shipment_direction <> 'inbound'
                    THEN 1 + ({sh_out} >> 20) % 12 END,
               NULL,
               CASE WHEN s.shipment_direction = 'inbound'
                    THEN 101 + ({sh_out} >> 24) % 260 END,
               CASE WHEN s.shipment_direction = 'outbound'
                    THEN {_refs.SYNTHETIC_ADDRESS_BASE} + ({sh_out} >> 28) % 90000 END,
               CASE WHEN s.shipment_direction <> 'outbound'
                    THEN 1 + ({sh_out} >> 32) % 12 END,
               'NB' || lpad((({sh_out} >> 36) % 9999999999)::VARCHAR, 10, '0'),
               [{services}][1 + ({sh_out} >> 40) % {len(SERVICE_LEVELS)}],
               round(0.400 + ({sh_out} >> 44) % 42000 / 100.0, 3)::DECIMAL(12,3),
               round(4.20 + ({sh_out} >> 48) % 24000 / 100.0, 4)::DECIMAL(18,4),
               'USD',
               s.shipped_at,
               s.shipped_at + INTERVAL 1 DAY * ((2 + ({sh_out} >> 52) % 4)::INTEGER),
               CASE WHEN s.delivered THEN s.shipped_at
                        + INTERVAL 1 DAY * ((1 + ({sh_out} >> 56) % 4)::INTEGER)
                        + INTERVAL 1 HOUR * ((({sh_out} >> 58) % 11)::INTEGER) END,
               CASE WHEN s.delivered THEN 'delivered'
                    WHEN ({sh_out} >> 60) % 3 = 0 THEN 'in_transit'
                    ELSE 'label_created' END
        FROM (
            SELECT 3100000 + row_number() OVER (ORDER BY d.ds, g.i) AS shipment_id,
                   {direction} AS shipment_direction,
                   {shipped} AS shipped_at,
                   ({sh} >> 8) % 100 < 91 AS delivered,
                   d.ds AS ds, g.i AS i
            FROM ({_refs.day_series(ctx)}) d
            CROSS JOIN generate_series(1, {_refs.per_day(ctx, SHIPMENTS)}) AS g(i)
        ) s
    """)

    # Shipment lines. Exactly one of the four document keys is populated, and
    # which one follows the direction. An outbound line names an order line
    # through the shared formula, off the day the shipment left.
    sl = streams.draw(ctx.seed, "'sim_logistics.shipment_lines'", "s.shipment_id", "g.i")
    order_line = (f"({ORDER_LINE_ID_BASE} + (d.dn * {SLOTS_PER_DAY}"
                  f" + 1 + ({sl} >> 8) % d.orders) * {LINES_PER_ORDER}"
                  f" + 1 + ({sl} >> 32) % 4)")
    ctx.sql("""
        CREATE OR REPLACE TABLE sim_logistics.shipment_lines (
            shipment_line_id BIGINT PRIMARY KEY,
            shipment_id BIGINT NOT NULL,
            variant_id BIGINT NOT NULL,
            qty DECIMAL(14,3) NOT NULL,
            order_line_id BIGINT,
            transfer_line_id BIGINT,
            po_line_id BIGINT,
            return_line_id BIGINT
        )
    """)
    ctx.sql(f"""
        INSERT INTO sim_logistics.shipment_lines
        SELECT 9950000 + row_number() OVER (ORDER BY s.shipment_id, g.i),
               s.shipment_id,
               v.variant_id,
               (1 + {sl} % 24)::DECIMAL(14,3),
               CASE WHEN s.shipment_direction = 'outbound' THEN {order_line} END,
               CASE WHEN s.shipment_direction = 'transfer'
                    THEN 660000 + ({sl} >> 20) % 20000 END,
               CASE WHEN s.shipment_direction = 'inbound'
                    THEN 3310000 + ({sl} >> 28) % 8000 END,
               NULL
        FROM sim_logistics.shipments s
        JOIN _util.days d ON d.ds = s.shipped_at::DATE
        CROSS JOIN generate_series(1, 3) AS g(i)
        JOIN (SELECT variant_id,
                     row_number() OVER (ORDER BY variant_id) - 1 AS n,
                     count(*) OVER () AS total
              FROM (SELECT DISTINCT variant_id FROM sim_inventory.reorder_policies)) v
          ON v.n = ({sl} >> 36) % v.total
        WHERE g.i <= 1 + ({sl} >> 44) % 3
    """)

    # Carrier scans. Each carrier keeps its own vocabulary; the common
    # milestone mapping is the extract's job, not this table's.
    te = streams.draw(ctx.seed, "'sim_logistics.tracking_events'", "s.shipment_id", "g.i")
    ctx.sql("""
        CREATE OR REPLACE TABLE sim_logistics.tracking_events (
            event_id BIGINT PRIMARY KEY,
            shipment_id BIGINT NOT NULL,
            event_code VARCHAR NOT NULL,
            description VARCHAR,
            city VARCHAR,
            country_id INTEGER,
            event_at TIMESTAMP NOT NULL,
            is_exception BOOLEAN NOT NULL
        )
    """)
    cities = ["Brooklyn", "Newark", "Columbus", "Memphis", "Denver",
              "Portland", "Leeds", "Dublin", "Hamburg"]
    city_list = ", ".join(f"'{c}'" for c in cities)
    code_list = ", ".join(f"'{c}'" for c in EVENT_CODES)
    ctx.sql(f"""
        INSERT INTO sim_logistics.tracking_events
        SELECT 60000000 + row_number() OVER (ORDER BY s.shipment_id, g.i),
               s.shipment_id,
               e.event_code,
               'Carrier scan: ' || lower(replace(e.event_code, '_', ' ')),
               [{city_list}][1 + ({te} >> 8) % {len(cities)}],
               1 + ({te} >> 16) % 5,
               s.shipped_at + INTERVAL 1 HOUR * (g.i * 8 + ({te} >> 20) % 7),
               e.event_code = 'EXCEPTION'
        FROM sim_logistics.shipments s
        CROSS JOIN generate_series(1, {TRACKING_PER_SHIPMENT}) AS g(i)
        CROSS JOIN LATERAL (SELECT CASE
                   WHEN g.i = {TRACKING_PER_SHIPMENT} AND s.delivered_at IS NOT NULL
                        THEN 'DELIVERED'
                   WHEN ({te} >> 4) % 100 < 4 THEN 'EXCEPTION'
                   ELSE [{code_list}][1 + g.i % 4] END AS event_code) e
        WHERE s.shipment_status <> 'label_created'
    """)

    # The own-fleet routing. Small, and read by nothing.
    rt = streams.draw(ctx.seed, "'sim_logistics.delivery_routes'", "d.ds", "g.i")
    ctx.sql("""
        CREATE OR REPLACE TABLE sim_logistics.delivery_routes (
            route_id BIGINT PRIMARY KEY,
            carrier_id INTEGER NOT NULL,
            origin_warehouse_id INTEGER NOT NULL,
            driver_employee_id INTEGER NOT NULL,
            vehicle_plate VARCHAR NOT NULL,
            route_date DATE NOT NULL,
            planned_start TIMESTAMP NOT NULL,
            actual_end TIMESTAMP,
            total_distance_km DECIMAL(12,3) NOT NULL,
            route_status VARCHAR NOT NULL
        )
    """)
    ctx.sql(f"""
        INSERT INTO sim_logistics.delivery_routes
        SELECT 450000 + row_number() OVER (ORDER BY d.ds, g.i),
               15,
               1 + {rt} % 12,
               2000 + ({rt} >> 8) % 1100,
               'CL' || lpad((({rt} >> 16) % 99999)::VARCHAR, 5, '0'),
               d.ds,
               d.ds::TIMESTAMP + INTERVAL 6 HOUR + INTERVAL 15 MINUTE * (({rt} >> 24) % 8),
               d.ds::TIMESTAMP + INTERVAL 15 HOUR + INTERVAL 1 MINUTE * (({rt} >> 28) % 220),
               round(40 + ({rt} >> 36) % 22000 / 100.0, 3)::DECIMAL(12,3),
               CASE WHEN ({rt} >> 44) % 100 < 93 THEN 'closed' ELSE 'exception' END
        FROM ({_refs.day_series(ctx)}) d
        CROSS JOIN generate_series(1, {_refs.per_day(ctx, ROUTES)}) AS g(i)
    """)

    rs = streams.draw(ctx.seed, "'sim_logistics.route_stops'", "r.route_id", "g.i")
    ctx.sql("""
        CREATE OR REPLACE TABLE sim_logistics.route_stops (
            stop_id BIGINT PRIMARY KEY,
            route_id BIGINT NOT NULL,
            shipment_id BIGINT,
            stop_sequence INTEGER NOT NULL,
            address_id BIGINT NOT NULL,
            eta TIMESTAMP NOT NULL,
            actual_arrival TIMESTAMP,
            dwell_minutes INTEGER,
            stop_status VARCHAR NOT NULL,
            failure_reason VARCHAR
        )
    """)
    eta = "r.planned_start + INTERVAL 20 MINUTE * g.i"
    failed = f"(({rs} >> 8) % 100 >= 96)"
    ctx.sql(f"""
        INSERT INTO sim_logistics.route_stops
        SELECT 8810000 + row_number() OVER (ORDER BY r.route_id, g.i),
               r.route_id,
               3100000 + {rs} % 90000,
               g.i,
               {_refs.SYNTHETIC_ADDRESS_BASE} + ({rs} >> 16) % 90000,
               {eta},
               CASE WHEN NOT {failed}
                    THEN {eta} + INTERVAL 1 MINUTE * (({rs} >> 24) % 40 - 12) END,
               CASE WHEN NOT {failed} THEN 4 + ({rs} >> 32) % 22 END,
               CASE WHEN {failed} THEN 'failed' ELSE 'delivered' END,
               CASE WHEN {failed} THEN ['no_access', 'refused', 'not_home'
                    ][1 + ({rs} >> 40) % 3] END
        FROM sim_logistics.delivery_routes r
        CROSS JOIN generate_series(1, 8) AS g(i)
    """)

    _plant_sample(ctx)


def _plant_sample(ctx: Context) -> None:
    """The sample storyline's physical leg: order 8840127 ships three units of
    variant 550231 out of warehouse 7 on Brightline Freight, delivered on the
    17th, seventeen minutes after the estimate on route 440112."""
    ctx.sql(f"""
        INSERT INTO sim_logistics.fulfillments VALUES
        ({SAMPLE_FULFILLMENT}, {SAMPLE_ORDER}, 'ship_from_dc', {SAMPLE_WAREHOUSE},
         NULL, NULL, 2870, TIMESTAMP '2026-04-18 23:59:00',
         TIMESTAMP '2026-04-15 06:20:00', TIMESTAMP '2026-04-15 08:05:00',
         TIMESTAMP '2026-04-15 14:40:00', TIMESTAMP '2026-04-17 15:22:00', 'delivered')
    """)
    ctx.sql(f"""
        INSERT INTO sim_logistics.shipments VALUES
        ({SAMPLE_SHIPMENT}, 'SH-{SAMPLE_SHIPMENT}', 12, 'outbound', {SAMPLE_WAREHOUSE},
         NULL, NULL, {SAMPLE_DEST_ADDRESS}, NULL, 'NB4471982203', 'ground_2day',
         5.400, 14.8200, 'USD', TIMESTAMP '2026-04-15 14:40:00',
         TIMESTAMP '2026-04-17 23:59:00', TIMESTAMP '2026-04-17 15:22:00', 'delivered'),
        ({SAMPLE_SHIPMENT_IN}, 'SH-{SAMPLE_SHIPMENT_IN}', 10, 'inbound', NULL,
         NULL, 118, NULL, {SAMPLE_WAREHOUSE}, 'CS0044719822', 'freight_std',
         6120.000, 2840.5500, 'USD', TIMESTAMP '2026-03-19 09:15:00',
         TIMESTAMP '2026-03-26 23:59:00', TIMESTAMP '2026-03-26 07:12:00', 'delivered'),
        ({SAMPLE_SHIPMENT_TRANSFER}, 'SH-{SAMPLE_SHIPMENT_TRANSFER}', 10, 'transfer',
         {SAMPLE_WAREHOUSE}, NULL, NULL, NULL, 1, 'CS0044718801', 'freight_std',
         820.000, 410.0000, 'USD', TIMESTAMP '2026-04-09 06:30:00',
         TIMESTAMP '2026-04-10 23:59:00', TIMESTAMP '2026-04-10 08:15:00', 'delivered')
    """)
    ctx.sql(f"""
        INSERT INTO sim_logistics.shipment_lines VALUES
        ({SAMPLE_SHIPMENT_LINE}, {SAMPLE_SHIPMENT}, {SAMPLE_VARIANT}, 3.000,
         {SAMPLE_ORDER_LINE}, NULL, NULL, NULL)
    """)
    ctx.sql(f"""
        INSERT INTO sim_logistics.tracking_events VALUES
        ({SAMPLE_TRACKING_EVENT}, {SAMPLE_SHIPMENT}, 'OUT_FOR_DELIVERY',
         'On vehicle for delivery', 'Brooklyn', 1,
         TIMESTAMP '2026-04-17 07:12:00', false)
    """)
    ctx.sql(f"""
        INSERT INTO sim_logistics.delivery_routes VALUES
        (440112, 15, {SAMPLE_WAREHOUSE}, 2870, 'CL04412', DATE '2026-04-17',
         TIMESTAMP '2026-04-17 06:30:00', TIMESTAMP '2026-04-17 16:48:00',
         184.400, 'closed')
    """)
    ctx.sql(f"""
        INSERT INTO sim_logistics.route_stops VALUES
        (8801447, 440112, {SAMPLE_SHIPMENT}, 23, {SAMPLE_DEST_ADDRESS},
         TIMESTAMP '2026-04-17 14:55:00', TIMESTAMP '2026-04-17 15:12:00',
         11, 'delivered', NULL)
    """)
