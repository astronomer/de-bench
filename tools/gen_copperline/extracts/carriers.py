"""S6b — the three carrier feeds: lanes, shipments, packages and invoices.

One CSV per carrier per week into `landing/carriers/<carrier>/`, a monthly
invoice beside it, and the rate cards, which are hand-authored and ship as
source (extract convention 7). This module writes everything except the cards.

**The package draw is this module's, not the upstream's.** `sim_logistics`
keeps Copperline's own record of what it dispatched, and it is small on
purpose — the WMS knows about a fraction of what the carriers moved. The
carrier feed is the carriers' record, at package grain, and it is sized from
spec chapter 03 section 11 rather than derived from the WMS:

* **9,200 packages a day inside the repricing window**, the ninety days from
  2026-02-01 to 2026-05-01 — 828,000 rows — and about 2,900 a day outside it.
  The window is the contractor season, and the three-to-one asymmetry is the
  whole of FIN-388's cost pressure.
* A shipment carries **2.20 packages on average** (1 to 5, weighted), so the
  daily shipment target is the package target over that mean and the shipment
  count lands near 1,400,000 over the range.
* Every `sim_logistics.shipments` row on one of the three covered carriers is
  folded in by id, so a shipment Copperline's own WMS knows about is the same
  shipment in the carrier's file. Drawn shipments take ids from a band well
  clear of the upstream's, so the two can never collide.

**`ship_date` is local at origin, `ship_time_utc` is not.** The two disagree
for every package dispatched near the ends of the origin's day, and the size
of the disagreement changes on the two 2026 DST days, both of which sit inside
the repricing window. Reading `ship_time_utc::DATE` as the ship date is right
most of the time and wrong on a population that moves when the clocks do.

**`brand` is armed and the affected lane set is never stated.** Northwave ran
its parcel operation out of one distribution centre, and the packages that
still move on those lanes are branded `northwave`. Nothing says which lanes
those are; the brand column and the acquired estate are the two ways to find
out.

**`billed_cents` is what the carrier billed**, which is the version 1 rate
card applied to the package's (carrier, service, zone, weight break). The
formula is `RATE_BASE_CENTS`, `ZONE_STEP_CENTS` and `WEIGHT_STEP_CENTS`
below, and `raw.carrier_rate_cards` — hand-authored, not written here — has to
carry the same numbers or FIN-388 has no answer to reach.
"""

from __future__ import annotations

import datetime as dt

from .. import streams
from ..config import Context
from . import _landing

# The three carriers whose feeds Copperline receives, by their SCAC codes in
# sim_logistics.CARRIERS. Two service levels each makes the six
# carrier-service pairs the rate card is sized on.
CARRIERS = {
    "BRFR": (12, "Brightline Freight", ("ground_2day", "express_next")),
    "PLPC": (13, "Pallas Parcel", ("ground_2day", "express_next")),
    "MWLG": (10, "Merriweather Logistics", ("ground_5day", "freight_std")),
}

# The rate card, version 1, in cents. A package's billed amount is
# base + zone * zone step + (weight break in kg) * weight step.
RATE_BASE_CENTS = {"ground_2day": 780, "ground_5day": 520,
                   "express_next": 1840, "freight_std": 4100}
ZONE_STEP_CENTS = {"BRFR": 96, "PLPC": 88, "MWLG": 145}
WEIGHT_STEP_CENTS = {"ground_2day": 41, "ground_5day": 28,
                     "express_next": 96, "freight_std": 66}
WEIGHT_BREAKS_G = (1_000, 5_000, 15_000, 30_000, 70_000)

# The repricing window: ninety days inside FY2026 Q1, the contractor season.
WINDOW_START = dt.date(2026, 2, 1)
WINDOW_END = dt.date(2026, 5, 1)
PACKAGES_IN_WINDOW = 9_200
PACKAGES_OUTSIDE = 2_900
PACKAGES_PER_SHIPMENT = 2.20

# Key bands. Both sit clear of sim_logistics.shipments, which numbers from
# 3,100,000, and of the 8,000,000..8,999,999 planted-row band.
SHIPMENT_ID_BASE = 3_300_000_000
SHIPMENT_SLOTS_PER_DAY = 20_000
PACKAGE_ID_BASE = 4_400_000_000
UPSTREAM_PACKAGE_ID_BASE = 4_300_000_000
PACKAGES_PER_SHIPMENT_MAX = 5

# Northwave ran its parcel operation out of Atlanta. Every lane off that
# distribution centre still carries the acquired brand.
NORTHWAVE_ORIGIN = "DC-ATL"

# The lane network: 44 lanes, 35 across North America and 9 inside Europe.
NA_ORIGINS = ("DC-POR", "DC-SAC", "DC-DAL", "DC-ATL", "DC-CHI",
              "HUB-DEN", "DC-EDI", "HUB-SEA", "RET-COL")
NA_DESTS = (
    ("US-NE", "US", "America/New_York"), ("US-SE", "US", "America/New_York"),
    ("US-MW", "US", "America/Chicago"), ("US-SW", "US", "America/Denver"),
    ("US-W", "US", "America/Los_Angeles"),
    ("CA-E", "CA", "America/Toronto"), ("CA-W", "CA", "America/Vancouver"),
)
EU_ORIGINS = ("DC-LEE", "HUB-DUB", "DC-HAM")
EU_DESTS = (("GB", "GB", "Europe/London"), ("IE", "IE", "Europe/Dublin"),
            ("DE", "DE", "Europe/Berlin"))
ORIGIN_TZ = {
    "DC-POR": "America/Los_Angeles", "DC-SAC": "America/Los_Angeles",
    "DC-DAL": "America/Chicago", "DC-ATL": "America/New_York",
    "DC-CHI": "America/Chicago", "HUB-DEN": "America/Denver",
    "DC-EDI": "America/New_York", "HUB-SEA": "America/Los_Angeles",
    "RET-COL": "America/New_York", "DC-LEE": "Europe/London",
    "HUB-DUB": "Europe/Dublin", "DC-HAM": "Europe/Berlin",
}
NA_LANES = 35

# A week of packages inside the repricing window is 64,000 rows, so RET-1's
# whole 90-day vendor window would be 120 MB of CSV in every trial. Thirty
# days plus the key dates is the same tree at a fifth of the cost. See
# extracts/_landing.py.
LANDING_DAYS = 30

PACKAGE_FILE_COLUMNS = [
    "package_id", "shipment_id", "order_id", "carrier_code", "service_level",
    "lane_id", "origin_dc", "dest_zip3", "dest_country", "ship_date",
    "ship_time_utc", "delivered_at", "billed_weight_g", "zone", "billed_cents",
    "accessorial_cents", "brand",
]


def _h(ctx: Context, stream: str, *parts: str) -> str:
    return streams.draw(ctx.seed, f"'{stream}'", *parts)


def lanes() -> list[tuple]:
    """(lane_id, origin_dc, dest_region, dest_country, region_code,
    origin_tz_name, dest_tz_name). 44 lanes, laid out so the set is stable
    when a destination is added at the end."""
    rows = []
    for origin in NA_ORIGINS:
        for region, country, zone in NA_DESTS:
            rows.append((origin, region, country, zone))
    rows = rows[:NA_LANES]
    for origin in EU_ORIGINS:
        for region, country, zone in EU_DESTS:
            rows.append((origin, region, country, zone))
    return [
        (f"LANE-{n:02d}", origin, region, country,
         f"{country}-{origin.split('-')[1]}", ORIGIN_TZ[origin], zone)
        for n, (origin, region, country, zone) in enumerate(rows, start=1)
    ]


def build(ctx: Context) -> None:
    from ._boundary import ensure_keys_or_stub

    ensure_keys_or_stub(ctx)  # shipment order refs come from _util.order_keys
    _lanes(ctx)
    _shipments(ctx)
    _packages(ctx)
    _invoices(ctx)
    _files(ctx)
    ctx.sql("DROP TABLE IF EXISTS _carrier_shipment")


# --- raw.lanes -------------------------------------------------------------

def _lanes(ctx: Context) -> None:
    ctx.sql("""
        CREATE OR REPLACE TABLE raw.lanes (
            lane_id VARCHAR NOT NULL PRIMARY KEY,
            origin_dc VARCHAR NOT NULL,
            dest_region VARCHAR NOT NULL,
            dest_country VARCHAR NOT NULL,
            region_code VARCHAR NOT NULL,
            origin_tz_name VARCHAR NOT NULL,
            dest_tz_name VARCHAR NOT NULL,
            active_from DATE NOT NULL,
            active_to DATE
        )
    """)
    ctx.con.executemany(
        "INSERT INTO raw.lanes VALUES (?,?,?,?,?,?,?,?,?)",
        [(*row, ctx.start - dt.timedelta(days=365), None) for row in lanes()])


# --- the shipment population -----------------------------------------------

def _shipments(ctx: Context) -> None:
    """Shipment grain: the carrier's own header for a consignment.

    Two populations. The drawn one meets the daily package target; the
    upstream one is every `sim_logistics.shipments` row on a covered carrier,
    folded in by id so the WMS and the carrier agree about the shipments they
    both know.
    """
    ctx.sql("""
        CREATE OR REPLACE TABLE raw.shipments (
            shipment_id VARCHAR NOT NULL PRIMARY KEY,
            carrier_code VARCHAR NOT NULL,
            order_id VARCHAR,
            service_level VARCHAR NOT NULL,
            lane_id VARCHAR NOT NULL,
            origin_dc VARCHAR NOT NULL,
            dest_zip3 VARCHAR NOT NULL,
            dest_country VARCHAR NOT NULL,
            ship_date DATE NOT NULL,
            ship_time_utc TIMESTAMP NOT NULL,
            delivered_at TIMESTAMP,
            package_count INTEGER NOT NULL,
            total_weight_g BIGINT NOT NULL,
            brand VARCHAR NOT NULL,
            shipment_status VARCHAR NOT NULL,
            loaded_at TIMESTAMP NOT NULL
        )
    """)
    n_lanes = len(lanes())
    day_target = _day_target(ctx)
    draw = _h(ctx, "raw.shipments", "d.ds", "g.i")
    body = _shipment_body(ctx, n_lanes)
    ctx.sql(f"""
        CREATE OR REPLACE TABLE _carrier_shipment AS
        WITH t AS (
            SELECT d.ds, d.dn,
                   greatest(1, round({day_target} / {PACKAGES_PER_SHIPMENT}))::INTEGER
                       AS shipments_target
            FROM _util.days d
        ), s AS (
            SELECT t.ds, t.dn, g.i AS slot,
                   {SHIPMENT_ID_BASE} + t.dn * {SHIPMENT_SLOTS_PER_DAY} + g.i AS shipment_key,
                   {draw.replace('d.ds', 't.ds')} AS dw
            FROM t
            CROSS JOIN LATERAL generate_series(1, t.shipments_target) AS g(i)
        )
        {body}
    """)
    # The upstream's own shipments, on the carriers whose feed we receive.
    up = _h(ctx, "raw.shipments.upstream", "u.shipment_id")
    covered = ", ".join(str(cid) for cid, _, _ in CARRIERS.values())
    carrier_from_id = " ".join(
        f"WHEN {cid} THEN '{code}'" for code, (cid, _, _) in CARRIERS.items())
    service_from_id = " ".join(
        f"WHEN {cid} THEN ['{levels[0]}', '{levels[1]}'][1 + ({up} >> 24) % 2]"
        for _code, (cid, _name, levels) in CARRIERS.items())
    ctx.sql(f"""
        INSERT INTO _carrier_shipment
        SELECT u.shipped_at::DATE AS ds,
               (u.shipped_at::DATE - DATE '{ctx.start}')::INTEGER AS dn,
               0 AS slot,
               u.shipment_id AS shipment_key,
               {up} AS dw,
               (CASE u.carrier_id {carrier_from_id} END) AS carrier_code,
               l.lane_id, l.origin_dc, l.dest_country, l.origin_tz_name,
               -- The upstream draws over every service level the network
               -- offers; a carrier only bills the two its own card covers, so
               -- the fold-in lands on one of those.
               (CASE u.carrier_id {service_from_id} END) AS service_level,
               1 + ({up} >> 8) % {PACKAGES_PER_SHIPMENT_MAX} AS package_count,
               u.shipped_at AS ship_time_utc,
               u.delivered_at,
               CASE WHEN u.delivered_at IS NOT NULL THEN 'delivered'
                    ELSE 'in_transit' END AS shipment_status,
               ok.raw_order_id AS order_id,
               lpad((({up} >> 40) % 1000)::VARCHAR, 3, '0') AS dest_zip3
        FROM sim_logistics.shipments u
        JOIN _util.days d ON d.ds = u.shipped_at::DATE
        LEFT JOIN _util.order_keys ok
          ON ok.order_id = 9000000 + d.dn * 200000 + 1 + ({up} >> 16) % d.orders
        JOIN raw.lanes l
          ON l.lane_id = 'LANE-' || lpad((1 + ({up} >> 4) % {n_lanes})::VARCHAR, 2, '0')
        WHERE u.carrier_id IN ({covered})
    """)
    ctx.sql(f"""
        INSERT INTO raw.shipments
        SELECT 'SH-' || s.shipment_key,
               s.carrier_code,
               s.order_id,
               s.service_level,
               s.lane_id,
               s.origin_dc,
               s.dest_zip3,
               s.dest_country,
               -- Local at origin. This is the column the carrier bills on and
               -- the one a UTC-truncating reader gets wrong.
               ((s.ship_time_utc AT TIME ZONE 'UTC')
                   AT TIME ZONE s.origin_tz_name)::DATE,
               s.ship_time_utc,
               s.delivered_at,
               s.package_count,
               (s.package_count * (900 + (s.dw >> 44) % 26000))::BIGINT,
               CASE WHEN s.origin_dc = '{NORTHWAVE_ORIGIN}'
                    THEN 'northwave' ELSE 'copperline' END,
               s.shipment_status,
               -- One file a week, sent on the Monday after the ship week.
               date_trunc('week', s.ship_time_utc)::TIMESTAMP + INTERVAL 7 DAY
                   + INTERVAL 9 HOUR + INTERVAL 1 MINUTE * ((s.dw >> 52) % 50)
        FROM _carrier_shipment s
        ORDER BY s.ship_time_utc, s.shipment_key
    """)


def _day_target(ctx: Context) -> str:
    """Packages a day: the repricing window's rate inside it, the ordinary
    rate outside. Scaled by profile, because these are fact volumes."""
    return (f"(CASE WHEN d.ds BETWEEN DATE '{WINDOW_START}' AND DATE '{WINDOW_END}' "
            f"THEN {ctx.scale(PACKAGES_IN_WINDOW)} "
            f"ELSE {ctx.scale(PACKAGES_OUTSIDE)} END)")


def _carrier_pick(draw: str) -> str:
    return streams.pick(draw, [(f"'{code}'", weight) for code, weight
                               in zip(CARRIERS, (46, 33, 21))])


def _shipment_body(ctx: Context, n_lanes: int) -> str:
    """The drawn shipment's columns, off one draw per (day, slot)."""
    carrier = _carrier_pick("(s.dw >> 3)")
    service = " ".join(
        f"WHEN '{code}' THEN ['{levels[0]}', '{levels[1]}'][1 + (s.dw >> 12) % 2]"
        for code, (_, _, levels) in CARRIERS.items())
    # An order reference off the shared day-block formula, so a package names
    # an order that exists. See upstream/logistics.py for the formula.
    order_seq = "d.dn * 200000 + 1 + (s.dw >> 20) % d.orders"
    return f"""
        SELECT s.ds, s.dn, s.slot, s.shipment_key, s.dw,
               c.carrier_code,
               l.lane_id, l.origin_dc, l.dest_country, l.origin_tz_name,
               (CASE c.carrier_code {service} END) AS service_level,
               1 + CASE WHEN s.dw % 100 < 35 THEN 0 WHEN s.dw % 100 < 65 THEN 1
                        WHEN s.dw % 100 < 85 THEN 2 WHEN s.dw % 100 < 95 THEN 3
                        ELSE 4 END AS package_count,
               s.ds::TIMESTAMP + INTERVAL 1 MINUTE * ((s.dw >> 28) % 1440)
                   AS ship_time_utc,
               CASE WHEN (s.dw >> 40) % 100 < 91
                    THEN s.ds::TIMESTAMP + INTERVAL 1 MINUTE * ((s.dw >> 28) % 1440)
                         + INTERVAL 1 DAY * (1 + (s.dw >> 42) % 5)
                         + INTERVAL 1 HOUR * ((s.dw >> 46) % 11) END AS delivered_at,
               CASE WHEN (s.dw >> 40) % 100 < 91 THEN 'delivered'
                    ELSE 'in_transit' END AS shipment_status,
               ok.raw_order_id AS order_id,
               lpad(((s.dw >> 48) % 1000)::VARCHAR, 3, '0') AS dest_zip3
        FROM s
        JOIN _util.days d ON d.ds = s.ds
        LEFT JOIN _util.order_keys ok
          ON ok.order_id = {9_000_000} + {order_seq}
        JOIN raw.lanes l
          ON l.lane_id = 'LANE-' || lpad((1 + (s.dw >> 8) % {n_lanes})::VARCHAR, 2, '0')
        CROSS JOIN (SELECT {carrier} AS carrier_code) c
    """


# --- raw.shipment_packages -------------------------------------------------

def _packages(ctx: Context) -> None:
    """One row per package. The shipment's package count decides how many, so
    the day's package total is the target the shipment count was sized from."""
    ctx.sql("""
        CREATE OR REPLACE TABLE raw.shipment_packages (
            package_id VARCHAR NOT NULL PRIMARY KEY,
            shipment_id VARCHAR NOT NULL,
            order_id VARCHAR,
            carrier_code VARCHAR NOT NULL,
            service_level VARCHAR NOT NULL,
            lane_id VARCHAR NOT NULL,
            origin_dc VARCHAR NOT NULL,
            dest_zip3 VARCHAR NOT NULL,
            dest_country VARCHAR NOT NULL,
            ship_date DATE NOT NULL,
            ship_time_utc TIMESTAMP NOT NULL,
            delivered_at TIMESTAMP,
            billed_weight_g BIGINT NOT NULL,
            zone INTEGER NOT NULL,
            billed_cents BIGINT NOT NULL,
            accessorial_cents BIGINT NOT NULL,
            brand VARCHAR NOT NULL,
            loaded_at TIMESTAMP NOT NULL
        )
    """)
    draw = _h(ctx, "raw.shipment_packages", "s.shipment_key", "g.k")
    breaks = ", ".join(str(w) for w in WEIGHT_BREAKS_G)
    base = _rate_case(RATE_BASE_CENTS, "r.service_level")
    step = _rate_case(WEIGHT_STEP_CENTS, "r.service_level")
    zone_step = _rate_case(ZONE_STEP_CENTS, "r.carrier_code")
    ctx.sql(f"""
        INSERT INTO raw.shipment_packages
        WITH p AS (
            SELECT h.*, s.shipment_key, s.slot, s.dn, g.k,
                   {draw} AS dw,
                   (1 + ({draw} >> 4) % 400 + ({draw} >> 14) % 26000)::BIGINT
                       AS billed_weight_g
            FROM _carrier_shipment s
            JOIN raw.shipments h ON h.shipment_id = 'SH-' || s.shipment_key
            CROSS JOIN generate_series(1, {PACKAGES_PER_SHIPMENT_MAX}) AS g(k)
            WHERE g.k <= h.package_count
        ), r AS (
            SELECT p.*,
                   2 + (p.dw >> 24) % 7 AS zone,
                   [{breaks}][least(5, 1 + (
                       CASE WHEN p.billed_weight_g > 1000 THEN 1 ELSE 0 END
                       + CASE WHEN p.billed_weight_g > 5000 THEN 1 ELSE 0 END
                       + CASE WHEN p.billed_weight_g > 15000 THEN 1 ELSE 0 END
                       + CASE WHEN p.billed_weight_g > 30000 THEN 1 ELSE 0 END))]
                       AS weight_break_g
            FROM p
        )
        SELECT 'PKG-' || lpad((CASE WHEN r.slot = 0
                    THEN {UPSTREAM_PACKAGE_ID_BASE} + r.shipment_key * 8
                    ELSE {PACKAGE_ID_BASE} + (r.dn * {SHIPMENT_SLOTS_PER_DAY}
                                              + r.slot) * 8 END
                    + r.k)::VARCHAR, 12, '0'),
               r.shipment_id, r.order_id, r.carrier_code, r.service_level,
               r.lane_id, r.origin_dc, r.dest_zip3, r.dest_country,
               r.ship_date, r.ship_time_utc, r.delivered_at,
               r.billed_weight_g, r.zone,
               ({base} + r.zone * {zone_step}
                + (r.weight_break_g / 1000) * {step})::BIGINT,
               CASE WHEN (r.dw >> 32) % 100 < 18
                    THEN (250 + (r.dw >> 36) % 1400)::BIGINT ELSE 0 END,
               r.brand,
               r.loaded_at
        FROM r
        ORDER BY r.ship_time_utc, r.shipment_id, r.k
    """)


def _rate_case(mapping: dict, column: str) -> str:
    arms = " ".join(f"WHEN '{key}' THEN {value}" for key, value in mapping.items())
    return f"(CASE {column} {arms} ELSE 0 END)"


# --- raw.carrier_invoices --------------------------------------------------

def _invoices(ctx: Context) -> None:
    """One invoice per carrier per month, plus the opening one each carrier
    sent in arrears at go-live: 3 carriers by 30 months, 90 rows."""
    ctx.sql("""
        CREATE OR REPLACE TABLE raw.carrier_invoices (
            carrier_invoice_id VARCHAR NOT NULL PRIMARY KEY,
            carrier_code VARCHAR NOT NULL,
            invoice_date DATE NOT NULL,
            period_start DATE NOT NULL,
            period_end DATE NOT NULL,
            total_cents BIGINT NOT NULL,
            status VARCHAR NOT NULL
        )
    """)
    months = []
    cursor = _month_before(dt.date(ctx.start.year, ctx.start.month, 1))
    while len(months) < 30:
        following = _next_month(cursor)
        months.append((cursor, min(following - dt.timedelta(days=1), ctx.end)))
        cursor = following
    values = ", ".join(f"(DATE '{start}', DATE '{end}')" for start, end in months)
    ctx.sql(f"""
        INSERT INTO raw.carrier_invoices
        WITH m(period_start, period_end) AS (VALUES {values}),
        c(carrier_code) AS (VALUES {', '.join(f"('{code}')" for code in CARRIERS)}),
        t AS (
            SELECT c.carrier_code, m.period_start, m.period_end,
                   coalesce(sum(p.billed_cents + p.accessorial_cents), 0)::BIGINT AS total
            FROM c CROSS JOIN m
            LEFT JOIN raw.shipment_packages p
                   ON p.carrier_code = c.carrier_code
                  AND p.ship_date BETWEEN m.period_start AND m.period_end
            GROUP BY 1, 2, 3
        )
        SELECT 'CINV-' || t.carrier_code || '-' || strftime(t.period_start, '%Y%m'),
               t.carrier_code,
               t.period_end + INTERVAL 6 DAY,
               t.period_start, t.period_end, t.total,
               CASE WHEN t.period_end + INTERVAL 36 DAY <= DATE '{ctx.today}' THEN 'paid'
                    WHEN t.period_end + INTERVAL 6 DAY <= DATE '{ctx.today}' THEN 'open'
                    ELSE 'draft' END
        FROM t
        ORDER BY t.carrier_code, t.period_start
    """)


def _month_before(first: dt.date) -> dt.date:
    previous = first - dt.timedelta(days=1)
    return dt.date(previous.year, previous.month, 1)


def _next_month(day: dt.date) -> dt.date:
    return (dt.date(day.year + 1, 1, 1) if day.month == 12
            else dt.date(day.year, day.month + 1, 1))


# --- the landing tree ------------------------------------------------------

def _files(ctx: Context) -> None:
    """`landing/carriers/<carrier>/`: one package file a week and one invoice
    a month. The carriers are third parties, so RET-1 keeps 90 days."""
    dates = _landing.window(ctx, LANDING_DAYS)
    weeks = sorted({day - dt.timedelta(days=(day - ctx.start).days % 7)
                    for day in dates})
    root = ctx.landing_dir("carriers")
    columns = ", ".join(PACKAGE_FILE_COLUMNS)
    for code in CARRIERS:
        directory = root / code.lower()
        for week in weeks:
            _landing.copy_csv(ctx, directory / f"packages_{week}.csv", f"""
                SELECT {columns} FROM raw.shipment_packages
                WHERE carrier_code = '{code}'
                  AND ship_date >= DATE '{week}'
                  AND ship_date < DATE '{week + dt.timedelta(days=7)}'
                ORDER BY package_id
            """)
        _landing.copy_csv(ctx, directory / "invoices.csv", f"""
            SELECT carrier_invoice_id, carrier_code, invoice_date, period_start,
                   period_end, total_cents, status
            FROM raw.carrier_invoices
            WHERE carrier_code = '{code}'
              AND period_end >= DATE '{min(weeks)}'
            ORDER BY period_start
        """)
