"""`sim_core` — reference and master data, spec chapter 02 section 4.

Six tables every other schema points at. Four of them are small enough to
read (currencies, countries, regions, tax rates) and are written from Python
literals; the address pool and the daily rate table are drawn in SQL.

Two things here are contracts other modules build on.

**Region ids.** Districts occupy 3301..3399, three to a metro, and metros
occupy 1201..1233, so metro *k* owns districts 3298+3k..3300+3k. That is what
puts Brooklyn South at 3312 under the New York metro at 1204, the two region
ids chapter 02 samples.

**The address pool.** 250,000 rows at 7,700,001..7,950,000, the same at every
profile, because stores, warehouses and suppliers reference addresses by
value and a boundary address must exist at both. A module that needs an
address for a drawn entity takes `7700001 + (draw % 250000)`.

The rate walk is smooth but per-day independent: a seasonal term over the day
index plus a per-day draw of a few basis points. No day reads the day before
it, so regenerating one day never moves another.
"""

from __future__ import annotations

from ..config import Context
from ..streams import uniform
from . import _util
from ._util import h

# --- reference populations ------------------------------------------------

# Copperline bills in USD, GBP, EUR and MXN. The other four are here because
# core.countries.currency_code is a foreign key and the spec says that column
# holds the country's own currency, not the market's billing currency.
CURRENCIES = [
    ("USD", "US Dollar", "$", 2),
    ("CAD", "Canadian Dollar", "$", 2),
    ("GBP", "Pound Sterling", "£", 2),
    ("EUR", "Euro", "€", 2),
    ("MXN", "Mexican Peso", "$", 2),
    ("BRL", "Brazilian Real", "R$", 2),
    ("PLN", "Polish Zloty", "zł", 2),
    ("IDR", "Indonesian Rupiah", "Rp", 0),
]

COUNTRIES = [
    (1, "US", "USA", "United States", "USD"),
    (2, "CA", "CAN", "Canada", "CAD"),
    (3, "GB", "GBR", "United Kingdom", "GBP"),
    (4, "IE", "IRL", "Ireland", "EUR"),
    (5, "DE", "DEU", "Germany", "EUR"),
    (6, "BR", "BRA", "Brazil", "BRL"),
    (7, "MX", "MEX", "Mexico", "MXN"),
    (8, "PL", "POL", "Poland", "PLN"),
    (9, "ID", "IDN", "Indonesia", "IDR"),
]

# The currency a market bills in, which is not always the country's own —
# chapter 01 section 3.4 holds the reasons. Every module that stamps a
# currency on a row reads this, so the world agrees with raw.market_config.
BILLING_CURRENCY = {1: "USD", 2: "USD", 3: "GBP", 4: "EUR", 5: "EUR",
                    6: "USD", 7: "MXN", 8: "EUR", 9: "USD"}

COUNTRY_REGION_LO = 1280   # country node = 1280 + country_id
STATE_LO = 1241
METRO_LO = 1201
DISTRICT_LO = 3301

STATES = [
    ("OR", "Oregon", 1), ("WA", "Washington", 1), ("IL", "Illinois", 1),
    ("NY", "New York", 1), ("CA", "California", 1), ("TX", "Texas", 1),
    ("FL", "Florida", 1), ("MA", "Massachusetts", 1), ("GA", "Georgia", 1),
    ("CO", "Colorado", 1), ("AZ", "Arizona", 1), ("PA", "Pennsylvania", 1),
    ("MI", "Michigan", 1),
    ("ON", "Ontario", 2), ("QC", "Quebec", 2), ("BC", "British Columbia", 2),
    ("AB", "Alberta", 2),
    ("ENG", "England", 3), ("SCT", "Scotland", 3), ("WLS", "Wales", 3),
    ("L", "Leinster", 4), ("M", "Munster", 4),
    ("BY", "Bayern", 5), ("NW", "Nordrhein-Westfalen", 5), ("BE", "Berlin", 5),
    ("HE", "Hessen", 5),
    ("SP", "Sao Paulo", 6),
    ("CMX", "Ciudad de Mexico", 7),
    ("MZ", "Mazowieckie", 8),
    ("JK", "Jakarta", 9),
]

# (name, state code, postal prefix, latitude, longitude). Order is fixed:
# the New York metro must be the fourth so that its districts land on
# 3310..3312 and Brooklyn South keeps the id chapter 02 samples.
METROS = [
    ("Portland", "OR", "972", 45.5152, -122.6784),
    ("Seattle", "WA", "981", 47.6062, -122.3321),
    ("Chicago", "IL", "606", 41.8781, -87.6298),
    ("New York", "NY", "112", 40.7128, -74.0060),
    ("Los Angeles", "CA", "900", 34.0522, -118.2437),
    ("San Francisco", "CA", "941", 37.7749, -122.4194),
    ("Dallas", "TX", "752", 32.7767, -96.7970),
    ("Miami", "FL", "331", 25.7617, -80.1918),
    ("Boston", "MA", "021", 42.3601, -71.0589),
    ("Atlanta", "GA", "303", 33.7490, -84.3880),
    ("Denver", "CO", "802", 39.7392, -104.9903),
    ("Phoenix", "AZ", "850", 33.4484, -112.0740),
    ("Philadelphia", "PA", "191", 39.9526, -75.1652),
    ("Detroit", "MI", "482", 42.3314, -83.0458),
    ("Toronto", "ON", "M5H", 43.6532, -79.3832),
    ("Montreal", "QC", "H2Y", 45.5019, -73.5674),
    ("Vancouver", "BC", "V6B", 49.2827, -123.1207),
    ("Calgary", "AB", "T2P", 51.0447, -114.0719),
    ("London", "ENG", "EC1", 51.5072, -0.1276),
    ("Manchester", "ENG", "M1", 53.4808, -2.2426),
    ("Birmingham", "ENG", "B1", 52.4862, -1.8904),
    ("Glasgow", "SCT", "G1", 55.8642, -4.2518),
    ("Cardiff", "WLS", "CF1", 51.4816, -3.1791),
    ("Dublin", "L", "D01", 53.3498, -6.2603),
    ("Cork", "M", "T12", 51.8985, -8.4756),
    ("Munich", "BY", "803", 48.1351, 11.5820),
    ("Cologne", "NW", "507", 50.9375, 6.9603),
    ("Berlin", "BE", "101", 52.5200, 13.4050),
    ("Frankfurt", "HE", "603", 50.1109, 8.6821),
    ("Sao Paulo", "SP", "011", -23.5505, -46.6333),
    ("Mexico City", "CMX", "060", 19.4326, -99.1332),
    ("Warsaw", "MZ", "00-", 52.2297, 21.0122),
    ("Jakarta", "JK", "104", -6.2088, 106.8456),
]

# The three districts of the New York metro carry real borough names, because
# the sample address in chapter 02 is a Brooklyn one.
NEW_YORK_DISTRICTS = [("Manhattan Midtown", "New York"),
                      ("Queens North", "Queens"),
                      ("Brooklyn South", "Brooklyn")]

TAX_CLASSES = ["standard", "home_standard", "garden_standard",
               "building_standard", "hardware_standard", "exempt"]

# Headline rate per country from 2025-01-01. The window before it is a
# little lower, and the class multiplier reduces building materials and
# zeroes the exempt class.
COUNTRY_RATE = {1: 0.0700, 2: 0.1300, 3: 0.2000, 4: 0.2300, 5: 0.1900,
                6: 0.1700, 7: 0.1600, 8: 0.2300, 9: 0.1100}
CLASS_MULTIPLIER = {"standard": 1.00, "home_standard": 1.00,
                    "garden_standard": 1.00, "building_standard": 0.90,
                    "hardware_standard": 1.00, "exempt": 0.00}

# Rate against the US dollar, and the phase of its seasonal term.
FX_BASE = [("CAD", 0.7400, 0.10), ("GBP", 1.2700, 0.35), ("EUR", 1.0800, 0.60),
           ("MXN", 0.0580, 0.85), ("BRL", 0.2000, 0.20), ("PLN", 0.2500, 0.45),
           ("IDR", 0.0000630, 0.70)]

STREETS = ["Rogers Ave", "Maple St", "Copper Row", "Bridge St", "Union Ave",
           "Harbor Way", "Chestnut St", "Kingsway", "Mill Lane", "Larch Rd",
           "Foundry St", "Beacon Ave", "Quarry Rd", "Willow Ct", "Grant St",
           "Sherman Ave", "Trellis Way", "Anchor St", "Camden Rd", "Ash Ln",
           "Pioneer Way", "Redwood Dr", "Slate Ave", "Orchard St"]


def _state_id(code: str) -> int:
    return STATE_LO + [s[0] for s in STATES].index(code)


def _regions() -> list[tuple]:
    """(region_id, name, level, parent_region_id, country_id)."""
    rows = [(COUNTRY_REGION_LO + cid, name, "country", None, cid)
            for cid, _, _, name, _ in COUNTRIES]
    for offset, (code, name, country_id) in enumerate(STATES):
        rows.append((STATE_LO + offset, name, "state",
                     COUNTRY_REGION_LO + country_id, country_id))
    for offset, (name, state_code, _, _, _) in enumerate(METROS):
        country_id = dict((s[0], s[2]) for s in STATES)[state_code]
        rows.append((METRO_LO + offset, f"{name} Metro", "metro",
                     _state_id(state_code), country_id))
    for offset, (name, state_code, _, _, _) in enumerate(METROS):
        country_id = dict((s[0], s[2]) for s in STATES)[state_code]
        metro_id = METRO_LO + offset
        names = (NEW_YORK_DISTRICTS if name == "New York" else
                 [(f"{name} North", name), (f"{name} Central", name),
                  (f"{name} South", name)])
        for j, (district_name, _city) in enumerate(names):
            rows.append((DISTRICT_LO + offset * 3 + j, district_name,
                         "district", metro_id, country_id))
    return rows


def _districts() -> list[tuple]:
    """The pick pool an address draws from: one row per district, with the
    city, state, postal prefix and centre point that go on the address."""
    by_code = {s[0]: s for s in STATES}
    out = []
    for offset, (metro, state_code, prefix, lat, lon) in enumerate(METROS):
        country_id = by_code[state_code][2]
        names = (NEW_YORK_DISTRICTS if metro == "New York" else
                 [(f"{metro} North", metro), (f"{metro} Central", metro),
                  (f"{metro} South", metro)])
        weight = 5 if country_id == 1 else 2 if country_id == 2 else 1
        for j, (_name, city) in enumerate(names):
            out.append((DISTRICT_LO + offset * 3 + j, city, state_code, prefix,
                        round(lat + (j - 1) * 0.06, 6),
                        round(lon + (j - 1) * 0.06, 6), country_id, weight))
    return out


def _tax_rates() -> list[tuple]:
    """(tax_rate_id, country_id, region_id, tax_class, rate_pct, valid_from,
    valid_to). Ids 201.. are the region-grain rows, 1001.. the country-grain
    ones. Chapter 02 pins 204: New York, home_standard, 8.88% from 2025."""
    old, new = ("2023-01-01", "2024-12-31"), ("2025-01-01", None)
    rows = []
    rate_id = 201
    for metro_offset, rate in ((3, 0.0888), (4, 0.1025), (2, 0.1025)):
        region_id = METRO_LO + metro_offset
        for tax_class in ("standard", "home_standard"):
            multiplier = CLASS_MULTIPLIER[tax_class]
            for valid_from, valid_to in (old, new):
                factor = 0.98 if valid_to else 1.0
                rows.append((rate_id, 1, region_id, tax_class,
                             round(rate * multiplier * factor, 4),
                             valid_from, valid_to))
                rate_id += 1
    rate_id = 1001
    for country_id, rate in sorted(COUNTRY_RATE.items()):
        for tax_class in TAX_CLASSES:
            multiplier = CLASS_MULTIPLIER[tax_class]
            for valid_from, valid_to in (old, new):
                factor = 0.98 if valid_to else 1.0
                rows.append((rate_id, country_id, None, tax_class,
                             round(rate * multiplier * factor, 4),
                             valid_from, valid_to))
                rate_id += 1
    return rows


# --- build ----------------------------------------------------------------

def build(ctx: Context) -> None:
    _util.ensure_days(ctx)
    ctx.sql("CREATE SCHEMA IF NOT EXISTS sim_core")

    ctx.sql("""
        CREATE OR REPLACE TABLE sim_core.currencies (
            currency_code VARCHAR PRIMARY KEY,
            name VARCHAR,
            symbol VARCHAR,
            minor_unit INTEGER
        )""")
    ctx.con.executemany("INSERT INTO sim_core.currencies VALUES (?,?,?,?)",
                        CURRENCIES)

    ctx.sql("""
        CREATE OR REPLACE TABLE sim_core.countries (
            country_id INTEGER PRIMARY KEY,
            iso2 VARCHAR,
            iso3 VARCHAR,
            name VARCHAR,
            currency_code VARCHAR
        )""")
    ctx.con.executemany("INSERT INTO sim_core.countries VALUES (?,?,?,?,?)",
                        COUNTRIES)

    ctx.sql("""
        CREATE OR REPLACE TABLE sim_core.regions (
            region_id INTEGER PRIMARY KEY,
            name VARCHAR,
            level VARCHAR,
            parent_region_id INTEGER,
            country_id INTEGER
        )""")
    ctx.con.executemany("INSERT INTO sim_core.regions VALUES (?,?,?,?,?)",
                        _regions())

    ctx.sql("""
        CREATE OR REPLACE TABLE sim_core.tax_rates (
            tax_rate_id INTEGER PRIMARY KEY,
            country_id INTEGER,
            region_id INTEGER,
            tax_class VARCHAR,
            rate_pct DECIMAL(6,4),
            valid_from DATE,
            valid_to DATE
        )""")
    ctx.con.executemany(
        "INSERT INTO sim_core.tax_rates VALUES (?,?,?,?,?,?,?)", _tax_rates())

    _addresses(ctx)
    _exchange_rates(ctx)


def _addresses(ctx: Context) -> None:
    """The address pool. Unscaled at every profile: entities elsewhere name
    an address by value, so the pool has to hold the same ids either way."""
    ctx.sql("""
        CREATE OR REPLACE TABLE sim_core._district_pick (
            rank INTEGER, region_id INTEGER, city VARCHAR, state_province VARCHAR,
            postal_prefix VARCHAR, latitude DOUBLE, longitude DOUBLE,
            country_id INTEGER
        )""")
    pool = []
    for region_id, city, state, prefix, lat, lon, country_id, weight in _districts():
        pool.extend([(region_id, city, state, prefix, lat, lon, country_id)] * weight)
    ctx.con.executemany(
        "INSERT INTO sim_core._district_pick VALUES (?,?,?,?,?,?,?,?)",
        [(rank, *row) for rank, row in enumerate(pool, start=1)])

    ctx.sql("CREATE OR REPLACE TABLE sim_core._street (rank INTEGER, name VARCHAR)")
    ctx.con.executemany("INSERT INTO sim_core._street VALUES (?,?)",
                        list(enumerate(STREETS, start=1)))

    ctx.sql("""
        CREATE OR REPLACE TABLE sim_core.addresses (
            address_id BIGINT PRIMARY KEY,
            line1 VARCHAR,
            line2 VARCHAR,
            city VARCHAR,
            state_province VARCHAR,
            postal_code VARCHAR,
            country_id INTEGER,
            region_id INTEGER,
            latitude DECIMAL(9,6),
            longitude DECIMAL(9,6)
        )""")
    n_pool = ctx.sql("SELECT count(*) FROM sim_core._district_pick").fetchone()[0]
    hd = h(ctx, "core.addresses.district", "g.i")
    hn = h(ctx, "core.addresses.number", "g.i")
    hs = h(ctx, "core.addresses.street", "g.i")
    h2 = h(ctx, "core.addresses.line2", "g.i")
    hp = h(ctx, "core.addresses.postal", "g.i")
    hj = h(ctx, "core.addresses.jitter", "g.i")
    ctx.sql(f"""
        INSERT INTO sim_core.addresses
        SELECT {_util.ADDRESS_LO} - 1 + g.i,
               ({uniform(hn, 10, 8999)})::VARCHAR || ' ' || s.name,
               CASE WHEN ({h2}) % 100 < 22
                    THEN 'Apt ' || ({uniform(h2, 1, 24)})::VARCHAR ||
                         (CASE ({h2}) % 4 WHEN 0 THEN 'A' WHEN 1 THEN 'B'
                                          WHEN 2 THEN 'C' ELSE 'D' END)
                    END,
               p.city, p.state_province,
               p.postal_prefix || lpad((({hp}) % 100)::VARCHAR, 2, '0'),
               p.country_id, p.region_id,
               (p.latitude + (({hj}) % 2001 - 1000) / 100000.0)::DECIMAL(9,6),
               (p.longitude + (({hj} >> 12) % 2001 - 1000) / 100000.0)::DECIMAL(9,6)
        FROM generate_series(1, {_util.ADDRESS_N}) g(i)
        JOIN sim_core._district_pick p ON p.rank = 1 + ({hd}) % {n_pool}
        JOIN sim_core._street s ON s.rank = 1 + ({hs}) % {len(STREETS)}
    """)
    # The storyline address, chapter 02 section 2. Written over a drawn row
    # rather than inserted, so the pool keeps its size at every profile.
    ctx.sql("""
        UPDATE sim_core.addresses SET
            line1 = '88 Rogers Ave', line2 = 'Apt 4C', city = 'Brooklyn',
            state_province = 'NY', postal_code = '11216', country_id = 1,
            region_id = 3312, latitude = 40.671200, longitude = -73.955800
        WHERE address_id = 7712045
    """)


def _exchange_rates(ctx: Context) -> None:
    ctx.sql("""
        CREATE OR REPLACE TABLE sim_core.exchange_rates (
            rate_id BIGINT PRIMARY KEY,
            from_currency VARCHAR,
            to_currency VARCHAR,
            rate_date DATE,
            rate DECIMAL(18,8)
        )""")
    ctx.sql("CREATE OR REPLACE TABLE sim_core._fx_base "
            "(pair_no INTEGER, code VARCHAR, base DOUBLE, phase DOUBLE)")
    ctx.con.executemany("INSERT INTO sim_core._fx_base VALUES (?,?,?,?)",
                        [(i, *row) for i, row in enumerate(FX_BASE, start=1)])

    noise = h(ctx, "core.exchange_rates", "d.ds", "c.code")
    walk = (f"c.base * (1 + 0.035 * sin(2 * pi() * (d.day_index / 365.25 + c.phase))"
            f" + ((({noise}) % 4001) - 2000) / 500000.0)")
    ctx.sql(f"""
        INSERT INTO sim_core.exchange_rates
        WITH r AS (
            SELECT d.ds, d.day_index, c.pair_no, c.code, {walk} AS rate
            FROM _util.days d CROSS JOIN sim_core._fx_base c
        )
        SELECT 400000 + day_index * 20 + pair_no * 2, code, 'USD', ds,
               round(rate, 8)::DECIMAL(18,8) FROM r
        UNION ALL
        SELECT 400000 + day_index * 20 + pair_no * 2 + 1, 'USD', code, ds,
               round(1 / rate, 8)::DECIMAL(18,8) FROM r
    """)
    # The rate chapter 02 samples on the storyline date.
    ctx.sql("UPDATE sim_core.exchange_rates SET rate = 1.27340000 "
            "WHERE from_currency = 'GBP' AND to_currency = 'USD' "
            "AND rate_date = DATE '2026-04-14'")
    ctx.sql("UPDATE sim_core.exchange_rates SET rate = round(1 / 1.27340000, 8) "
            "WHERE from_currency = 'USD' AND to_currency = 'GBP' "
            "AND rate_date = DATE '2026-04-14'")
