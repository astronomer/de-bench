"""`sim_store` — the estate: channels, stores, registers and period targets.

The store master is the entity table the most graded numbers hang off, so
almost every row here is pinned rather than drawn:

* **268 stores**, ids 101..368. The organic estate is 101..324, opened in id
  order: 101..291 are the **191 stores Copperline held through FY2023**, which
  is the count the pre-history slab multiplies by, and 292..324 are the 33
  openings inside the fixture range. 325..368 are the 44 stores that came with
  Northwave, with `open_date` well before the 2025-02-03 close and
  `acquired_from` set — CMP-4 needs both columns or the clause is not
  derivable from the row.
* **166 of the 268 sit in Americas timezones**, 61.9%. That share is the whole
  shape of E1's error: reading the 23:05 local close stamp as UTC moves those
  stores' batches into the next day and leaves the European ones right.
* **The three comp-store fixtures are fixed rows**: store 309 opens
  2025-04-18 (CMP-1), store 147 closes 2026-03-21 (CMP-2), store 233 is shut
  for remodel 2026-02-03..28 (CMP-3).

The upstream carries no eras: one id scheme, one clock, no local stamps. The
extract applies all of that.

`store_versions` is not in spec chapter 02, and it is the one table here the
chapter does not name. It has to exist somewhere: `raw.stores` ships as an
SCD2 with 268 current rows and 22 prior versions, and a single-row-per-store
master cannot say when a store changed district. Rather than bolt validity
columns onto `stores` — which would make every reader of the master handle
history it does not want — the history lives beside it, at one row per
version, and the master keeps the current values.
"""

from __future__ import annotations

import datetime as dt

from ..config import Context
from . import _refs

FIRST_STORE_OPENED = dt.date(2009, 3, 14)   # Ray Copper's Portland yard
FY2023_START = dt.date(2023, 1, 29)         # the whole slab year must be open

LEGACY_IDS = range(101, 292)                # 191, open through FY2023
NEW_IDS = range(292, 325)                   # 33, opened inside the range
ACQUIRED_IDS = range(325, 369)              # 44, ex-Northwave

# CMP fixture stores, and the sample store the spec reads down the page.
CMP1_OPENED = (309, dt.date(2025, 4, 18))
CMP2_CLOSED = (147, dt.date(2026, 3, 21))
CMP3_REMODEL = (233, dt.date(2026, 2, 3), dt.date(2026, 2, 28))
SAMPLE_STORE = 214

# The estate by country. US and CA are the Americas share the E1 error runs
# on; the acquired estate is US-only, because Northwave was a regional
# competitor. 150 + 16 = 166 of 268 = 61.9%.
COUNTRY_MIX = {"US": 150, "CA": 16, "GB": 45, "IE": 15, "DE": 42}
ZONES = {
    "US": ["America/New_York", "America/New_York", "America/Chicago",
           "America/Denver", "America/Los_Angeles", "America/Phoenix"],
    "CA": ["America/Toronto", "America/Vancouver"],
    "GB": ["Europe/London"],
    "IE": ["Europe/Dublin"],
    "DE": ["Europe/Berlin"],
}
FORMATS = ["express", "neighborhood", "superstore", "trade_counter"]
FORMAT_SQFT = {"express": 4200, "neighborhood": 11500,
               "superstore": 38000, "trade_counter": 6800}

CHANNELS = [
    (1, "Retail Stores", "store", True),
    (2, "Web Store", "web", True),
    (3, "Copperline Marketplace", "marketplace", True),
    (4, "Trade Counter", "trade", True),
]


def _open_dates(ctx: Context) -> dict[int, dt.date]:
    """Opening dates, monotonic in store id for the organic estate.

    101..291 fill 2009-03-14 to FY2023's start, so all 191 are open for every
    day of the slab year. 292..324 fill the fixture range, and one of them is
    the CMP-1 fixture. The acquired estate opened 2011..2023 under Northwave.
    """
    out: dict[int, dt.date] = {}

    span = (FY2023_START - dt.timedelta(days=30) - FIRST_STORE_OPENED).days
    for n, store_id in enumerate(LEGACY_IDS):
        share = span * n // (len(LEGACY_IDS) - 1)
        jitter = _refs.py_draw(ctx.seed, "sim_store.stores.open_date", store_id) % 21
        opened = FIRST_STORE_OPENED + dt.timedelta(days=share + (jitter if n else 0))
        out[store_id] = min(opened, FY2023_START - dt.timedelta(days=1))

    new_span = (ctx.end - dt.timedelta(days=14) - ctx.start).days
    for n, store_id in enumerate(NEW_IDS):
        out[store_id] = ctx.start + dt.timedelta(days=new_span * n // (len(NEW_IDS) - 1))
    out[CMP1_OPENED[0]] = CMP1_OPENED[1]

    for store_id in ACQUIRED_IDS:
        draw = _refs.py_draw(ctx.seed, "sim_store.stores.open_date", store_id)
        out[store_id] = dt.date(2011, 1, 1) + dt.timedelta(days=draw % 4_380)

    # The one date the chapter states outright. It is the single store whose
    # opening does not follow its place in the id order.
    out[SAMPLE_STORE] = dt.date(2019, 3, 15)
    return out


def _countries(ctx: Context) -> dict[int, str]:
    """One country per store, hitting the declared mix exactly. The acquired
    estate takes 44 of the US rows; the rest are handed out in draw order, so
    no store's country depends on any other store's."""
    out = {store_id: "US" for store_id in ACQUIRED_IDS}
    out[SAMPLE_STORE] = "US"          # Flatbush, which chapter 02 reads
    remaining = []
    for iso2, n in COUNTRY_MIX.items():
        remaining += [iso2] * (n - sum(1 for v in out.values() if v == iso2))
    organic = _refs.shuffled(
        ctx.seed, "sim_store.stores.country",
        [s for s in list(LEGACY_IDS) + list(NEW_IDS) if s != SAMPLE_STORE])
    out.update(dict(zip(organic, remaining)))
    return out


def _versions(ctx: Context, stores: dict[int, dict]) -> list[tuple]:
    """268 current versions and 22 prior ones — what `raw.stores` ships as.

    Nine stores changed district, one of them mid-FY2026 (the re-district the
    comp restatement has to follow); ten changed format; and the two CMP
    fixture stores carry their closure and their remodel as spans.
    """
    changers = _refs.shuffled(
        ctx.seed, "sim_store.store_versions",
        [s for s in LEGACY_IDS if s not in (CMP2_CLOSED[0], CMP3_REMODEL[0])],
    )
    districted, reformatted = changers[:9], changers[9:19]

    spans: dict[int, list[tuple[dt.date, int, str, str]]] = {}
    for store_id, row in stores.items():
        spans[store_id] = [(row["open_date"], row["region_id"], row["store_format"], "open")]

    for n, store_id in enumerate(districted):
        # The first is the mid-year re-district; the rest are spread back
        # over the range so "district as of the charge date" is a real join.
        moved = dt.date(2026, 1, 11) if n == 0 else \
            ctx.start + dt.timedelta(days=120 + 70 * n)
        pool = [r for r in stores[store_id]["district_pool"] if r != stores[store_id]["region_id"]]
        old_region = pool[_refs.py_draw(ctx.seed, "store_versions.region", store_id) % len(pool)]
        spans[store_id] = [(stores[store_id]["open_date"], old_region,
                            stores[store_id]["store_format"], "open"),
                           (moved, stores[store_id]["region_id"],
                            stores[store_id]["store_format"], "open")]

    for n, store_id in enumerate(reformatted):
        changed = ctx.start + dt.timedelta(days=200 + 40 * n)
        old = FORMATS[_refs.py_draw(ctx.seed, "store_versions.format", store_id) % len(FORMATS)]
        if old == stores[store_id]["store_format"]:
            old = FORMATS[(FORMATS.index(old) + 1) % len(FORMATS)]
        spans[store_id] = [(stores[store_id]["open_date"], stores[store_id]["region_id"],
                            old, "open"),
                           (changed, stores[store_id]["region_id"],
                            stores[store_id]["store_format"], "open")]

    closed_id, closed_on = CMP2_CLOSED
    spans[closed_id] = [(stores[closed_id]["open_date"], stores[closed_id]["region_id"],
                         stores[closed_id]["store_format"], "open"),
                        (closed_on, stores[closed_id]["region_id"],
                         stores[closed_id]["store_format"], "closed")]

    remodel_id, shut, reopened = CMP3_REMODEL
    spans[remodel_id] = [(stores[remodel_id]["open_date"], stores[remodel_id]["region_id"],
                          stores[remodel_id]["store_format"], "open"),
                         (shut, stores[remodel_id]["region_id"],
                          stores[remodel_id]["store_format"], "remodel"),
                         (reopened + dt.timedelta(days=1), stores[remodel_id]["region_id"],
                          stores[remodel_id]["store_format"], "open")]

    rows = []
    for store_id in sorted(spans):
        versions = spans[store_id]
        for n, (valid_from, region_id, store_format, status) in enumerate(versions):
            last = n == len(versions) - 1
            valid_to = None if last else versions[n + 1][0] - dt.timedelta(days=1)
            rows.append((store_id, n + 1, region_id, store_format, status,
                         valid_from, valid_to, last))
    return rows


def build(ctx: Context) -> None:
    ctx.sql("CREATE SCHEMA IF NOT EXISTS sim_store")

    ctx.sql("""
        CREATE OR REPLACE TABLE sim_store.channels (
            channel_id INTEGER PRIMARY KEY,
            name VARCHAR NOT NULL,
            channel_type VARCHAR NOT NULL,
            is_active BOOLEAN NOT NULL
        )
    """)
    ctx.con.executemany("INSERT INTO sim_store.channels VALUES (?,?,?,?)", CHANNELS)

    countries = _countries(ctx)
    opened = _open_dates(ctx)
    country_ids = _refs.country_ids(ctx)
    districts = _refs.districts_by_country(ctx)
    ids = sorted(list(LEGACY_IDS) + list(NEW_IDS) + list(ACQUIRED_IDS))
    addresses = dict(zip(ids, _refs.address_ids(ctx, "sim_store.stores", len(ids))))

    stores: dict[int, dict] = {}
    for store_id in ids:
        iso2 = countries[store_id]
        country_id = country_ids[iso2]
        pool = districts[country_id]
        zones = ZONES[iso2]
        draw = _refs.py_draw(ctx.seed, "sim_store.stores", store_id)
        store_format = FORMATS[draw % len(FORMATS)]
        if store_id == SAMPLE_STORE:
            store_format = "express"
        stores[store_id] = {
            "store_code": f"S-{store_id:04d}",
            "name": f"Copperline {store_id:04d}",
            "store_format": store_format,
            "region_id": pool[(draw >> 8) % len(pool)],
            "district_pool": pool,
            "address_id": addresses[store_id],
            "manager_employee_id": 2000 + (draw >> 16) % 1100,
            "cost_center_id": 5000 + store_id,     # the formula finance builds to
            "sqft_selling": FORMAT_SQFT[store_format] + (draw >> 24) % 900,
            "timezone": zones[(draw >> 32) % len(zones)],
            "open_date": opened[store_id],
            "close_date": None,
            "status": "open",
            "acquired_from": "northwave" if store_id in ACQUIRED_IDS else None,
        }
    stores[SAMPLE_STORE].update(name="Flatbush Express", sqft_selling=4200,
                                timezone="America/New_York")
    stores[CMP2_CLOSED[0]].update(close_date=CMP2_CLOSED[1], status="closed")

    ctx.sql("""
        CREATE OR REPLACE TABLE sim_store.stores (
            store_id INTEGER PRIMARY KEY,
            store_code VARCHAR NOT NULL,
            name VARCHAR NOT NULL,
            store_format VARCHAR NOT NULL,
            region_id INTEGER NOT NULL,
            address_id BIGINT NOT NULL,
            manager_employee_id INTEGER NOT NULL,
            cost_center_id INTEGER NOT NULL,
            sqft_selling INTEGER NOT NULL,
            timezone VARCHAR NOT NULL,
            open_date DATE NOT NULL,
            close_date DATE,
            status VARCHAR NOT NULL,
            acquired_from VARCHAR
        )
    """)
    ctx.con.executemany(
        "INSERT INTO sim_store.stores VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        [(store_id, r["store_code"], r["name"], r["store_format"], r["region_id"],
          r["address_id"], r["manager_employee_id"], r["cost_center_id"],
          r["sqft_selling"], r["timezone"], r["open_date"], r["close_date"],
          r["status"], r["acquired_from"])
         for store_id, r in sorted(stores.items())],
    )

    ctx.sql("""
        CREATE OR REPLACE TABLE sim_store.store_versions (
            store_id INTEGER NOT NULL,
            version_no INTEGER NOT NULL,
            region_id INTEGER NOT NULL,
            store_format VARCHAR NOT NULL,
            status VARCHAR NOT NULL,
            valid_from DATE NOT NULL,
            valid_to DATE,
            is_current BOOLEAN NOT NULL,
            PRIMARY KEY (store_id, version_no)
        )
    """)
    ctx.con.executemany(
        "INSERT INTO sim_store.store_versions VALUES (?,?,?,?,?,?,?,?)",
        _versions(ctx, stores),
    )

    # Registers: lane count follows the format, and about a third of the
    # lanes are self-checkout. One stream per store, not per row, so adding a
    # lane to one store never moves another's.
    reg = f"(hash({ctx.seed}, 'sim_store.registers', s.store_id, g.i) >> 1)::BIGINT"
    ctx.sql("""
        CREATE OR REPLACE TABLE sim_store.registers (
            register_id INTEGER PRIMARY KEY,
            store_id INTEGER NOT NULL,
            terminal_code VARCHAR NOT NULL,
            register_type VARCHAR NOT NULL,
            status VARCHAR NOT NULL
        )
    """)
    ctx.sql(f"""
        INSERT INTO sim_store.registers
        SELECT s.store_id * 100 + g.i,
               s.store_id,
               'POS-' || lpad(g.i::VARCHAR, 2, '0'),
               CASE WHEN {reg} % 100 < 32 THEN 'self_checkout' ELSE 'staffed' END,
               CASE WHEN s.status = 'closed' THEN 'retired' ELSE 'active' END
        FROM sim_store.stores s,
             LATERAL generate_series(1, CASE s.store_format
                 WHEN 'superstore' THEN 14 WHEN 'neighborhood' THEN 8
                 WHEN 'trade_counter' THEN 4 ELSE 3 END) AS g(i)
    """)

    # Targets are set per store per fiscal period, from the period the store
    # opens in. A store carries no target for a year it did not trade.
    periods = _refs.fiscal_periods(ctx.start.year, ctx.end.year)
    values = ", ".join(f"({pid}, DATE '{start}', DATE '{end}')"
                       for pid, start, end in periods if start <= ctx.end)
    tgt = f"(hash({ctx.seed}, 'sim_store.store_targets', s.store_id, p.period_id) >> 1)::BIGINT"
    ctx.sql("""
        CREATE OR REPLACE TABLE sim_store.store_targets (
            target_id INTEGER PRIMARY KEY,
            store_id INTEGER NOT NULL,
            fiscal_period_id INTEGER NOT NULL,
            revenue_target DECIMAL(18,2) NOT NULL,
            margin_target_pct DECIMAL(6,4) NOT NULL,
            footfall_target INTEGER NOT NULL,
            currency_code VARCHAR NOT NULL
        )
    """)
    ctx.sql(f"""
        INSERT INTO sim_store.store_targets
        SELECT (row_number() OVER (ORDER BY s.store_id, p.period_id))::INTEGER + 33000,
               s.store_id,
               p.period_id,
               (round(s.sqft_selling * (7.4 + ({tgt} % 260) / 100.0), 2))::DECIMAL(18,2),
               (0.3200 + ({tgt} >> 12) % 1400 / 10000.0)::DECIMAL(6,4),
               (s.sqft_selling * 4 + ({tgt} >> 24) % 9000)::INTEGER,
               -- A target is written in the currency the legal entity bills
               -- in, which is why the Canadian stores plan in USD.
               CASE s.timezone WHEN 'Europe/London' THEN 'GBP'
                               WHEN 'Europe/Dublin' THEN 'EUR'
                               WHEN 'Europe/Berlin' THEN 'EUR'
                               ELSE 'USD' END
        FROM sim_store.stores s
        JOIN (SELECT * FROM (VALUES {values}) AS v(period_id, period_start, period_end)) p
          ON p.period_start >= s.open_date
         AND (s.close_date IS NULL OR p.period_start <= s.close_date)
    """)
