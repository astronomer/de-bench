"""Helpers the upstream modules share, plus the cross-module id contract.

`_util.days` is the day series every fact table draws against: one row per
date in the fixture range, with the weekday and growth factors timeline.yaml
holds. Any module may call `ensure_days`; the first call builds it and the
rest are no-ops. The integrator may hoist that call into build.py — nothing
here depends on which module ran first.

`dep` is what lets a module build on its own. It returns the real sim table
when the module that owns it has already run, and a stand-in over the agreed
id ranges when it has not. The stand-in is deterministic and carries the
spec's column names, so a standalone build produces the same shape as a full
one — different ids, same structure.

The id ranges below are the integration contract. A module that references
another schema's key by value uses these and nothing else.
"""

from __future__ import annotations

import datetime as dt

from .. import streams
from ..config import Context

# Selling channels. Fixed by the integration contract, so no module has to
# read store.channels to know what a channel_id means.
CHANNEL_ID = {"store": 1, "web": 2, "marketplace": 3, "trade": 4}

# core.addresses: the pool this generator writes is the first 250,000 of the
# reserved 7,700,000..7,999,999 block.
ADDRESS_LO = 7_700_001
ADDRESS_N = 250_000

# store.stores: 101..368, of which 325..368 are the ex-Northwave estate.
STORE_LO, STORE_HI = 101, 368

# inventory.warehouses: 1..12.
WAREHOUSE_N = 12

# customer.customers: trade accounts take 70,001..74,000; consumers sit
# outside that block.
TRADE_LO, TRADE_N = 70_001, 4_000

# hr.employees: store and merchandising staff.
EMPLOYEE_LO, EMPLOYEE_N = 3_000, 1_000

# The block each generated key is drawn from. Every one sits clear of the
# sample values chapter 02 pins, so a planted storyline row can never collide
# with a drawn one.
# Orders start above the 8,000,000..8,999,999 band the integration contract
# reserves for planted and sample rows, so a drawn id can never take one.
ORDER_LO = 9_000_000
ORDER_LINE_LO = 1_000_000_000
ORDER_LINE_DISCOUNT_LO = 4_000_000_000
PAYMENT_LO = 3_000_000_000
RETURN_LO = 500_000_000
RETURN_LINE_LO = 600_000_000
FULFILLMENT_LO = 200_000_000
COUPON_LO = 3_200
SESSION_LO = 100_000_000
PAGE_VIEW_LO = 10_000_000_000
CART_LO = 700_000_000
CART_LINE_LO = 800_000_000
SEARCH_LO = 900_000_000

# Keys per day, per table. A day's block is this wide, so day D's rows can
# never reach into day D+1's — which is what makes regenerating one day safe.
DAY_BLOCK = 200_000


def h(ctx: Context, stream: str, *parts: str) -> str:
    """A draw from a named stream. Parts are SQL expressions."""
    return streams.draw(ctx.seed, f"'{stream}'", *parts)


def money(expr: str) -> str:
    """Money as the upstream holds it: two decimals in a DEC(18,4) column."""
    return f"round({expr}, 2)::DECIMAL(18,4)"


def has_table(ctx: Context, schema: str, table: str) -> bool:
    return bool(ctx.sql(
        "SELECT count(*) FROM information_schema.tables "
        f"WHERE table_schema = '{schema}' AND table_name = '{table}'"
    ).fetchone()[0])


def dep(ctx: Context, schema: str, table: str, standalone: str) -> str:
    """The real table if its module has run, else the stand-in."""
    if has_table(ctx, schema, table):
        return f"{schema}.{table}"
    return f"({standalone})"


# Share of orders by channel. Trade is 6% of orders and about a third of
# revenue, because a trade order is many lines of many units.
CHANNEL_SHARE = [(1, "store", 0.55), (2, "web", 0.29),
                 (3, "marketplace", 0.10), (4, "trade", 0.06)]


def ensure_days(ctx: Context) -> None:
    """`_util.days` and `_util.day_channels`, built once per run.

    day_channels is the one place a day's order volume is computed — sales
    mints orders from it and freight derives order references from the day
    totals, so the two can only agree. days carries the total as `orders`
    (with `dn` as the day number) for modules that need the day grain."""
    ctx.sql("CREATE SCHEMA IF NOT EXISTS _util")
    if has_table(ctx, "_util", "days"):
        return
    volumes = ctx.cfg["volumes"]
    shape = list(volumes["weekday_shape"])  # Sunday .. Saturday
    if len(shape) != 7:
        raise ValueError("volumes.weekday_shape must hold seven factors")
    arms = " ".join(f"WHEN {i} THEN {factor}" for i, factor in enumerate(shape))
    growth = 1 + float(volumes["annual_growth_pct"]) / 100
    ctx.sql(f"""
        CREATE TABLE _util._day_shape AS
        SELECT g.ds::DATE                                        AS ds,
               (g.ds::DATE - DATE '{ctx.start}')::INTEGER        AS day_index,
               dayofweek(g.ds)::INTEGER                          AS dow,
               (CASE dayofweek(g.ds) {arms} END)::DOUBLE         AS weekday_factor,
               pow({growth}, (g.ds::DATE - DATE '{ctx.start}') / 365.25)::DOUBLE
                                                                 AS growth_factor
        FROM generate_series(DATE '{ctx.start}', DATE '{ctx.end}', INTERVAL 1 DAY) g(ds)
    """)
    base = ctx.scale(ctx.cfg["volumes"]["orders_per_day_base"])
    shares = ", ".join(f"({cid}, '{ctype}', {share})"
                       for cid, ctype, share in CHANNEL_SHARE)
    spike = spike_case(ctx, "c.channel_type", "d.ds")
    shut = ", ".join(f"DATE '{d}'"
                     for d in volumes.get("all_market_closures") or ())
    shut_case = (f"CASE WHEN c.channel_type = 'store' AND d.ds IN ({shut}) "
                 "THEN 0 ELSE 1 END" if shut else "1")
    ctx.sql(f"""
        CREATE TABLE _util.day_channels AS
        WITH c(channel_id, channel_type, share) AS (VALUES {shares}),
        t AS (
            -- Shut stores sell nothing: on an all-market closure the store
            -- channel is zero, not one, or POS and the order spine disagree
            -- about days no file could exist for.
            SELECT d.ds, d.day_index, c.channel_id, c.channel_type,
                   (greatest(1, round({base} * c.share * d.weekday_factor
                                     * d.growth_factor * {spike}))
                    * {shut_case})::INTEGER
                       AS orders_target
            FROM _util._day_shape d CROSS JOIN c
        )
        SELECT *, coalesce(sum(orders_target) OVER (
                       PARTITION BY ds ORDER BY channel_id
                       ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0)::INTEGER
                   AS offset_in_day
        FROM t
    """)
    ctx.sql("""
        CREATE TABLE _util.days AS
        SELECT d.*, d.day_index AS dn, t.orders
        FROM _util._day_shape d
        JOIN (SELECT ds, sum(orders_target)::INTEGER AS orders
              FROM _util.day_channels GROUP BY ds) t USING (ds)
    """)
    ctx.sql("DROP TABLE _util._day_shape")
    busiest = ctx.sql("SELECT max(orders) FROM _util.days").fetchone()[0]
    if busiest >= DAY_BLOCK:
        raise ValueError(
            f"the busiest day wants {busiest} orders and the per-day key block "
            f"holds {DAY_BLOCK}; widen DAY_BLOCK")


def spike_case(ctx: Context, channel_expr: str, ds_expr: str = "d.ds") -> str:
    """The peak-day multiplier for a channel, from volumes.spikes."""
    arms = []
    for spike in ctx.cfg["volumes"].get("spikes") or []:
        day = dt.date.fromisoformat(str(spike["date"]))
        channels = ", ".join(f"'{c}'" for c in spike["channels"])
        arms.append(
            f"WHEN {ds_expr} = DATE '{day}' AND {channel_expr} IN ({channels}) "
            f"THEN {float(spike['factor'])}"
        )
    if not arms:
        return "1.0"
    return f"(CASE {' '.join(arms)} ELSE 1.0 END)"


# --- stand-ins, used only when the owning module has not run ---------------

def standalone_variants(n: int = 1000) -> str:
    return (
        f"SELECT 550000 + i AS variant_id, 4000 + (i // 12) AS product_id, "
        f"'active' AS status FROM generate_series(1, {n}) g(i)"
    )


def standalone_products() -> str:
    return (
        "SELECT 4000 + i AS product_id, "
        "(CASE i % 5 WHEN 0 THEN 'home_standard' WHEN 1 THEN 'garden_standard' "
        "WHEN 2 THEN 'building_standard' WHEN 3 THEN 'hardware_standard' "
        "ELSE 'standard' END) AS tax_class, "
        "318 AS category_id FROM generate_series(1, 200) g(i)"
    )


def standalone_stores(start: dt.date) -> str:
    return (
        f"SELECT {STORE_LO} + i AS store_id, 3301 + (i % 99) AS region_id, "
        f"DATE '{start}' - INTERVAL 400 DAY AS open_date, "
        f"NULL::DATE AS close_date, 'open' AS status "
        f"FROM generate_series(0, {STORE_HI - STORE_LO}) g(i)"
    )


def standalone_registers() -> str:
    return (
        f"SELECT s.store_id * 10 + r.k AS register_id, s.store_id "
        f"FROM generate_series({STORE_LO}, {STORE_HI}) s(store_id), "
        f"generate_series(1, 4) r(k)"
    )


def standalone_customers(start: dt.date, n: int = 20000) -> str:
    """Consumers below the trade block, plus the 4,000 trade accounts."""
    return (
        f"SELECT 10000 + i AS customer_id, "
        f"DATE '{start}' - INTERVAL 500 DAY + INTERVAL 1 DAY * (i % 900) AS signup_date, "
        f"NULL::BIGINT AS primary_address_id FROM generate_series(1, {n}) g(i) "
        f"UNION ALL SELECT {TRADE_LO} - 1 + i, DATE '{start}' - INTERVAL 900 DAY, "
        f"NULL::BIGINT FROM generate_series(1, {TRADE_N}) t(i)"
    )


def standalone_price_lists(start: dt.date, end: dt.date) -> str:
    return (
        f"SELECT 10 + c.channel_id AS price_list_id, c.channel_id, "
        f"DATE '{start}' - INTERVAL 30 DAY AS valid_from, NULL::DATE AS valid_to, "
        f"10 AS priority FROM generate_series(1, 4) c(channel_id)"
    )


def standalone_prices() -> str:
    return ("SELECT NULL::BIGINT AS price_id, NULL::INTEGER AS price_list_id, "
            "NULL::BIGINT AS variant_id, NULL::DECIMAL(18,4) AS unit_price, "
            "NULL::INTEGER AS min_qty, NULL::DATE AS valid_from WHERE false")


def standalone_promotions(start: dt.date) -> str:
    """A hundred promotions on rolling four-week windows, one per channel."""
    return (
        f"SELECT 700 + i AS promotion_id, 1 + (i % 4) AS channel_id, "
        f"DATE '{start}' + INTERVAL 1 DAY * (i * 9) AS start_date, "
        f"DATE '{start}' + INTERVAL 1 DAY * (i * 9 + 27) AS end_date "
        f"FROM generate_series(0, 99) g(i)"
    )


def standalone_campaigns() -> str:
    return ("SELECT NULL::INTEGER AS campaign_id, NULL::DATE AS start_date, "
            "NULL::DATE AS end_date WHERE false")
