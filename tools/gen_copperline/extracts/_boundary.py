"""What the OMS and AR extracts share: the id contract at the boundary, the
two clocks and integer cents.

THE BOUNDARY IS WHERE THE MESS IS APPLIED (spec 03, the six ways). Nothing
here invents a fact; it re-spells one. The sim tables hold decimal money, one
clock and integer keys, and these helpers turn that into the text ids, the
`*_cents` columns and the `event_time_utc` / `loaded_at` pair the landing
layer ships.

## The three key tables, and why they exist

`_util.order_keys`, `_util.customer_keys` and `_util.sku_keys` are built once
per run by `ensure_keys` and dropped with the rest of `_util`. They are the
integration contract between extract modules: every feed that names an order,
a trade account or a SKU joins one of these rather than deriving the string
again. Two feeds deriving the same id twice is how a world gets a join that
almost works.

They carry more than this family needs, on purpose. The POS feed has to write
the same store-local clock (`utc_offset_hours`), the payments feeds have to
write the same `order_ref`, and the PIM change feed has to write the same
`SKU-#####` as `raw.order_lines`. Any extract module may join them.

### The shipped order reference: ORD-######## from (ds, slot), not from the key

Spec 03 section 1 rule 4 fixes `ORD-########` — eight digits, so the widest
value it holds is 99,999,999. The simulated key does not fit: `sim_sales`
mints `order_id = 9,000,000 + day_index * 200,000 + slot`, which passes
99,999,999 on day 455 — 2025-05-04 — and reaches about 181,000,000 by the end
of the range.

**`lpad` does not save it. `lpad` truncates.** In DuckDB, as in Postgres,
`lpad('121600050', 8, '0')` is `'12160005'`: the last digit is dropped. So
`'ORD-' || lpad(order_id, 8, '0')` maps every ten consecutive orders above
99,999,999 onto one reference — 172,132 orders become 94,196 references at
the small profile — and `raw.orders.order_id` cannot be a key at all.

So the shipped reference is derived from the day and the slot instead:

    order_no = (orders on every day before ds) + (this order's rank within ds)
    order_id = 'ORD-' || lpad(order_no, 8, '0')

which is a dense sequence over the whole range in date order — what an OMS
counter looks like — and reaches 3.5M at full scale, well inside the eight
digits. The day's block width comes from the day's own row count, so the
mapping is a pure function of the day series and the seed. The rank inside a
day is by `order_id`, so the storyline rows the sim inserts by hand (key band
8,000,000..8,999,999, below the drawn block) sort first on their day and
still get a reference.

**Any module that names an order joins `_util.order_keys` on the simulated
key and takes `raw_order_id`.** A module that spells the reference itself
writes one of ten orders under the same string and joins the wrong row.

`OE-#######`, the processor-facing reference, is **not** renumbered. It is
`upstream.external.ORDER_REF` — `order_id % 10,000,000` in seven digits —
which is what the sim's own processor tables carry and what
`raw.payment_intents` asserts against. Seven digits cannot hold the key, so
the reference recycles every 50 days of order volume, and that ambiguity is
the arming: a settlement joined to an order on the reference picks one of
about seventeen orders, and the bridge is the only honest link (NLO-4).

## Rounding

Every money column in the sim is already two decimals — `_util.money` rounds
to 2 in a `DECIMAL(18,4)` — so `round(x * 100)` never meets a half-way case
and the cast to `BIGINT` is exact. `round()` on DuckDB decimals rounds half
away from zero, which is half-up for the positive amounts here. Derived
amounts (an allocated share, a partial refund) are rounded to two decimals in
the sim as well, for the same reason.
"""

from __future__ import annotations

import datetime as dt

from .. import streams
from ..config import Context
from ..upstream.external import ORDER_REF
from ._land import OWN_DAYS, window_start, write_hive
from .crm import BILLING_ERA_CURRENT_PCT, LEGACY_KEYED

# --- E2, the customer re-key ----------------------------------------------
# Copperline re-keyed the trade book at E2. `LEGACY_KEYED` accounts existed
# under the old scheme and carry a `CUST####` id, which `extracts.crm` writes
# on `raw.customers.legacy_id` by the same ordinal rule; the rest were always
# in the current numbering. The last 60 of them lost their crosswalk row in
# the migration, so `raw.customer_id_map` carries their legacy id with no
# current id. Those 60 are D7's population — an inner join to the crosswalk
# drops them, and the runbook says to roll them up under the legacy id
# instead, so dropping them is wrong and so is ignoring them.
LEGACY_ORPHANS = 60
LEGACY_MAPPED = LEGACY_KEYED - LEGACY_ORPHANS             # 2,600
LEGACY_ORPHAN_LO = LEGACY_MAPPED + 1                      # trade_seq 2601

# Old-format `customer_ref` written after the cutover, by day offset from it.
# Fourteen days, forty-five rows, authored rather than drawn: spec 03 section
# 6 grades the count exactly.
E2_LATE_QUOTA = (4, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 4, 4)
E2_LATE_ROWS = sum(E2_LATE_QUOTA)

# --- populations spec 03 states as counts, before the profile divisor -----
TEST_ORDERS = 18_000
SOFT_DELETED_ORDERS = 5_200

# The trade book's payment terms, `raw.payment_terms` and the code every
# trade account and invoice carries. (code, description, net days, early
# settlement discount in basis points, the days it is available, weight).
PAYMENT_TERMS = [
    ("NET30", "Net 30 days", 30, 0, 0, 30),
    ("NET45", "Net 45 days", 45, 0, 0, 11),
    ("NET60", "Net 60 days", 60, 0, 0, 7),
    ("NET15", "Net 15 days", 15, 0, 0, 9),
    ("NET90", "Net 90 days", 90, 0, 0, 2),
    ("NET7", "Net 7 days", 7, 0, 0, 3),
    ("2-10-NET30", "2% 10 days, net 30", 30, 200, 10, 8),
    ("1-15-NET45", "1% 15 days, net 45", 45, 100, 15, 4),
    ("3-5-NET30", "3% 5 days, net 30", 30, 300, 5, 2),
    ("EOM30", "Net 30 from end of month", 30, 0, 0, 6),
    ("EOM60", "Net 60 from end of month", 60, 0, 0, 2),
    ("COD", "Cash on delivery", 0, 0, 0, 4),
    ("PREPAY", "Paid before despatch", 0, 0, 0, 3),
    ("PROFORMA", "Proforma, paid on issue", 0, 0, 0, 1),
]

# The legal entity a market's revenue books to (`_common.LEGAL_ENTITIES`).
# Copperline runs five entities and nine markets, so four markets book into a
# neighbour's entity — which is why an entity total is not a market total.
ENTITY_OF_MARKET = {"US": "CL-US", "CA": "CL-US", "GB": "CL-GB", "IE": "CL-IE",
                    "DE": "CL-DE", "MX": "CL-MX", "BR": "CL-US",
                    "PL": "CL-IE", "ID": "CL-US"}

# The day each of those entities began trading. Before it, the market it covers
# had no local entity, and `docs/finance-policy.md`'s entity table puts a
# cross-border market with no local entity on CL-US. A feed that stamps CL-GB
# on a 2024 row is claiming an entity that did not exist, so `entity_at` is
# what every feed carrying an entity code goes through.
ENTITY_FIRST_TRADED = {"CL-US": "2009-03-14", "CL-GB": "2025-04-07",
                       "CL-IE": "2025-04-07", "CL-DE": "2025-04-07",
                       "CL-MX": "2026-02-01"}

# Standard offset from UTC. No DST: the local clock a store wrote is a
# story S3 owns (the 23:05 batch stamp) and the two DST days are FIN-388's
# through `raw.shipment_packages.ship_date`. ICU is not available inside the
# generator, so a fixed offset per zone is what the world can hold.
ZONE_OFFSET = {
    "America/New_York": -5, "America/Chicago": -6, "America/Denver": -7,
    "America/Los_Angeles": -8, "America/Phoenix": -7, "America/Toronto": -5,
    "America/Vancouver": -8, "Europe/London": 0, "Europe/Dublin": 0,
    "Europe/Berlin": 1,
}
# By market, for the rows no store wrote.
MARKET_OFFSET = {"US": -5, "CA": -5, "GB": 0, "IE": 0, "DE": 1,
                 "BR": -3, "MX": -6, "PL": 1, "ID": 7}
MARKET_OF_COUNTRY = {1: "US", 2: "CA", 3: "GB", 4: "IE", 5: "DE",
                     6: "BR", 7: "MX", 8: "PL", 9: "ID"}


def h(ctx: Context, stream: str, *parts: str) -> str:
    """A draw from a named stream. Parts are SQL expressions."""
    return streams.draw(ctx.seed, f"'{stream}'", *parts)


def cents(expr: str) -> str:
    """Decimal money as the integer count of minor units it lands as."""
    return f"round(({expr}) * 100)::BIGINT"


def era_date(ctx: Context, era_id: str, key: str = "at") -> dt.date:
    return dt.date.fromisoformat(str(ctx.era(era_id)[key]))


def minute(expr: str) -> str:
    """Truncate a timestamp expression to the minute. Every clock the world
    ships is minute-grain, so a downstream minute-truncation is a no-op."""
    return f"date_trunc('minute', {expr})"


def loaded_at(ctx: Context, stream: str, event_expr: str, parts: tuple[str, ...],
              tail: str = "default") -> str:
    """When the warehouse saw an event that happened at `event_expr`.

    The lag is drawn from the per-mille tail in timeline.yaml: nothing later
    than day 5, and day 5 always happens. It is a lag in *days crossed*, so
    `datediff('day', event_time_utc, loaded_at)` reads back exactly the drawn
    number — a row is delivered some minutes after the event and then held for
    whole days, and the delivery is clipped to the end of the day it lands on
    rather than being allowed to roll into the next one. That is what makes
    the measured safe lookback a number and not an argument.
    """
    lag = streams.lag_days(h(ctx, f"{stream}.lag", *parts), ctx.cfg["late_tail"][tail])
    at = h(ctx, f"{stream}.at", *parts)
    held = f"INTERVAL 1 DAY * ({lag})"
    return (
        f"least({event_expr} + {held} + INTERVAL 1 MINUTE * (15 + ({at}) % 225),"
        f" date_trunc('day', {event_expr}) + {held}"
        f" + INTERVAL 23 HOUR + INTERVAL 59 MINUTE)"
    )


def _case(column: str, mapping: dict, quote_value: bool = True) -> str:
    arms = []
    for key, value in mapping.items():
        k = f"'{key}'" if isinstance(key, str) else str(key)
        v = f"'{value}'" if quote_value else str(value)
        arms.append(f"WHEN {k} THEN {v}")
    return f"(CASE {column} {' '.join(arms)} END)"


def entity_at(market: str, on: str) -> str:
    """SQL for the legal entity that billed `market` on the date `on`.

    `ENTITY_OF_MARKET` says which entity covers a market today.
    `ENTITY_FIRST_TRADED` says when that entity opened. Before it opened the
    market was a cross-border market with no local entity, and the policy's
    entity table puts those on CL-US. `market` and `on` are SQL expressions.
    """
    arms = []
    for code, entity in ENTITY_OF_MARKET.items():
        first = ENTITY_FIRST_TRADED[entity]
        arms.append(f"WHEN {market} = '{code}' AND {on} < DATE '{first}' "
                    f"THEN 'CL-US'")
    return f"(CASE {' '.join(arms)} ELSE {_case(market, ENTITY_OF_MARKET)} END)"


# --- the key tables --------------------------------------------------------

def ensure_keys(ctx: Context) -> None:
    """Build `_util.order_keys`, `_util.customer_keys` and `_util.sku_keys`.

    Any extract module may call this; the first call builds them and the rest
    are no-ops. They live in `_util`, which the orchestrator drops, so none of
    this reaches the agent.
    """
    from ..upstream._util import ensure_days

    ensure_days(ctx)
    if _has(ctx, "_util", "order_keys"):
        return
    _customer_keys(ctx)
    _sku_keys(ctx)
    _order_keys(ctx)


def ensure_keys_or_stub(ctx: Context) -> None:
    """ensure_keys where the sales sim exists; an empty `_util.order_keys`
    where it does not, so a module built standalone in a test keeps valid
    joins and NULL references. The full build always takes the real path —
    sales is in upstream.ORDER — so a stub can never reach a shipped world."""
    if all(_has(ctx, s, t) for s, t in (("sim_sales", "orders"),
                                        ("sim_customer", "customers"),
                                        ("sim_store", "stores"))):
        ensure_keys(ctx)
        return
    ctx.sql("CREATE SCHEMA IF NOT EXISTS _util")
    if not _has(ctx, "_util", "order_keys"):
        ctx.sql("CREATE TABLE _util.order_keys "
                "(order_id BIGINT, raw_order_id VARCHAR)")


def ensure_fiscal(ctx: Context) -> None:
    """`_util.fiscal_month`: one row per date with the fiscal month it falls
    in, as `FY2026-P03`.

    The 4-5-4 calendar is `calendars.rows`, and this reads that function
    rather than `raw.fiscal_calendar` — an extract reads the sim and its own
    generator code, never another extract's table.
    """
    ctx.sql("CREATE SCHEMA IF NOT EXISTS _util")
    if _has(ctx, "_util", "fiscal_month"):
        return
    from .calendars import rows

    ctx.sql("CREATE TABLE _util.fiscal_month "
            "(cal_date DATE PRIMARY KEY, period_label VARCHAR, "
            " fiscal_year VARCHAR, fiscal_period TINYINT)")
    ctx.con.executemany(
        "INSERT INTO _util.fiscal_month VALUES (?,?,?,?)",
        [(row[0], f"{row[1]}-P{row[3]:02d}", row[1], row[3])
         for row in rows(ctx.cfg)],
    )


def gift_card_source(ctx: Context) -> str:
    """`raw.orders.gift_card_applied_cents` sums the redeem entries of the gift
    card ledger — but an extract never reads another extract's `raw` table, so
    the promos module leaves the per-order total in `_util.gift_card_applied`
    and this returns it. Where promos has not run (a module test that only
    builds S1) it returns an empty stand-in with the same shape, so the column
    lands as zero rather than failing the build."""
    if _has(ctx, "_util", "gift_card_applied"):
        return "_util.gift_card_applied"
    return ("(SELECT NULL::BIGINT AS order_id, NULL::BIGINT AS applied_cents "
            "WHERE false)")


def _has(ctx: Context, schema: str, table: str) -> bool:
    return bool(ctx.sql(
        "SELECT count(*) FROM information_schema.tables "
        f"WHERE table_schema = '{schema}' AND table_name = '{table}'"
    ).fetchone()[0])


def _customer_keys(ctx: Context) -> None:
    """One row per trade account: the two id formats, and which era each one
    belongs to.

    `legacy_ref` is the account's own `CUST####`, NULL for the 1,400 accounts
    that never sat in the legacy CRM. `era_ref_before` is what a row written
    *before* the re-key shows for this account: for a legacy account that is
    its own legacy id, and for a current-numbering account it is the legacy id
    of the account that held that slot in the old book. The old book only ever
    held 2,600 numbers, so a pre-E2 row can only ever show one of them.
    """
    # `billing_era` is a property of the billing contract and `extracts.crm`
    # draws it for `raw.customers`. The same account has to read the same way
    # on its invoices and its plan lines, so this is the same stream and the
    # same threshold, taken from that module rather than restated.
    hb = h(ctx, "crm_billing_era", "c.trade_seq")
    ht = h(ctx, "customers.terms", "c.trade_seq")
    terms = streams.pick(ht, [(f"'{code}'", weight)
                              for code, _, _, _, _, weight in PAYMENT_TERMS])
    ctx.sql(f"""
        CREATE OR REPLACE TABLE _util.customer_keys AS
        SELECT c.customer_id,
               c.trade_seq,
               c.customer_code                                   AS customer_ref,
               CASE WHEN c.trade_seq <= {LEGACY_KEYED}
                    THEN 'CUST' || lpad(c.trade_seq::VARCHAR, 4, '0') END
                                                                 AS legacy_ref,
               'CUST' || lpad((1 + (c.trade_seq - 1) % {LEGACY_KEYED})::VARCHAR,
                              4, '0')                            AS era_ref_before,
               c.trade_seq BETWEEN {LEGACY_ORPHAN_LO} AND {LEGACY_KEYED}
                                                                 AS is_orphan,
               CASE WHEN ({hb}) % 100 < {BILLING_ERA_CURRENT_PCT}
                    THEN 'current' ELSE 'legacy' END             AS billing_era,
               {terms}                                           AS payment_terms_code
        FROM sim_customer.customers c
        WHERE c.trade_seq IS NOT NULL
    """)


def _sku_keys(ctx: Context) -> None:
    """`SKU-#####` per spec 03 section 1 rule 4. The sim's own `sku` is a
    readable merchandising code (`AUR-CUSH-SLT-STD`); the landing layer ships
    the padded form, and this table is where the two meet."""
    ctx.sql("""
        CREATE OR REPLACE TABLE _util.sku_keys AS
        SELECT v.variant_id,
               v.product_id,
               v.sku                                             AS source_sku,
               'SKU-' || lpad((v.variant_id - 550000)::VARCHAR, 5, '0') AS sku
        FROM sim_product.product_variants v
    """)


def _order_keys(ctx: Context) -> None:
    zone = _case("s.timezone", ZONE_OFFSET, quote_value=False)
    market = _case("k.country_id", MARKET_OF_COUNTRY)
    offset = _case("r.market_code", MARKET_OFFSET, quote_value=False)
    channel = _case("left(o.order_number, 1)",
                    {"S": "store", "W": "web", "M": "marketplace", "T": "trade"})
    # Which market an order was taken in lives on `sim_sales._orders_base` and
    # nowhere public: the currency cannot say, because Canada bills in USD and
    # Poland in EUR. That table is private to the sales module, so the join is
    # guarded — where it is gone the currency is the best guess left, and the
    # storyline rows the sim inserts by hand take that path anyway.
    base = ("LEFT JOIN sim_sales._orders_base b ON b.order_id = o.order_id"
            if _has(ctx, "sim_sales", "_orders_base") else
            "LEFT JOIN (SELECT NULL::BIGINT AS order_id, NULL::INTEGER AS country_id"
            " WHERE false) b ON b.order_id = o.order_id")
    ctx.sql(f"""
        CREATE OR REPLACE TABLE _util._order_src AS
        SELECT o.order_id,
               o.order_datetime,
               o.order_datetime::DATE                            AS ds,
               {channel}                                         AS channel,
               o.customer_id, o.store_id, o.currency_code, o.order_status,
               o.subtotal_amount, o.order_discount_amount, o.tax_amount,
               o.shipping_amount, o.grand_total,
               coalesce(b.country_id,
                        CASE o.currency_code WHEN 'GBP' THEN 3 WHEN 'MXN' THEN 7
                                             ELSE 1 END)         AS country_id
        FROM sim_sales.orders o
        {base}
    """)
    ctx.sql("""
        CREATE OR REPLACE TABLE _util._order_day AS
        SELECT ds, count(*)::BIGINT AS n,
               coalesce(sum(count(*)) OVER (ORDER BY ds
                   ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0)::BIGINT
                   AS day_base
        FROM _util._order_src GROUP BY ds
    """)
    ctx.sql(f"""
        CREATE OR REPLACE TABLE _util.order_keys AS
        WITH k AS (
            -- `slot` is the order's rank inside its day. It is what the draws
            -- here key on, so a day's stream is its own, and it is not the
            -- simulation's slot: the storyline rows sit below the drawn key
            -- block and take the first ranks of their day.
            SELECT s.*, d.day_base,
                   row_number() OVER (PARTITION BY s.ds ORDER BY s.order_id) AS slot
            FROM _util._order_src s JOIN _util._order_day d USING (ds)
        ), r AS (
            SELECT k.*, k.day_base + k.slot AS order_no, {market} AS market_code
            FROM k
        )
        SELECT r.order_id, r.ds, r.slot, r.order_no, r.channel, r.market_code,
               r.country_id, r.customer_id, r.store_id, r.currency_code,
               r.order_status,
               -- The sim's timestamp is the wall clock whoever took the order
               -- read, so it is the LOCAL clock and the UTC instant is derived
               -- from it. That keeps the business date inside the fixture
               -- range and puts an evening sale in the Americas on the next
               -- UTC day, which is the whole of the E1 story.
               r.order_datetime                                  AS event_local,
               r.order_datetime
                   - INTERVAL 1 HOUR * coalesce({zone}, {offset}, 0)
                                                                 AS event_utc,
               r.subtotal_amount, r.order_discount_amount, r.tax_amount,
               r.shipping_amount, r.grand_total,
               'ORD-' || lpad(r.order_no::VARCHAR, 8, '0')       AS raw_order_id,
               -- The one id the boundary does not renumber. Every processor
               -- feed spells it this way and its ambiguity is armed: seven
               -- digits cannot hold the key, so it recycles every 50 days.
               {ORDER_REF.format(order_id="r.order_id")}          AS order_ref,
               CASE WHEN r.store_id IS NOT NULL
                    THEN 'S-' || lpad(r.store_id::VARCHAR, 4, '0') END
                                                                 AS store_ref,
               s.acquired_from                                   AS store_acquired_from,
               coalesce({zone}, {offset}, 0)                      AS utc_offset_hours,
               false                                             AS is_test
        FROM r LEFT JOIN sim_store.stores s ON s.store_id = r.store_id
    """)
    ctx.sql("DROP TABLE _util._order_src")
    n_test = ctx.scale(TEST_ORDERS)
    ctx.sql(f"""
        UPDATE _util.order_keys SET is_test = true WHERE order_id IN (
            SELECT order_id FROM _util.order_keys
            QUALIFY row_number() OVER (
                ORDER BY {h(ctx, 'orders.is_test', 'order_id')}) <= {n_test}
        )
    """)
    flagged = ctx.sql(
        "SELECT count(*) FROM _util.order_keys WHERE is_test").fetchone()[0]
    assert flagged == n_test, (flagged, n_test)


# --- the landing tree ------------------------------------------------------

def land(ctx: Context, source: str, name: str, query: str,
         fmt: str = "csv") -> int:
    """Write `query` as one file per day under `landing/<source>/dt=<ds>/`.

    The query selects a `dt` column, which DuckDB partitions on and leaves out
    of the file. `_land.write_hive` does the writing and the renaming; this
    only names the source and the leaf.

    Every source here is one of Copperline's own, so the tree holds the RET-2
    window — 400 live days — and `raw` holds the whole history. A rebuild that
    reaches further back reads the archive, and there is no archive here.
    """
    return write_hive(
        ctx, query, ctx.landing_dir(source),
        "PARQUET" if fmt == "parquet" else "CSV",
        ("dt",), name.rsplit(".", 1)[0],
        options="" if fmt == "parquet" else "HEADER",
    )


def live_from(ctx: Context) -> dt.date:
    """The first day still in the landing tree for one of Copperline's own
    sources: RET-2 keeps 400 days and the archive keeps the rest."""
    return window_start(ctx, OWN_DAYS)
