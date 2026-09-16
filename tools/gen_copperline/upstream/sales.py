"""`sim_sales` — the order spine, spec chapter 02 section 8.

Eight tables: orders and their lines, the discount attribution, coupons,
tender and its methods, returns and returned lines. Everything is drawn in
SQL over `generate_series`, one block of keys per day, so regenerating a day
never moves another one.

Four properties here are the ones tasks grade, so they are built in rather
than left to chance.

**The header discount never reaches a line.** An order that carries
`order_discount_amount` has `line_discount_amount = 0` on every one of its
lines, and the attribution rows in `order_line_discounts` say which promotion
took the money without moving it. `subtotal_amount` is the sum of
`line_total` on every order, so a line-grain sum is right for subtotal and
wrong for revenue, which is the trap the allocation clause exists to settle.

**Guest checkout leaves `customer_id` NULL** on about 15% of orders. Trade
orders always name an account, so the draw runs at 16% over the other three
channels.

**Returned quantities are positive.** `return_lines.qty` counts units coming
back; nothing in the row says to subtract it.

**Volume follows the day, not the table.** `volumes.orders_per_day_base` with
the weekday shape, the annual growth and the peak-day factors from
timeline.yaml decide how many orders each channel writes on each date.

Cross-schema reads are `sim_product`, `sim_store` and `sim_customer`, so this
module runs after all three. Where one has not been built, `_util.dep`
substitutes a stand-in over the agreed id ranges and the module still runs.
"""

from __future__ import annotations

from ..config import Context
from ..streams import minute_time, pick, uniform
from . import _util, core
from ._util import h, money

# The channel mix lives in _util.CHANNEL_SHARE: the day-channel volumes are
# computed once, in _util.ensure_days, so sales and freight cannot disagree.
CHANNEL_SHARE = _util.CHANNEL_SHARE

# Which market an order can come from, by date: US and CA until E4, the
# three European markets from 2025-04-07, the second wave from the start of
# FY2026. The values are country ids, written as SQL text because that is
# what streams.pick puts into its CASE arms.
MARKETS_EARLY = [("1", 88), ("2", 12)]
MARKETS_E4 = [("1", 62), ("2", 9), ("3", 13), ("4", 5), ("5", 11)]
MARKETS_WAVE2 = [("1", 58), ("2", 8), ("3", 12), ("4", 5), ("5", 10),
                 ("6", 3), ("7", 2), ("8", 1), ("9", 1)]

E4_CURRENCY_CUTOVER = "2025-04-07"
WAVE2_LAUNCH = "2026-02-01"

# One tender row per method. `provider` names the system that will settle
# it; the card rows read Meridian Pay because the processor switch is behind
# world-today.
PAYMENT_METHODS = [
    (1, "Cash", "cash", "Copperline Retail", 0, 0.0000),
    (2, "Copperline Store Card", "card", "Meridian Pay", 2, 0.0180),
    (3, "Mastercard Credit", "card", "Meridian Pay", 2, 0.0215),
    (4, "Visa Credit", "card", "Meridian Pay", 2, 0.0229),
    (5, "Visa Debit", "card", "Meridian Pay", 1, 0.0125),
    (6, "American Express", "card", "Meridian Pay", 3, 0.0295),
    (7, "PayPal", "wallet", "Meridian Pay", 2, 0.0249),
    (8, "Apple Pay", "wallet", "Meridian Pay", 2, 0.0215),
    (9, "Marketplace Settlement", "marketplace", "Copperline Marketplace", 14, 0.1200),
    (10, "Trade Terms Net 30", "invoice", "Copperline Trade", 30, 0.0000),
]

# How an order's status reads by its age against world-today.
# `cancelled` keeps that spelling: the general ledger skips exactly
# that string when it posts revenue.
FRESH_STATUS = [("'pending'", 25), ("'processing'", 45), ("'shipped'", 30)]
RECENT_STATUS = [("'processing'", 10), ("'shipped'", 55),
                 ("'delivered'", 30), ("'cancelled'", 5)]
SETTLED_STATUS = [("'delivered'", 62), ("'completed'", 30), ("'cancelled'", 8)]

RETURN_RATE_PERMILLE = {"store": 35, "web": 80, "marketplace": 60, "trade": 15}

REASON_CODES = [("'wrong_size'", 26), ("'not_as_described'", 19),
                ("'damaged'", 14), ("'changed_mind'", 22),
                ("'late_delivery'", 8), ("'faulty'", 11)]


def build(ctx: Context) -> None:
    _util.ensure_days(ctx)
    ctx.sql("CREATE SCHEMA IF NOT EXISTS sim_sales")
    _payment_methods(ctx)
    _pools(ctx)
    _coupons(ctx)
    _orders_base(ctx)
    _order_lines(ctx)
    _orders(ctx)
    _order_line_discounts(ctx)
    _payments(ctx)
    _returns(ctx)
    _storyline(ctx)


# --- reference and pick pools ---------------------------------------------

def _payment_methods(ctx: Context) -> None:
    ctx.sql("""
        CREATE OR REPLACE TABLE sim_sales.payment_methods (
            payment_method_id INTEGER PRIMARY KEY,
            name VARCHAR,
            method_type VARCHAR,
            provider VARCHAR,
            settlement_days INTEGER,
            fee_pct DECIMAL(6,4)
        )""")
    ctx.con.executemany(
        "INSERT INTO sim_sales.payment_methods VALUES (?,?,?,?,?,?)",
        PAYMENT_METHODS)


def _pools(ctx: Context) -> None:
    """The per-day pick pools every draw joins against."""
    variants = _util.dep(ctx, "sim_product", "product_variants",
                         _util.standalone_variants())
    products = _util.dep(ctx, "sim_product", "products", _util.standalone_products())
    stores = _util.dep(ctx, "sim_store", "stores", _util.standalone_stores(ctx.start))
    registers = _util.dep(ctx, "sim_store", "registers", _util.standalone_registers())
    customers = _util.dep(ctx, "sim_customer", "customers",
                          _util.standalone_customers(ctx.start))
    price_lists = _util.dep(ctx, "sim_product", "price_lists",
                            _util.standalone_price_lists(ctx.start, ctx.end))
    prices = _util.dep(ctx, "sim_product", "prices", _util.standalone_prices())
    promotions = _util.dep(ctx, "sim_product", "promotions",
                           _util.standalone_promotions(ctx.start))

    ctx.sql(f"""
        CREATE OR REPLACE TABLE sim_sales._variant AS
        SELECT row_number() OVER (ORDER BY v.variant_id) AS rank,
               v.variant_id, coalesce(p.tax_class, 'standard') AS tax_class
        FROM {variants} v
        LEFT JOIN {products} p ON p.product_id = v.product_id
        WHERE v.status = 'active'
    """)
    ctx.sql(f"""
        CREATE OR REPLACE TABLE sim_sales._cust AS
        SELECT row_number() OVER (ORDER BY signup_date, customer_id) AS rank,
               customer_id, signup_date, primary_address_id
        FROM {customers}
        WHERE customer_id NOT BETWEEN {_util.TRADE_LO}
                                  AND {_util.TRADE_LO + _util.TRADE_N - 1}
    """)
    ctx.sql("""
        CREATE OR REPLACE TABLE sim_sales._cust_eligible AS
        WITH s AS (SELECT signup_date, count(*) AS c FROM sim_sales._cust GROUP BY 1)
        SELECT d.ds, coalesce(sum(s.c), 0)::BIGINT AS n
        FROM _util.days d LEFT JOIN s ON s.signup_date <= d.ds
        GROUP BY d.ds
    """)
    # A store under remodel sells nothing: its version row says so, and POS
    # ships no file for it, so the spine has to agree.
    versions = ("LEFT JOIN sim_store.store_versions v"
                " ON v.store_id = s.store_id AND v.valid_from <= d.ds"
                " AND (v.valid_to IS NULL OR v.valid_to >= d.ds)"
                " AND v.status = 'remodel'"
                if _util.has_table(ctx, "sim_store", "store_versions") else
                "LEFT JOIN (SELECT NULL AS store_id) v ON v.store_id = s.store_id")
    ctx.sql(f"""
        CREATE OR REPLACE TABLE sim_sales._store_day AS
        SELECT d.ds,
               row_number() OVER (PARTITION BY d.ds ORDER BY s.store_id) AS rank,
               s.store_id, s.region_id
        FROM _util.days d
        JOIN {stores} s ON s.open_date <= d.ds
                       AND (s.close_date IS NULL OR s.close_date > d.ds)
        {versions}
        WHERE v.store_id IS NULL
    """)
    ctx.sql("CREATE OR REPLACE TABLE sim_sales._store_day_n AS "
            "SELECT ds, count(*)::BIGINT AS n FROM sim_sales._store_day GROUP BY ds")
    ctx.sql("""
        CREATE OR REPLACE TABLE sim_sales._store_country AS
        SELECT s.store_id, coalesce(r.country_id, 1) AS country_id
        FROM (SELECT DISTINCT store_id, region_id FROM sim_sales._store_day) s
        LEFT JOIN sim_core.regions r ON r.region_id = s.region_id
    """)
    ctx.sql(f"""
        CREATE OR REPLACE TABLE sim_sales._register AS
        SELECT store_id,
               row_number() OVER (PARTITION BY store_id ORDER BY register_id) AS rank,
               register_id
        FROM {registers}
    """)
    ctx.sql("CREATE OR REPLACE TABLE sim_sales._register_n AS "
            "SELECT store_id, count(*)::BIGINT AS n FROM sim_sales._register "
            "GROUP BY store_id")
    ctx.sql(f"""
        CREATE OR REPLACE TABLE sim_sales._price_list_day AS
        SELECT d.ds, p.channel_id,
               arg_max(p.price_list_id, p.priority * 1000000 + p.price_list_id)
                   AS price_list_id
        FROM _util.days d
        JOIN {price_lists} p ON p.valid_from <= d.ds
                            AND (p.valid_to IS NULL OR p.valid_to >= d.ds)
        GROUP BY d.ds, p.channel_id
    """)
    ctx.sql(f"""
        CREATE OR REPLACE TABLE sim_sales._list_price AS
        SELECT price_list_id, variant_id,
               arg_max(unit_price, epoch(valid_from::TIMESTAMP)::BIGINT * 1000000
                                   + price_id)::DOUBLE AS unit_price
        FROM {prices} WHERE min_qty <= 1 GROUP BY price_list_id, variant_id
    """)
    ctx.sql("""
        CREATE OR REPLACE TABLE sim_sales._tax_pick AS
        SELECT country_id, tax_class, valid_from,
               coalesce(valid_to, DATE '9999-12-31') AS valid_to,
               tax_rate_id, rate_pct::DOUBLE AS rate_pct
        FROM sim_core.tax_rates WHERE region_id IS NULL
    """)
    ctx.sql(f"""
        CREATE OR REPLACE TABLE sim_sales._promo_day AS
        SELECT d.ds, p.channel_id,
               row_number() OVER (PARTITION BY d.ds, p.channel_id
                                  ORDER BY p.promotion_id) AS rank,
               p.promotion_id
        FROM _util.days d
        JOIN {promotions} p ON p.start_date <= d.ds AND p.end_date >= d.ds
    """)
    ctx.sql("CREATE OR REPLACE TABLE sim_sales._promo_day_n AS "
            "SELECT ds, channel_id, count(*)::BIGINT AS n "
            "FROM sim_sales._promo_day GROUP BY ds, channel_id")


def _coupons(ctx: Context) -> None:
    promotions = _util.dep(ctx, "sim_product", "promotions",
                           _util.standalone_promotions(ctx.start))
    ctx.sql("""
        CREATE OR REPLACE TABLE sim_sales.coupons (
            coupon_id INTEGER PRIMARY KEY,
            coupon_code VARCHAR UNIQUE,
            promotion_id INTEGER,
            max_redemptions INTEGER,
            per_customer_limit INTEGER,
            valid_from DATE,
            valid_to DATE,
            status VARCHAR
        )""")
    hc = h(ctx, "sales.coupons", "p.promotion_id")
    ctx.sql(f"""
        INSERT INTO sim_sales.coupons
        WITH p AS (
            SELECT DISTINCT promotion_id, start_date, end_date FROM {promotions}
        ), c AS (
            SELECT p.*, {_util.COUPON_LO} - 1
                   + row_number() OVER (ORDER BY promotion_id) AS coupon_id,
                   {hc} AS hc
            FROM p WHERE ({hc}) % 100 < 40 AND promotion_id <> 742
        )
        SELECT coupon_id, 'CPL' || promotion_id::VARCHAR, promotion_id,
               {uniform("hc", 1, 20)} * 5000,
               CASE WHEN (hc >> 8) % 100 < 70 THEN 1 ELSE 3 END,
               start_date, end_date,
               CASE WHEN end_date >= DATE '{ctx.today}' THEN 'active'
                    ELSE 'expired' END
        FROM c
    """)
    ctx.sql("""
        INSERT INTO sim_sales.coupons VALUES
        (3018, 'SPRING25', 742, 50000, 1, DATE '2026-04-01', DATE '2026-04-30',
         'expired')
    """)
    ctx.sql("""
        CREATE OR REPLACE TABLE sim_sales._promo_coupon AS
        SELECT promotion_id, min(coupon_id) AS coupon_id
        FROM sim_sales.coupons GROUP BY promotion_id
    """)


# --- orders ---------------------------------------------------------------

def _orders_base(ctx: Context) -> None:
    """Everything about an order that does not depend on its lines."""
    n_variants = ctx.sql("SELECT count(*) FROM sim_sales._variant").fetchone()[0]
    if not n_variants:
        raise ValueError("no active product variants to sell")

    def draw(stream: str, alias: str) -> str:
        return h(ctx, f"sales.orders.{stream}", f"{alias}.ds", f"{alias}.slot")

    def country(alias: str) -> str:
        d = draw("country", alias)
        return (
            f"CASE WHEN {alias}.channel_type = 'store' "
            f"     THEN coalesce({alias}.store_country_id, 1) "
            f"WHEN {alias}.ds < DATE '{E4_CURRENCY_CUTOVER}' THEN {pick(d, MARKETS_EARLY)} "
            f"WHEN {alias}.ds < DATE '{WAVE2_LAUNCH}' THEN {pick(d, MARKETS_E4)} "
            f"ELSE {pick(d, MARKETS_WAVE2)} END"
        )

    def status(alias: str) -> str:
        d = draw("status", alias)
        return (
            f"CASE WHEN DATE '{ctx.today}' - {alias}.ds <= 2 THEN "
            f"{pick(d, FRESH_STATUS)} "
            f"WHEN DATE '{ctx.today}' - {alias}.ds <= 6 THEN {pick(d, RECENT_STATUS)} "
            f"ELSE {pick(d, SETTLED_STATUS)} END"
        )

    def n_lines(alias: str) -> str:
        d = draw("lines", alias)
        return (f"CASE WHEN {alias}.channel_type = 'trade' THEN {uniform(d, 3, 12)} "
                f"ELSE {pick(d, [('1', 35), ('2', 25), ('3', 16), ('4', 11), ('5', 8), ('6', 5)])} END")

    def discount_pct(alias: str) -> str:
        d = draw("discount", alias)
        return (f"CASE WHEN ({d}) % 1000 < 220 THEN "
                f"{pick(f'(({d}) >> 10)', [('0.05', 25), ('0.10', 30), ('0.15', 20), ('0.20', 15), ('0.25', 10)])} "
                f"ELSE 0.0 END")

    def shipping(alias: str) -> str:
        d = draw("shipping", alias)
        return (f"CASE WHEN {alias}.channel_type IN ('web', 'marketplace') THEN "
                f"{pick(d, [('0.00', 40), ('4.99', 35), ('7.99', 25)])} "
                f"WHEN {alias}.channel_type = 'trade' THEN "
                f"{pick(d, [('0.00', 70), ('24.99', 30)])} ELSE 0.00 END")

    addr = draw("address", "r")
    pool_addr = f"{_util.ADDRESS_LO} + ({addr}) % {_util.ADDRESS_N}"
    ctx.sql(f"""
        CREATE OR REPLACE TABLE sim_sales._orders_base AS
        WITH gen AS (
            SELECT dc.ds, dc.day_index, dc.channel_id, dc.channel_type,
                   dc.offset_in_day,
                   unnest(generate_series(1, dc.orders_target)) AS i
            FROM _util.day_channels dc
        ), k AS (
            SELECT g.*, g.offset_in_day + g.i AS slot,
                   g.day_index * {_util.DAY_BLOCK} + g.offset_in_day + g.i AS seq
            FROM gen g
        ), n AS (
            -- How many stores and how many customers this day can pick from.
            -- The pool sizes have to be columns of the row before the rank is
            -- computed: a join key that reads another joined table cannot hash,
            -- and the plan falls back to comparing every pair.
            SELECT k.*, greatest(coalesce(sdn.n, 1), 1) AS store_n,
                   coalesce(ce.n, 0) AS cust_n
            FROM k
            LEFT JOIN sim_sales._store_day_n sdn ON sdn.ds = k.ds
            LEFT JOIN sim_sales._cust_eligible ce ON ce.ds = k.ds
        ), j AS (
            SELECT n.*,
                   CASE WHEN n.channel_type = 'store' THEN sd.store_id END AS store_id,
                   sc.country_id AS store_country_id,
                   c.customer_id AS pool_customer_id,
                   c.primary_address_id AS pool_address_id,
                   pl.price_list_id
            FROM n
            LEFT JOIN sim_sales._store_day sd
                   ON sd.ds = n.ds
                  AND sd.rank = 1 + ({draw("store", "n")}) % n.store_n
            LEFT JOIN sim_sales._store_country sc ON sc.store_id = sd.store_id
            LEFT JOIN sim_sales._cust c
                   ON c.rank = 1 + ({draw("customer", "n")}) % greatest(n.cust_n, 1)
            LEFT JOIN sim_sales._price_list_day pl
                   ON pl.ds = n.ds AND pl.channel_id = n.channel_id
        ), rn AS (
            SELECT j.*, greatest(coalesce(x.n, 1), 1) AS register_n
            FROM j LEFT JOIN sim_sales._register_n x ON x.store_id = j.store_id
        ), r AS (
            SELECT rn.*, rg.register_id
            FROM rn
            LEFT JOIN sim_sales._register rg
                   ON rg.store_id = rn.store_id
                  AND rg.rank = 1 + ({draw("register", "rn")}) % rn.register_n
        )
        SELECT {_util.ORDER_LO} + r.seq AS order_id,
               r.ds, r.day_index, r.slot, r.seq, r.channel_id, r.channel_type,
               (CASE r.channel_type WHEN 'store' THEN 'S' WHEN 'web' THEN 'W'
                                    WHEN 'marketplace' THEN 'M' ELSE 'T' END)
                   || '-' || year(r.ds)::VARCHAR || '-'
                   || ({_util.ORDER_LO} + r.seq)::VARCHAR AS order_number,
               CASE WHEN r.channel_type = 'trade'
                    THEN {_util.TRADE_LO} + ({draw("customer", "r")}) % {_util.TRADE_N}
                    WHEN ({draw("guest", "r")}) % 10000 < 1600 THEN NULL
                    WHEN r.cust_n = 0 THEN NULL
                    ELSE r.pool_customer_id END AS customer_id,
               r.store_id,
               CASE WHEN r.store_id IS NOT NULL THEN r.register_id END AS register_id,
               CASE WHEN r.store_id IS NOT NULL
                    THEN {_util.EMPLOYEE_LO} + ({draw("cashier", "r")}) % {_util.EMPLOYEE_N}
                    END AS cashier_employee_id,
               r.price_list_id,
               {minute_time(draw("time", "r"), "r.ds")} AS order_datetime,
               {status("r")} AS order_status,
               ({country("r")})::INTEGER AS country_id,
               ({n_lines("r")})::INTEGER AS n_lines,
               ({discount_pct("r")})::DOUBLE AS order_discount_pct,
               {money(shipping("r"))} AS shipping_amount,
               coalesce(r.pool_address_id, {pool_addr})::BIGINT AS billing_address_id,
               CASE WHEN ({addr}) % 100 < 85
                    THEN coalesce(r.pool_address_id, {pool_addr})
                    ELSE {_util.ADDRESS_LO} + (({addr}) >> 20) % {_util.ADDRESS_N}
                    END::BIGINT AS shipping_address_id
        FROM r
    """)


def _currency_case(column: str) -> str:
    """The currency a country's market bills in, which is not always the
    country's own: CA bills USD and PL bills EUR, the rows raw.market_config
    carries and a reader disbelieves."""
    arms = " ".join(f"WHEN {cid} THEN '{code}'"
                    for cid, code in sorted(core.BILLING_CURRENCY.items()))
    return f"(CASE {column} {arms} ELSE 'USD' END)"


def _order_lines(ctx: Context) -> None:
    n_variants = ctx.sql("SELECT count(*) FROM sim_sales._variant").fetchone()[0]
    max_lines = ctx.sql("SELECT max(n_lines) FROM sim_sales._orders_base").fetchone()[0]
    if max_lines and max_lines > 15:
        raise ValueError(f"an order wants {max_lines} lines; the key block holds 15")

    def draw(stream: str, alias: str) -> str:
        return h(ctx, f"sales.order_lines.{stream}",
                 f"{alias}.ds", f"{alias}.slot", f"{alias}.line_no")

    def per_variant(stream: str, alias: str) -> str:
        return f"(hash({ctx.seed}, '{stream}', {alias}.variant_id) >> 1)::BIGINT"

    qty = (f"CASE WHEN g.channel_type = 'trade' THEN {uniform(draw('qty', 'g'), 1, 30)} "
           f"ELSE {pick(draw('qty', 'g'), [('1', 60), ('2', 25), ('3', 10), ('4', 3), ('5', 2)])} END")
    override = (f"CASE WHEN ({draw('override', 'g')}) % 100 < 8 "
                f"THEN 0.85 + (({draw('override', 'g')}) >> 8) % 14 / 100.0 ELSE 1.0 END")
    line_pct = pick(f"(({draw('discount', 'v')}) >> 12)",
                    [('0.05', 34), ('0.10', 31), ('0.15', 21), ('0.20', 14)])

    ctx.sql("""
        CREATE OR REPLACE TABLE sim_sales.order_lines (
            order_line_id BIGINT,
            order_id BIGINT,
            line_no INTEGER,
            variant_id BIGINT,
            qty DECIMAL(12,3),
            unit_price DECIMAL(18,4),
            unit_cost DECIMAL(18,4),
            line_discount_amount DECIMAL(18,4),
            tax_rate_id INTEGER,
            tax_amount DECIMAL(18,4),
            line_total DECIMAL(18,4),
            fulfillment_id BIGINT,
            line_status VARCHAR
        )""")
    ctx.sql(f"""
        INSERT INTO sim_sales.order_lines
        WITH g AS (
            SELECT o.order_id, o.seq, o.ds, o.slot, o.channel_type, o.country_id,
                   o.order_status, o.order_discount_pct, o.price_list_id,
                   unnest(generate_series(1, o.n_lines)) AS line_no
            FROM sim_sales._orders_base o
        ), v AS (
            SELECT g.*, vv.variant_id, vv.tax_class,
                   ({qty})::DOUBLE AS qty,
                   round(coalesce(lp.unit_price,
                                  4.99 + {per_variant('variant.price', 'vv')} % 24001 / 100.0)
                         * ({override}), 2) AS unit_price
            FROM g
            JOIN sim_sales._variant vv ON vv.rank = 1 + ({draw('variant', 'g')}) % {n_variants}
            LEFT JOIN sim_sales._list_price lp
                   ON lp.variant_id = vv.variant_id
                  AND lp.price_list_id = g.price_list_id
        ), d AS (
            SELECT v.*,
                   CASE WHEN v.order_discount_pct > 0 THEN 0.0
                        WHEN ({draw('discount', 'v')}) % 1000 < 180
                        THEN round(v.qty * v.unit_price * ({line_pct}), 2)
                        ELSE 0.0 END AS line_discount_amount
            FROM v
        ), t AS (
            SELECT d.*,
                   round(d.qty * d.unit_price - d.line_discount_amount, 2) AS line_total,
                   tp.tax_rate_id, coalesce(tp.rate_pct, 0.0) AS rate_pct
            FROM d
            LEFT JOIN sim_sales._tax_pick tp
                   ON tp.country_id = d.country_id AND tp.tax_class = d.tax_class
                  AND d.ds BETWEEN tp.valid_from AND tp.valid_to
        )
        SELECT {_util.ORDER_LINE_LO} + t.seq * 16 + t.line_no,
               t.order_id, t.line_no, t.variant_id,
               t.qty::DECIMAL(12,3),
               t.unit_price::DECIMAL(18,4),
               {money(f"t.unit_price * (0.38 + {per_variant('variant.cost', 't')} % 25 / 100.0)")},
               t.line_discount_amount::DECIMAL(18,4),
               t.tax_rate_id,
               {money("t.line_total * (1 - t.order_discount_pct) * t.rate_pct")},
               t.line_total::DECIMAL(18,4),
               CASE WHEN t.channel_type <> 'store'
                     AND t.order_status IN ('shipped', 'delivered', 'completed')
                    THEN {_util.FULFILLMENT_LO} + t.seq END,
               t.order_status
        FROM t
    """)


def _orders(ctx: Context) -> None:
    ctx.sql("""
        CREATE OR REPLACE TABLE sim_sales.orders (
            order_id BIGINT PRIMARY KEY,
            order_number VARCHAR UNIQUE,
            customer_id BIGINT,
            channel_id INTEGER,
            store_id INTEGER,
            register_id INTEGER,
            cashier_employee_id INTEGER,
            price_list_id INTEGER,
            order_datetime TIMESTAMP,
            order_status VARCHAR,
            currency_code VARCHAR,
            subtotal_amount DECIMAL(18,4),
            order_discount_amount DECIMAL(18,4),
            tax_amount DECIMAL(18,4),
            shipping_amount DECIMAL(18,4),
            grand_total DECIMAL(18,4),
            billing_address_id BIGINT,
            shipping_address_id BIGINT
        )""")
    currency = (f"CASE WHEN o.ds < DATE '{E4_CURRENCY_CUTOVER}' THEN 'USD' "
                f"ELSE {_currency_case('o.country_id')} END")
    ctx.sql(f"""
        INSERT INTO sim_sales.orders
        WITH a AS (
            SELECT order_id, sum(line_total) AS subtotal, sum(tax_amount) AS tax
            FROM sim_sales.order_lines GROUP BY order_id
        )
        SELECT o.order_id, o.order_number, o.customer_id, o.channel_id, o.store_id,
               o.register_id, o.cashier_employee_id, o.price_list_id,
               o.order_datetime, o.order_status, {currency},
               a.subtotal::DECIMAL(18,4),
               {money("a.subtotal * o.order_discount_pct")},
               a.tax::DECIMAL(18,4), o.shipping_amount,
               (a.subtotal - {money("a.subtotal * o.order_discount_pct")}
                + a.tax + o.shipping_amount)::DECIMAL(18,4),
               o.billing_address_id, o.shipping_address_id
        FROM sim_sales._orders_base o JOIN a ON a.order_id = o.order_id
    """)


def _order_line_discounts(ctx: Context) -> None:
    """Attribution, not money. A header discount is spread across the order's
    lines here while `line_discount_amount` stays zero on every one of them —
    which is the whole reason a line-grain sum cannot answer for revenue."""
    ctx.sql("""
        CREATE OR REPLACE TABLE sim_sales.order_line_discounts (
            order_line_discount_id BIGINT PRIMARY KEY,
            order_line_id BIGINT,
            promotion_id INTEGER,
            coupon_id INTEGER,
            discount_amount DECIMAL(18,4)
        )""")
    hpromo = h(ctx, "sales.order_line_discounts.promotion", "n.order_line_id")
    hcoup = h(ctx, "sales.order_line_discounts.coupon", "j.order_line_id")
    ctx.sql(f"""
        INSERT INTO sim_sales.order_line_discounts
        WITH hdr AS (
            SELECT ol.order_line_id, ol.order_id, ol.line_no, ol.line_total,
                   o.order_discount_amount AS pot,
                   sum(ol.line_total) OVER (PARTITION BY ol.order_id) AS subtotal,
                   row_number() OVER (PARTITION BY ol.order_id
                                      ORDER BY ol.line_no DESC) AS rn_desc
            FROM sim_sales.order_lines ol
            JOIN sim_sales.orders o ON o.order_id = ol.order_id
            WHERE o.order_discount_amount > 0
        ), alloc AS (
            SELECT *, round(pot * line_total / nullif(subtotal, 0), 2) AS share FROM hdr
        ), spread AS (
            SELECT order_line_id, order_id,
                   CASE WHEN rn_desc = 1
                        THEN pot - (sum(share) OVER (PARTITION BY order_id) - share)
                        ELSE share END AS amount
            FROM alloc
        ), b AS (
            SELECT s.order_line_id, s.order_id, s.amount FROM spread s WHERE s.amount > 0
            UNION ALL
            SELECT ol.order_line_id, ol.order_id, ol.line_discount_amount
            FROM sim_sales.order_lines ol WHERE ol.line_discount_amount > 0
        ), n AS (
            SELECT b.*, o.ds, o.channel_id, greatest(coalesce(pn.n, 1), 1) AS promo_n
            FROM b
            JOIN sim_sales._orders_base o ON o.order_id = b.order_id
            LEFT JOIN sim_sales._promo_day_n pn
                   ON pn.ds = o.ds AND pn.channel_id = o.channel_id
        ), j AS (
            SELECT n.*, pd.promotion_id
            FROM n
            LEFT JOIN sim_sales._promo_day pd
                   ON pd.ds = n.ds AND pd.channel_id = n.channel_id
                  AND pd.rank = 1 + ({hpromo}) % n.promo_n
        )
        SELECT {_util.ORDER_LINE_DISCOUNT_LO}
                   + (j.order_line_id - {_util.ORDER_LINE_LO}) * 2 AS order_line_discount_id,
               j.order_line_id, j.promotion_id,
               CASE WHEN ({hcoup}) % 100 < 35 THEN pc.coupon_id END,
               j.amount::DECIMAL(18,4)
        FROM j LEFT JOIN sim_sales._promo_coupon pc ON pc.promotion_id = j.promotion_id
    """)


def _payments(ctx: Context) -> None:
    ctx.sql("""
        CREATE OR REPLACE TABLE sim_sales.payments (
            payment_id BIGINT PRIMARY KEY,
            order_id BIGINT,
            payment_method_id INTEGER,
            amount DECIMAL(18,4),
            currency_code VARCHAR,
            status VARCHAR,
            authorized_at TIMESTAMP,
            captured_at TIMESTAMP,
            gateway_reference VARCHAR
        )""")
    hsplit = h(ctx, "sales.payments.split", "b.ds", "b.slot")
    hshare = h(ctx, "sales.payments.share", "b.ds", "b.slot")
    hmethod = h(ctx, "sales.payments.method", "b.ds", "b.slot", "b.k")
    hsec = h(ctx, "sales.payments.second", "b.ds", "b.slot", "b.k")
    hgw = h(ctx, "sales.payments.gateway", "m.ds", "m.slot", "m.k")
    hfail = h(ctx, "sales.payments.failed", "b.ds", "b.slot")

    method = (
        "CASE WHEN b.channel_type = 'marketplace' THEN 9 "
        "WHEN b.channel_type = 'trade' THEN 10 "
        f"WHEN b.channel_type = 'store' THEN {pick(hmethod, [('1', 22), ('2', 12), ('3', 24), ('4', 28), ('5', 14)])} "
        f"ELSE {pick(hmethod, [('3', 24), ('4', 30), ('5', 12), ('6', 8), ('7', 16), ('8', 10)])} END"
    )
    ctx.sql(f"""
        INSERT INTO sim_sales.payments
        WITH o AS (
            SELECT b.order_id, b.ds, b.slot, b.seq, b.channel_type, b.order_status,
                   x.grand_total, x.currency_code, x.order_datetime,
                   CASE WHEN ({hsplit}) % 100 < 12 THEN 2 ELSE 1 END AS n_pay,
                   0.30 + ({hshare}) % 41 / 100.0 AS first_share,
                   CASE WHEN ({hfail}) % 1000 < 30 THEN 1 ELSE 0 END AS has_failed
            FROM sim_sales._orders_base b
            JOIN sim_sales.orders x ON x.order_id = b.order_id
            WHERE x.grand_total > 0
        ), gen AS (
            SELECT o.*, unnest(generate_series(1, o.n_pay + o.has_failed)) AS k FROM o
        ), b AS (
            SELECT gen.*,
                   CASE WHEN k > n_pay THEN grand_total
                        WHEN n_pay = 1 THEN grand_total
                        WHEN k = 1 THEN round(grand_total * first_share, 2)
                        ELSE grand_total - round(grand_total * first_share, 2)
                   END AS amount
            FROM gen
        ), m AS (
            SELECT b.*, ({method}) AS payment_method_id,
                   b.order_datetime + INTERVAL 1 SECOND * (({hsec}) % 240) AS authorized_at
            FROM b
        )
        SELECT {_util.PAYMENT_LO} + m.seq * 4 + m.k, m.order_id, m.payment_method_id,
               m.amount::DECIMAL(18,4), m.currency_code,
               CASE WHEN m.k > m.n_pay THEN 'failed'
                    WHEN m.order_status = 'cancelled' THEN 'refunded'
                    WHEN date_trunc('day', m.authorized_at) + INTERVAL 1 DAY
                         > TIMESTAMP '{ctx.today} 00:00:00' THEN 'authorized'
                    ELSE 'captured' END,
               m.authorized_at,
               CASE WHEN m.k <= m.n_pay
                     AND m.order_status <> 'cancelled'
                     AND date_trunc('day', m.authorized_at) + INTERVAL 1 DAY
                         <= TIMESTAMP '{ctx.today} 00:00:00'
                    THEN date_trunc('day', m.authorized_at) + INTERVAL 1 DAY
                         + INTERVAL 3 HOUR + INTERVAL 10 MINUTE
                         + INTERVAL 1 MINUTE * (({hgw}) % 50) END,
               -- 48 bits, not 32: the reference is the only key the
               -- processor seam shares, and 4M draws from 2^32 collide 873
               -- times (a full-profile-only birthday bug the payments
               -- extract hit as a duplicate key). From 2^48 the expected
               -- collisions are ~0.06 per world.
               CASE WHEN m.ds < DATE '2025-07-01' THEN 'hp_' ELSE 'mp_' END
               || lpad(to_hex(({hgw}) % 281474976710656), 12, '0')
        FROM m
    """)


def _returns(ctx: Context) -> None:
    ctx.sql("""
        CREATE OR REPLACE TABLE sim_sales.returns (
            return_id BIGINT PRIMARY KEY,
            rma_number VARCHAR UNIQUE,
            order_id BIGINT,
            customer_id BIGINT,
            store_id INTEGER,
            requested_at TIMESTAMP,
            approved_by_employee_id INTEGER,
            reason_code VARCHAR,
            return_status VARCHAR,
            refund_amount DECIMAL(18,4),
            refund_payment_id BIGINT
        )""")
    ctx.sql("""
        CREATE OR REPLACE TABLE sim_sales.return_lines (
            return_line_id BIGINT PRIMARY KEY,
            return_id BIGINT,
            order_line_id BIGINT,
            variant_id BIGINT,
            qty DECIMAL(12,3),
            item_condition VARCHAR,
            restock_flag BOOLEAN,
            restock_warehouse_id INTEGER,
            refund_amount DECIMAL(18,4)
        )""")

    hret = h(ctx, "sales.returns.taken", "o.ds", "o.slot")
    hlag = h(ctx, "sales.returns.lag", "o.ds", "o.slot")
    htime = h(ctx, "sales.returns.time", "o.ds", "o.slot")
    hline = h(ctx, "sales.returns.line", "p.ds", "p.slot")
    hqty = h(ctx, "sales.return_lines.qty", "r.ds", "r.slot")
    hcond = h(ctx, "sales.return_lines.condition", "q.ds", "q.slot")
    hwh = h(ctx, "sales.return_lines.warehouse", "q.ds", "q.slot")
    hstore = h(ctx, "sales.returns.store", "n.ds", "n.slot")
    hemp = h(ctx, "sales.returns.employee", "s.ds", "s.slot")
    hreason = h(ctx, "sales.returns.reason", "s.ds", "s.slot")
    hstatus = h(ctx, "sales.returns.status", "s.ds", "s.slot")

    rate = " ".join(f"WHEN o.channel_type = '{c}' THEN {p}"
                    for c, p in RETURN_RATE_PERMILLE.items())
    lag = (f"CASE WHEN ({hlag}) % 100 < 80 THEN {uniform(f'({hlag} >> 8)', 3, 21)} "
           f"ELSE {uniform(f'({hlag} >> 8)', 22, 60)} END")

    ctx.sql(f"""
        CREATE OR REPLACE TABLE sim_sales._return_pick AS
        SELECT o.order_id, o.ds, o.slot, o.seq, o.customer_id, o.store_id,
               o.channel_type, o.n_lines, o.order_discount_pct,
               (o.ds + INTERVAL 1 DAY * ({lag}))::DATE AS req_date,
               {htime} AS ht
        FROM sim_sales._orders_base o
        JOIN sim_sales.orders x ON x.order_id = o.order_id
        WHERE x.order_status IN ('shipped', 'delivered', 'completed')
          AND ({hret}) % 1000 < (CASE {rate} ELSE 30 END)
          AND (o.ds + INTERVAL 1 DAY * ({lag}))::DATE <= DATE '{ctx.end}'
    """)
    ctx.sql(f"""
        INSERT INTO sim_sales.return_lines
        WITH r AS (
            SELECT p.*, ol.order_line_id, ol.variant_id, ol.qty AS line_qty,
                   ol.line_total
            FROM sim_sales._return_pick p
            JOIN sim_sales.order_lines ol
              ON ol.order_id = p.order_id
             AND ol.line_no = 1 + ({hline}) % p.n_lines
        ), q AS (
            SELECT r.*, 1 + ({hqty}) % greatest(least(3, r.line_qty::INTEGER), 1) AS qty_back
            FROM r
        )
        SELECT {_util.RETURN_LINE_LO} + q.seq, {_util.RETURN_LO} + q.seq,
               q.order_line_id, q.variant_id, q.qty_back::DECIMAL(12,3),
               {pick(hcond, [("'new'", 62), ("'opened'", 26), ("'damaged'", 12)])},
               ({hcond}) % 100 < 62,
               CASE WHEN ({hcond}) % 100 < 62
                    THEN 1 + ({hwh}) % {_util.WAREHOUSE_N} END,
               {money("q.qty_back * (q.line_total / q.line_qty) * (1 - q.order_discount_pct)")}
        FROM q
    """)
    ctx.sql(f"""
        INSERT INTO sim_sales.returns
        WITH r AS (
            SELECT p.*, rl.refund_amount
            FROM sim_sales._return_pick p
            JOIN sim_sales.return_lines rl ON rl.return_id = {_util.RETURN_LO} + p.seq
        ), n AS (
            SELECT r.*, greatest(coalesce(sdn.n, 1), 1) AS store_n
            FROM r LEFT JOIN sim_sales._store_day_n sdn ON sdn.ds = r.req_date
        ), s AS (
            SELECT n.*, sd.store_id AS drawn_store_id
            FROM n
            LEFT JOIN sim_sales._store_day sd
                   ON sd.ds = n.req_date AND sd.rank = 1 + ({hstore}) % n.store_n
        )
        SELECT {_util.RETURN_LO} + s.seq,
               'RMA-' || ({_util.RETURN_LO} + s.seq)::VARCHAR,
               s.order_id, s.customer_id,
               CASE WHEN s.store_id IS NOT NULL THEN s.store_id
                    WHEN (s.ht >> 30) % 100 < 40 THEN s.drawn_store_id END,
               s.req_date::TIMESTAMP + INTERVAL 1 SECOND
                   * ((s.ht % 46800 + 28800) - (s.ht % 46800 + 28800) % 60),
               {_util.EMPLOYEE_LO} + ({hemp}) % {_util.EMPLOYEE_N},
               {pick(hreason, REASON_CODES)},
               {pick(hstatus, [("'completed'", 78), ("'approved'", 12), ("'pending'", 6), ("'rejected'", 4)])},
               s.refund_amount,
               {_util.PAYMENT_LO} + s.seq * 4 + 1
        FROM s
    """)


# --- the storyline --------------------------------------------------------

def _storyline(ctx: Context) -> None:
    """The thread chapter 02 samples down the page: Priya Raman buys three
    Aurora deck cushions on the web and sends one back nine days later. These
    are fixed inserts, not draws, and every key sits outside the drawn blocks
    so no volume change can collide with them."""
    ctx.sql("""
        INSERT INTO sim_sales.orders VALUES
        (8840127, 'W-2026-8840127', 90412, 2, NULL, NULL, NULL, 11,
         TIMESTAMP '2026-04-14 19:58:00', 'shipped', 'USD',
         89.9700, 22.4900, 6.0000, 4.9900, 78.4700, 7712045, 7712045)
    """)
    ctx.sql("""
        INSERT INTO sim_sales.order_lines VALUES
        (19338451, 8840127, 1, 550231, 3.000, 29.9900, 11.2000, 0.0000,
         204, 6.0000, 89.9700, 5520118, 'shipped')
    """)
    ctx.sql("""
        INSERT INTO sim_sales.order_line_discounts VALUES
        (7710223, 19338451, 742, 3018, 22.4900)
    """)
    ctx.sql("""
        INSERT INTO sim_sales.payments VALUES
        (5510887, 8840127, 4, 78.4700, 'USD', 'captured',
         TIMESTAMP '2026-04-14 19:58:12', TIMESTAMP '2026-04-15 03:10:00',
         'mp_3PxK92Lb')
    """)
    ctx.sql("""
        INSERT INTO sim_sales.returns VALUES
        (440218, 'RMA-440218', 8840127, 90412, 214,
         TIMESTAMP '2026-04-23 11:20:00', 3391, 'wrong_size', 'completed',
         22.4900, 5510887)
    """)
    ctx.sql("""
        INSERT INTO sim_sales.return_lines VALUES
        (991042, 440218, 19338451, 550231, 1.000, 'new', true, 7, 22.4900)
    """)
