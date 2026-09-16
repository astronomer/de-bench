"""`sim_product` — the catalog: hierarchy, brands, SKUs, price lists, prices,
promotions and reviews.

Three properties here are graded, and everything else is furniture that keeps
them honest:

* **The hierarchy is three levels under a root** — department, class,
  subclass — and there are five departments (`HDW`, `BLD`, `GDN`, `OUT`,
  `HOM`). `dept_code` is the grain the retail inventory method values stock at
  from E8, so it is the one level a graded number depends on.
* **Price windows overlap, and priority decides the winner.** Every variant
  carries a standard price for the fiscal year and some carry a clearance
  price whose window sits inside it on a different list. Nothing in the data
  says which wins; the pricing policy does, which is what makes list-against-
  paid a clause question rather than a guess.
* **The sample thread resolves**: variant 550231, `AUR-CUSH-SLT-STD`, product
  4180 `Aurora Deck Cushion`, brand 62 `Aurora`, subclass 318 `Outdoor
  Cushions` in department `OUT`, priced 29.9900 on list 11 from 2026-02-01.

Category ids are laid out so the spec's own sample joins: department `OUT` is
300, its classes are 301..305, and each class owns six subclasses starting at
311 — which puts 318 under class 302, exactly as chapter 02 has it.
"""

from __future__ import annotations

import datetime as dt

from ..config import Context
from .. import streams
from . import _refs

DEPARTMENTS = [
    (400, "HDW", "Hardware", ["Fasteners", "Power Tools", "Hand Tools",
                              "Paint Sundries", "Electrical"]),
    (500, "BLD", "Building Materials", ["Lumber", "Drywall", "Roofing",
                                        "Concrete", "Insulation"]),
    (600, "GDN", "Garden", ["Live Plants", "Soils", "Watering",
                            "Lawn Care", "Planters"]),
    (300, "OUT", "Outdoor Living", ["Patio Furniture", "Outdoor Textiles",
                                    "Grills", "Shade", "Fire Pits"]),
    (700, "HOM", "Home", ["Storage", "Kitchen", "Lighting",
                          "Bath", "Floor Care"]),
]
SUBCLASS_WORDS = ["Cushions", "Covers", "Frames", "Sets", "Accessories", "Spares"]

PRODUCTS = 6_000
VARIANTS = 25_000
PRODUCT_ID_BASE = 4_000        # product ids 4001..10000; the sample is 4180
VARIANT_ID_BASE = 550_000      # variant ids 550001..575000; the sample is 550231
PROMOTIONS = 380
PROMOTION_ID_BASE = 600        # promotion ids 601..980; the sample is 742
REVIEWS_TOTAL = 155_000

SAMPLE_VARIANT = 550_231
SAMPLE_PRODUCT = 4_180
SAMPLE_BRAND = 62
SAMPLE_SUBCLASS = 318
SAMPLE_SUPPLIER = 118          # Lakeshore Outdoor Co, in sim_supply_chain

COLORS = ["Slate", "Sand", "Moss", "Cobalt", "Char", "Ivory", "Clay", "Fern"]
SIZES = ["STD", "LRG", "SML", "XL", "2PK", "4PK"]
TAX_CLASSES = ["home_standard", "hardware_standard", "garden_reduced", "zero_rated"]


def _categories() -> list[tuple]:
    rows = [(100, "All Products", None, 1, None)]
    for dept_id, dept_code, dept_name, classes in DEPARTMENTS:
        rows.append((dept_id, dept_name, 100, 2, dept_code))
        for k, class_name in enumerate(classes, start=1):
            class_id = dept_id + k
            rows.append((class_id, class_name, dept_id, 3, dept_code))
            for j, word in enumerate(SUBCLASS_WORDS):
                subclass_id = dept_id + 11 + 6 * (k - 1) + j
                rows.append((subclass_id, f"{class_name} {word}",
                             class_id, 4, dept_code))
    # The spec's sample subclass, which lands under class 302 by construction.
    return [(cid, "Outdoor Cushions" if cid == SAMPLE_SUBCLASS else name,
             parent, level, dept) for cid, name, parent, level, dept in rows]


def _brands(ctx: Context) -> list[tuple]:
    rows = []
    for brand_id in range(1, 81):
        draw = _refs.py_draw(ctx.seed, "sim_product.brands", brand_id)
        private = draw % 100 < 22
        rows.append((brand_id, f"Brand {brand_id:03d}",
                     101 + draw % 260, private, 1 + draw % 5))
    rows[SAMPLE_BRAND - 1] = (SAMPLE_BRAND, "Aurora", SAMPLE_SUPPLIER, True, 1)
    return rows


def _price_lists(ctx: Context) -> list[tuple]:
    """Twelve standard lists — one per channel per fiscal year — and six
    clearance lists whose windows sit inside them at a different priority.

    The channel order puts the FY2026 web list at id 11, which is the list the
    spec's sample price belongs to.
    """
    years = [y for y in (2024, 2025, 2026)]
    windows = {y: (_refs.fiscal_year_start(y),
                   _refs.fiscal_year_start(y + 1) - dt.timedelta(days=1))
               for y in years}
    order = [(1, "Store", "USD"), (3, "Marketplace", "USD"),
             (2, "Web", "USD"), (4, "Trade", "USD")]
    rows, list_id = [], 0
    for year in years:
        start, end = windows[year]
        for channel_id, label, currency in order:
            list_id += 1
            rows.append((list_id, f"US {label} Standard FY{year}", currency,
                         channel_id, None, start, end, 10))
    clearance = [
        (2, "Clearance", dt.date(2024, 6, 1), dt.date(2024, 9, 15), 20),
        (2, "Holiday Markdown", dt.date(2024, 11, 15), dt.date(2024, 12, 31), 20),
        (2, "Clearance", dt.date(2025, 6, 1), dt.date(2025, 9, 15), 20),
        (2, "Holiday Markdown", dt.date(2025, 11, 15), dt.date(2025, 12, 31), 20),
        (2, "Clearance", dt.date(2026, 3, 15), dt.date(2026, 5, 30), 20),
        (4, "Contract", dt.date(2026, 2, 15), dt.date(2026, 8, 31), 30),
    ]
    for channel_id, label, start, end, priority in clearance:
        list_id += 1
        rows.append((list_id, f"US {label} {start.year}", "USD",
                     channel_id, None, start, end, priority))
    return rows


# The lists a variant is priced on: the web and trade standard list of each
# fiscal year, then the clearance lists a share of variants land on.
STANDARD_LISTS = (3, 4, 7, 8, 11, 12)
CLEARANCE_LISTS = (13, 14, 15, 16, 17, 18)


def build(ctx: Context) -> None:
    ctx.sql("CREATE SCHEMA IF NOT EXISTS sim_product")

    ctx.sql("""
        CREATE OR REPLACE TABLE sim_product.categories (
            category_id INTEGER PRIMARY KEY,
            name VARCHAR NOT NULL,
            parent_category_id INTEGER,
            level INTEGER NOT NULL,
            dept_code VARCHAR
        )
    """)
    ctx.con.executemany("INSERT INTO sim_product.categories VALUES (?,?,?,?,?)",
                        _categories())

    ctx.sql("""
        CREATE OR REPLACE TABLE sim_product.brands (
            brand_id INTEGER PRIMARY KEY,
            name VARCHAR NOT NULL,
            supplier_id INTEGER NOT NULL,
            is_private_label BOOLEAN NOT NULL,
            country_id INTEGER NOT NULL
        )
    """)
    ctx.con.executemany("INSERT INTO sim_product.brands VALUES (?,?,?,?,?)",
                        _brands(ctx))

    # Products. `created_at` never passes the end of the range: a catalog on
    # day D holds what had been created by D and nothing later.
    p = streams.draw(ctx.seed, "'sim_product.products'", "g.i")
    ctx.sql("""
        CREATE OR REPLACE TABLE sim_product.products (
            product_id INTEGER PRIMARY KEY,
            sku_root VARCHAR NOT NULL,
            name VARCHAR NOT NULL,
            description VARCHAR,
            brand_id INTEGER NOT NULL,
            category_id INTEGER NOT NULL,
            tax_class VARCHAR NOT NULL,
            base_uom VARCHAR NOT NULL,
            lifecycle_status VARCHAR NOT NULL,
            is_perishable BOOLEAN NOT NULL,
            created_at TIMESTAMP NOT NULL
        )
    """)
    tax_case = streams.pick(f"({p} >> 20)", [(f"'{t}'", w) for t, w in
                                             zip(TAX_CLASSES, (46, 34, 12, 8))])
    ctx.sql(f"""
        INSERT INTO sim_product.products
        SELECT {PRODUCT_ID_BASE} + g.i,
               c.dept_code || '-' || lpad(g.i::VARCHAR, 4, '0'),
               c.name || ' ' || lpad(g.i::VARCHAR, 4, '0'),
               'Copperline ' || lower(c.name),
               1 + {p} % 80,
               c.category_id,
               {tax_case},
               CASE WHEN ({p} >> 24) % 100 < 6 THEN 'BOX' ELSE 'EA' END,
               CASE WHEN ({p} >> 28) % 100 < 8 THEN 'discontinued'
                    WHEN ({p} >> 28) % 100 < 14 THEN 'new' ELSE 'active' END,
               c.dept_code = 'GDN' AND ({p} >> 32) % 100 < 30,
               (DATE '2013-01-07'
                + (({p} >> 36) % {(ctx.end - dt.date(2013, 1, 7)).days})::INTEGER
                )::TIMESTAMP + INTERVAL 1 MINUTE * ((({p} >> 40) % 600 + 480)::INTEGER)
        FROM generate_series(1, {PRODUCTS}) AS g(i)
        JOIN (SELECT category_id, name, dept_code,
                     row_number() OVER (ORDER BY category_id) - 1 AS n,
                     count(*) OVER () AS total
              FROM sim_product.categories WHERE level = 4) c
          ON c.n = {p} % c.total
    """)
    ctx.sql(f"""
        UPDATE sim_product.products SET
            sku_root = 'AUR-CUSH', name = 'Aurora Deck Cushion',
            description = 'Weatherproof deck seat cushion',
            brand_id = {SAMPLE_BRAND}, category_id = {SAMPLE_SUBCLASS},
            tax_class = 'home_standard', base_uom = 'EA',
            lifecycle_status = 'active', is_perishable = false,
            created_at = TIMESTAMP '2024-02-11 09:14:00'
        WHERE product_id = {SAMPLE_PRODUCT}
    """)

    # Variants. Every transactional table in the world joins here, so the
    # sample SKU is pinned onto the sample product after the draw.
    v = streams.draw(ctx.seed, "'sim_product.product_variants'", "g.i")
    ctx.sql("""
        CREATE OR REPLACE TABLE sim_product.product_variants (
            variant_id BIGINT PRIMARY KEY,
            product_id INTEGER NOT NULL,
            sku VARCHAR NOT NULL UNIQUE,
            barcode_ean VARCHAR NOT NULL,
            size VARCHAR NOT NULL,
            color VARCHAR,
            weight_g INTEGER NOT NULL,
            volume_ml INTEGER,
            status VARCHAR NOT NULL
        )
    """)
    colors = ", ".join(f"'{c}'" for c in COLORS)
    sizes = ", ".join(f"'{s}'" for s in SIZES)
    ctx.sql(f"""
        INSERT INTO sim_product.product_variants
        SELECT {VARIANT_ID_BASE} + g.i,
               p.product_id,
               p.sku_root || '-' || upper(left([{colors}][1 + {v} % {len(COLORS)}], 3))
                          || '-' || [{sizes}][1 + ({v} >> 8) % {len(SIZES)}]
                          || '-' || lpad(g.i::VARCHAR, 5, '0'),
               (4006381000000 + {v} % 999999)::VARCHAR,
               [{sizes}][1 + ({v} >> 8) % {len(SIZES)}],
               [{colors}][1 + {v} % {len(COLORS)}],
               120 + ({v} >> 16) % 24000,
               CASE WHEN ({v} >> 24) % 100 < 18 THEN 250 + ({v} >> 28) % 4000 END,
               CASE WHEN p.lifecycle_status = 'discontinued' THEN 'inactive'
                    ELSE 'active' END
        FROM generate_series(1, {VARIANTS}) AS g(i)
        JOIN sim_product.products p
          ON p.product_id = {PRODUCT_ID_BASE} + 1 + (g.i - 1) % {PRODUCTS}
    """)
    ctx.sql(f"""
        UPDATE sim_product.product_variants SET
            product_id = {SAMPLE_PRODUCT}, sku = 'AUR-CUSH-SLT-STD',
            barcode_ean = '4006381233118', size = 'STD', color = 'Slate',
            weight_g = 1800, volume_ml = NULL, status = 'active'
        WHERE variant_id = {SAMPLE_VARIANT}
    """)

    ctx.sql("""
        CREATE OR REPLACE TABLE sim_product.price_lists (
            price_list_id INTEGER PRIMARY KEY,
            name VARCHAR NOT NULL,
            currency_code VARCHAR NOT NULL,
            channel_id INTEGER NOT NULL,
            region_id INTEGER,
            valid_from DATE NOT NULL,
            valid_to DATE,
            priority INTEGER NOT NULL
        )
    """)
    ctx.con.executemany("INSERT INTO sim_product.price_lists VALUES (?,?,?,?,?,?,?,?)",
                        _price_lists(ctx))

    # Prices. A variant carries the standard price of each fiscal year on the
    # web and trade lists, and about one in seven also carries a clearance
    # price whose window sits inside the standard one at a higher priority.
    # The current fiscal year's rows stay open-ended, which is what makes
    # "the price today" a window question rather than a max().
    pr = streams.draw(ctx.seed, "'sim_product.prices'", "pv.variant_id", "l.price_list_id")
    ctx.sql("""
        CREATE OR REPLACE TABLE sim_product.prices (
            price_id BIGINT PRIMARY KEY,
            price_list_id INTEGER NOT NULL,
            variant_id BIGINT NOT NULL,
            unit_price DECIMAL(18,4) NOT NULL,
            min_qty INTEGER NOT NULL,
            valid_from DATE NOT NULL,
            valid_to DATE
        )
    """)
    ctx.sql(f"""
        INSERT INTO sim_product.prices
        SELECT 880000 + row_number() OVER (ORDER BY pv.variant_id, l.price_list_id),
               l.price_list_id,
               pv.variant_id,
               round((4.99 + ({pr} % 49500) / 100.0)
                     * CASE WHEN l.priority > 10 THEN 0.75 ELSE 1.00 END,
                     2)::DECIMAL(18,4),
               CASE WHEN l.channel_id = 4 THEN 6 ELSE 1 END,
               l.valid_from,
               CASE WHEN l.valid_to >= DATE '{ctx.end}' AND l.priority = 10
                    THEN NULL ELSE l.valid_to END
        FROM sim_product.product_variants pv
        JOIN sim_product.price_lists l
          ON l.price_list_id IN {STANDARD_LISTS}
          OR (l.price_list_id IN {CLEARANCE_LISTS}
              AND ({pr} % 7 = 0 OR pv.variant_id = {SAMPLE_VARIANT}))
    """)
    ctx.sql(f"""
        UPDATE sim_product.prices
        SET unit_price = 29.9900, min_qty = 1,
            valid_from = DATE '2026-02-01', valid_to = NULL
        WHERE variant_id = {SAMPLE_VARIANT} AND price_list_id = 11
    """)

    # Promotions and the SKUs they were eligible on — the denominator any
    # incremental-lift maths needs, against what actually got discounted.
    pm = streams.draw(ctx.seed, "'sim_product.promotions'", "g.i")
    ctx.sql("""
        CREATE OR REPLACE TABLE sim_product.promotions (
            promotion_id INTEGER PRIMARY KEY,
            name VARCHAR NOT NULL,
            mechanic VARCHAR NOT NULL,
            discount_pct DECIMAL(6,4),
            discount_amount DECIMAL(18,4),
            channel_id INTEGER NOT NULL,
            start_date DATE NOT NULL,
            end_date DATE NOT NULL,
            budget_amount DECIMAL(18,4) NOT NULL,
            owner_employee_id INTEGER NOT NULL
        )
    """)
    span = (ctx.end - ctx.start).days - 30
    ctx.sql(f"""
        INSERT INTO sim_product.promotions
        SELECT {PROMOTION_ID_BASE} + g.i,
               'Promotion ' || g.i,
               CASE WHEN {pm} % 100 < 62 THEN 'pct_off'
                    WHEN {pm} % 100 < 88 THEN 'amount_off' ELSE 'bogo' END,
               CASE WHEN {pm} % 100 < 62
                    THEN round((5 + ({pm} >> 8) % 36) / 100.0, 4)::DECIMAL(6,4) END,
               CASE WHEN {pm} % 100 >= 62 AND {pm} % 100 < 88
                    THEN round((200 + ({pm} >> 8) % 4800) / 100.0, 4)::DECIMAL(18,4) END,
               1 + ({pm} >> 16) % 4,
               DATE '{ctx.start}' + (({pm} >> 20) % {span})::INTEGER,
               DATE '{ctx.start}' + ((({pm} >> 20) % {span})
                                     + 7 + ({pm} >> 32) % 45)::INTEGER,
               (5000 + ({pm} >> 36) % 90000)::DECIMAL(18,4),
               3000 + ({pm} >> 44) % 900
        FROM generate_series(1, {PROMOTIONS}) AS g(i)
    """)
    ctx.sql("""
        UPDATE sim_product.promotions SET
            name = 'Spring Basics 25%', mechanic = 'pct_off',
            discount_pct = 0.2500, discount_amount = NULL, channel_id = 2,
            start_date = DATE '2026-04-01', end_date = DATE '2026-04-30',
            budget_amount = 45000.0000, owner_employee_id = 3391
        WHERE promotion_id = 742
    """)

    pp = streams.draw(ctx.seed, "'sim_product.promotion_products'",
                      "p.promotion_id", "g.i")
    ctx.sql("""
        CREATE OR REPLACE TABLE sim_product.promotion_products (
            promotion_id INTEGER NOT NULL,
            variant_id BIGINT NOT NULL,
            min_qty INTEGER NOT NULL,
            reward_qty INTEGER,
            PRIMARY KEY (promotion_id, variant_id)
        )
    """)
    ctx.sql(f"""
        INSERT INTO sim_product.promotion_products
        SELECT promotion_id, variant_id, min_qty, reward_qty FROM (
            SELECT p.promotion_id,
                   {VARIANT_ID_BASE} + 1 + {pp} % {VARIANTS} AS variant_id,
                   1 + ({pp} >> 20) % 2 AS min_qty,
                   CASE WHEN p.mechanic = 'bogo' THEN 1 END AS reward_qty,
                   row_number() OVER (PARTITION BY p.promotion_id,
                       {VARIANT_ID_BASE} + 1 + {pp} % {VARIANTS} ORDER BY g.i) AS dedupe
            FROM sim_product.promotions p,
                 LATERAL generate_series(1, 40) AS g(i)
        ) WHERE dedupe = 1
    """)
    ctx.sql(f"""
        INSERT INTO sim_product.promotion_products
        SELECT 742, {SAMPLE_VARIANT}, 1, NULL
        WHERE NOT EXISTS (SELECT 1 FROM sim_product.promotion_products
                          WHERE promotion_id = 742 AND variant_id = {SAMPLE_VARIANT})
    """)

    # Reviews arrive on the day they were written, so the stream is per day.
    rv = streams.draw(ctx.seed, "'sim_product.product_reviews'", "d.ds", "g.i")
    ctx.sql("""
        CREATE OR REPLACE TABLE sim_product.product_reviews (
            review_id BIGINT PRIMARY KEY,
            variant_id BIGINT NOT NULL,
            customer_id BIGINT,
            order_id BIGINT,
            rating INTEGER NOT NULL,
            title VARCHAR,
            body VARCHAR,
            is_verified_purchase BOOLEAN NOT NULL,
            created_at TIMESTAMP NOT NULL
        )
    """)
    ctx.sql(f"""
        INSERT INTO sim_product.product_reviews
        SELECT 1200000 + row_number() OVER (ORDER BY d.ds, g.i),
               {VARIANT_ID_BASE} + 1 + {rv} % {VARIANTS},
               90000 + ({rv} >> 20) % 40000,
               8000000 + ({rv} >> 28) % 900000,
               1 + ({rv} >> 40) % 5,
               'Review ' || g.i,
               'Written after purchase.',
               ({rv} >> 44) % 100 < 71,
               {streams.minute_time(f"({rv} >> 48)", "d.ds")}
        FROM ({_refs.day_series(ctx)}) d,
             LATERAL generate_series(1, {_refs.per_day(ctx, REVIEWS_TOTAL)}) AS g(i)
    """)
    ctx.sql(f"""
        UPDATE sim_product.product_reviews SET
            variant_id = {SAMPLE_VARIANT}, customer_id = 90412, order_id = 8840127,
            rating = 2, title = 'Smaller than listed',
            body = 'A size under what the listing says.',
            is_verified_purchase = true,
            created_at = TIMESTAMP '2026-04-22 18:41:00'
        WHERE review_id = (SELECT min(review_id) FROM sim_product.product_reviews
                           WHERE created_at::DATE = DATE '2026-04-22')
    """)
