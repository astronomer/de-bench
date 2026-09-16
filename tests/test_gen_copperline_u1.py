"""The copperline upstream: `sim_core`, `sim_sales` and `sim_ecommerce`.

These three modules simulate the application databases chapter 02 of the
retail-world spec describes. They read `sim_product`, `sim_store` and
`sim_customer`, which other modules build, so the fixtures here stand those
three up as small tables with the spec's own column lists — enough rows to
exercise every join, few enough to reason about.

What is asserted is what a task grades: the volumes, the guest-checkout
share, the header discount staying off the lines, the sample storyline
joining end to end, and the two generator properties the whole world rests
on — the same config gives the same rows, and shortening the range never
moves a day that is still in it.
"""

from __future__ import annotations

import datetime as dt
import sys
from pathlib import Path

import duckdb
import pytest

from de_bench.tasks import repo_root

sys.path.insert(0, str(repo_root() / "tools"))

from gen_copperline import config                       # noqa: E402
from gen_copperline.upstream import core, ecommerce, sales   # noqa: E402

WORLD = repo_root() / "worlds" / "copperline"

# A window that holds the storyline order (2026-04-14) and its return nine
# days later, and is short enough to build in a second.
START = "2026-03-20"
END = "2026-05-04"


# --- the tables other modules own -----------------------------------------

def _stubs(ctx) -> None:
    """`sim_product`, `sim_store` and `sim_customer` at the spec's column
    lists. The rows chapter 02 samples are written in by hand; the rest are
    filler so the pick pools have something to pick."""
    con = ctx.con
    for schema in ("sim_product", "sim_store", "sim_customer"):
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")

    con.execute("""
        CREATE TABLE sim_product.products (
            product_id INTEGER, sku_root VARCHAR, name VARCHAR,
            description VARCHAR, brand_id INTEGER, category_id INTEGER,
            tax_class VARCHAR, base_uom VARCHAR, lifecycle_status VARCHAR,
            is_perishable BOOLEAN, created_at TIMESTAMP)""")
    con.execute("""
        INSERT INTO sim_product.products
        SELECT 4000 + i, 'SKU-' || i::VARCHAR, 'Product ' || i::VARCHAR, '',
               62, 300 + i % 20,
               CASE i % 5 WHEN 0 THEN 'home_standard' WHEN 1 THEN 'garden_standard'
                          WHEN 2 THEN 'building_standard'
                          WHEN 3 THEN 'hardware_standard' ELSE 'standard' END,
               'EA', 'active', false, TIMESTAMP '2024-02-11 09:14:00'
        FROM generate_series(1, 199) g(i)""")
    con.execute("""
        INSERT INTO sim_product.products VALUES
        (4180, 'AUR-CUSH', 'Aurora Deck Cushion', 'Weatherproof deck seat cushion',
         62, 318, 'home_standard', 'EA', 'active', false,
         TIMESTAMP '2024-02-11 09:14:00')""")

    con.execute("""
        CREATE TABLE sim_product.product_variants (
            variant_id BIGINT, product_id INTEGER, sku VARCHAR,
            barcode_ean VARCHAR, size VARCHAR, color VARCHAR, weight_g INTEGER,
            volume_ml INTEGER, status VARCHAR)""")
    con.execute("""
        INSERT INTO sim_product.product_variants
        SELECT 550000 + i, 4000 + i, 'SKU-' || i::VARCHAR || '-STD',
               '400638123' || lpad(i::VARCHAR, 4, '0'), 'STD', 'Slate', 1800,
               NULL, 'active'
        FROM generate_series(1, 199) g(i)""")
    con.execute("""
        INSERT INTO sim_product.product_variants VALUES
        (550231, 4180, 'AUR-CUSH-SLT-STD', '4006381233118', 'STD', 'Slate',
         1800, NULL, 'active')""")

    con.execute("""
        CREATE TABLE sim_product.price_lists (
            price_list_id INTEGER, name VARCHAR, currency_code VARCHAR,
            channel_id INTEGER, region_id INTEGER, valid_from DATE,
            valid_to DATE, priority INTEGER)""")
    con.execute("""
        INSERT INTO sim_product.price_lists VALUES
        (11, 'US Web Standard FY2026', 'USD', 2, 1204, DATE '2024-01-01', NULL, 10),
        (12, 'US Store Standard', 'USD', 1, 1204, DATE '2024-01-01', NULL, 10),
        (13, 'Marketplace Standard', 'USD', 3, 1204, DATE '2024-01-01', NULL, 10),
        (14, 'Trade Standard', 'USD', 4, 1204, DATE '2024-01-01', NULL, 10)""")

    con.execute("""
        CREATE TABLE sim_product.prices (
            price_id BIGINT, price_list_id INTEGER, variant_id BIGINT,
            unit_price DECIMAL(18,4), min_qty INTEGER, valid_from DATE,
            valid_to DATE)""")
    con.execute("""
        INSERT INTO sim_product.prices
        SELECT 880000 + i, 11, 550000 + i, (9.99 + i % 40)::DECIMAL(18,4), 1,
               DATE '2024-01-01', NULL
        FROM generate_series(1, 199) g(i)""")
    con.execute("""
        INSERT INTO sim_product.prices VALUES
        (880431, 11, 550231, 29.9900, 1, DATE '2026-02-01', NULL)""")

    con.execute("""
        CREATE TABLE sim_product.promotions (
            promotion_id INTEGER, name VARCHAR, mechanic VARCHAR,
            discount_pct DECIMAL(6,4), discount_amount DECIMAL(18,4),
            channel_id INTEGER, start_date DATE, end_date DATE,
            budget_amount DECIMAL(18,4), owner_employee_id INTEGER)""")
    con.execute("""
        INSERT INTO sim_product.promotions
        SELECT 700 + i, 'Promotion ' || i::VARCHAR, 'pct_off', 0.1500, NULL,
               1 + i % 4, DATE '2026-03-01' + INTERVAL 1 DAY * (i * 2),
               DATE '2026-03-01' + INTERVAL 1 DAY * (i * 2 + 27),
               45000.0000, 3391
        FROM generate_series(1, 19) g(i)""")
    con.execute("""
        INSERT INTO sim_product.promotions VALUES
        (742, 'Spring Basics 25%', 'pct_off', 0.2500, NULL, 2,
         DATE '2026-04-01', DATE '2026-04-30', 45000.0000, 3391)""")

    con.execute("""
        CREATE TABLE sim_store.stores (
            store_id INTEGER, store_code VARCHAR, name VARCHAR,
            store_format VARCHAR, region_id INTEGER, address_id BIGINT,
            manager_employee_id INTEGER, cost_center_id INTEGER,
            sqft_selling INTEGER, timezone VARCHAR, open_date DATE,
            close_date DATE, status VARCHAR)""")
    con.execute("""
        INSERT INTO sim_store.stores
        SELECT 100 + i, 'ST-' || lpad(i::VARCHAR, 4, '0'), 'Store ' || i::VARCHAR,
               'express', 3301 + i % 99, 7700000 + i, 3391, 5000 + i, 4200,
               'America/New_York', DATE '2019-03-15', NULL, 'open'
        FROM generate_series(1, 199) g(i) WHERE i <> 114""")
    con.execute("""
        INSERT INTO sim_store.stores VALUES
        (214, 'BK-FLAT', 'Flatbush Express', 'express', 3312, 7712900, 3391,
         5214, 4200, 'America/New_York', DATE '2019-03-15', NULL, 'open')""")

    con.execute("""
        CREATE TABLE sim_store.registers (
            register_id INTEGER, store_id INTEGER, terminal_code VARCHAR,
            register_type VARCHAR, status VARCHAR)""")
    con.execute("""
        INSERT INTO sim_store.registers
        SELECT s.store_id * 10 + r.k, s.store_id,
               'POS-' || lpad(r.k::VARCHAR, 2, '0'),
               CASE WHEN r.k = 1 THEN 'self_checkout' ELSE 'staffed' END, 'active'
        FROM sim_store.stores s, generate_series(1, 2) r(k)""")

    con.execute("""
        CREATE TABLE sim_customer.customers (
            customer_id BIGINT, customer_code VARCHAR, first_name VARCHAR,
            last_name VARCHAR, email VARCHAR, phone VARCHAR, birth_date DATE,
            gender VARCHAR, primary_address_id BIGINT, preferred_store_id INTEGER,
            acquisition_channel_id INTEGER, signup_date DATE, status VARCHAR,
            marketing_opt_in BOOLEAN)""")
    con.execute("""
        INSERT INTO sim_customer.customers
        SELECT 10000 + i, 'C-' || lpad((10000 + i)::VARCHAR, 6, '0'), 'First',
               'Last', 'c' || i::VARCHAR || '@example.com', '+1-347-555-0000',
               DATE '1991-06-04', 'F', NULL, 214, 2,
               DATE '2023-11-02' + INTERVAL 1 DAY * (i % 300), 'active', true
        FROM generate_series(1, 149) g(i)""")
    con.execute("""
        INSERT INTO sim_customer.customers
        SELECT 70000 + i, 'C-' || lpad((70000 + i)::VARCHAR, 6, '0'), 'Trade',
               'Account', 't' || i::VARCHAR || '@example.com', NULL, NULL, NULL,
               NULL, NULL, 4, DATE '2022-05-01', 'active', false
        FROM generate_series(1, 50) g(i)""")
    con.execute("""
        INSERT INTO sim_customer.customers VALUES
        (90412, 'C-090412', 'Priya', 'Raman', 'priya.raman@example.com',
         '+1-347-555-0182', DATE '1991-06-04', 'F', 7712045, 214, 2,
         DATE '2023-11-02', 'active', true)""")

    con.execute("""
        CREATE TABLE sim_customer.marketing_campaigns (
            campaign_id INTEGER, name VARCHAR, medium VARCHAR,
            promotion_id INTEGER, start_date DATE, end_date DATE,
            budget_amount DECIMAL(18,2), cost_center_id INTEGER,
            owner_employee_id INTEGER)""")
    con.execute("""
        INSERT INTO sim_customer.marketing_campaigns
        SELECT 1100 + i, 'Campaign ' || i::VARCHAR, 'email', 700 + i,
               DATE '2026-03-01' + INTERVAL 1 DAY * (i * 3),
               DATE '2026-03-15' + INTERVAL 1 DAY * (i * 3), 12000.00, 5290, 3391
        FROM generate_series(1, 19) g(i)""")
    con.execute("""
        INSERT INTO sim_customer.marketing_campaigns VALUES
        (1180, 'Spring Basics Email Blast', 'email', 742, DATE '2026-04-01',
         DATE '2026-04-14', 12000.00, 5290, 3391)""")


def _build(tmp_path: Path, name: str, end: str = END) -> Path:
    """One upstream build at the small profile, into its own database."""
    db = tmp_path / f"{name}.duckdb"
    cfg = config.load_timeline(WORLD / "timeline.yaml")
    # A shorter fact range. `today` stays what the world says it is: it is a
    # property of the company, not of how much history was generated.
    cfg["range"]["start"] = START
    cfg["range"]["end"] = end
    con = duckdb.connect(str(db))
    ctx = config.Context(con=con, cfg=cfg, profile="small", landing=tmp_path / name)
    _stubs(ctx)
    for module in (core, sales, ecommerce):
        module.build(ctx)
    con.close()
    return db


@pytest.fixture(scope="module")
def built(tmp_path_factory) -> Path:
    return _build(tmp_path_factory.mktemp("u1"), "world")


def _one(db: Path, sql: str):
    con = duckdb.connect(str(db), read_only=True)
    try:
        return con.execute(sql).fetchone()
    finally:
        con.close()


def _all(db: Path, sql: str):
    con = duckdb.connect(str(db), read_only=True)
    try:
        return con.execute(sql).fetchall()
    finally:
        con.close()


# --- shape and volume -----------------------------------------------------

def test_every_table_the_spec_names_is_built(built):
    got = {f"{s}.{t}" for s, t in _all(built,
        "SELECT table_schema, table_name FROM information_schema.tables "
        "WHERE table_schema LIKE 'sim\\_%' ESCAPE '\\' "
        "AND table_name NOT LIKE '\\_%' ESCAPE '\\'")}
    assert {
        "sim_core.currencies", "sim_core.countries", "sim_core.exchange_rates",
        "sim_core.regions", "sim_core.addresses", "sim_core.tax_rates",
        "sim_sales.orders", "sim_sales.order_lines",
        "sim_sales.order_line_discounts", "sim_sales.coupons",
        "sim_sales.payment_methods", "sim_sales.payments", "sim_sales.returns",
        "sim_sales.return_lines",
        "sim_ecommerce.web_sessions", "sim_ecommerce.page_views",
        "sim_ecommerce.carts", "sim_ecommerce.cart_lines",
        "sim_ecommerce.search_queries",
    } <= got


def test_reference_populations(built):
    assert _one(built, "SELECT count(*) FROM sim_core.currencies") == (8,)
    assert _one(built, "SELECT count(*) FROM sim_core.countries") == (9,)
    # The address pool never scales: an entity elsewhere names an address by
    # value, so the same ids have to exist at either profile.
    assert _one(built, "SELECT count(*) FROM sim_core.addresses") == (250000,)
    days = (dt.date.fromisoformat(END) - dt.date.fromisoformat(START)).days + 1
    assert _one(built, "SELECT count(*) FROM sim_core.exchange_rates") == (days * 14,)
    # Brooklyn South under the New York metro, the two ids chapter 02 samples.
    assert _one(built, "SELECT name, level, parent_region_id FROM sim_core.regions "
                       "WHERE region_id = 3312") == ("Brooklyn South", "district", 1204)


def test_order_volume_follows_the_day(built):
    days = (dt.date.fromisoformat(END) - dt.date.fromisoformat(START)).days + 1
    n, distinct_days = _one(built,
        "SELECT count(*), count(DISTINCT order_datetime::DATE) FROM sim_sales.orders")
    assert distinct_days == days
    per_day = n / days
    # 3,000 a day at the shipped profile, a twentieth of that here, times the
    # weekday shape and the growth the range has accrued.
    assert 150 < per_day < 280, per_day

    lines, orders = _one(built,
        "SELECT count(*), count(DISTINCT order_id) FROM sim_sales.order_lines")
    assert 2.4 < lines / orders < 3.1, lines / orders

    # Friday is the week's peak at 1.48 and Sunday its floor at 0.86, so the
    # weekday shape shows up as a ratio near 1.7 between them.
    fri, sun = _one(built, """
        SELECT sum(n) FILTER (WHERE dow = 5), sum(n) FILTER (WHERE dow = 0)
        FROM (SELECT dayofweek(order_datetime) AS dow, count(*) AS n
              FROM sim_sales.orders GROUP BY 1, order_datetime::DATE)""")
    assert 1.5 < fri / sun < 1.9, (fri, sun)


def test_the_clickstream_carries_its_own_volume(built):
    sessions, views, searches = _one(built,
        "SELECT (SELECT count(*) FROM sim_ecommerce.web_sessions), "
        "(SELECT count(*) FROM sim_ecommerce.page_views), "
        "(SELECT count(*) FROM sim_ecommerce.search_queries)")
    assert 3.2 < views / sessions < 4.2, views / sessions
    days = (dt.date.fromisoformat(END) - dt.date.fromisoformat(START)).days + 1
    # What the extract collapses and samples: about 3,100 events a day at the
    # shipped profile, which is the baseline the feed-decay incident measures.
    # Growth counts from the range start, so this short window sits at the
    # base rather than a fifth above it; the shipped range lands near 3,100.
    events = (sessions + views + searches) * 20 * 0.25 / days
    assert 2200 < events < 3400, events


# --- the properties tasks grade -------------------------------------------

def test_guest_checkout_is_about_fifteen_percent(built):
    share, = _one(built, "SELECT count(*) FILTER (WHERE customer_id IS NULL)"
                         "::DOUBLE / count(*) FROM sim_sales.orders")
    assert 0.13 < share < 0.17, share
    # A trade order always names its account.
    assert _one(built, """
        SELECT count(*) FROM sim_sales.orders o
        JOIN sim_sales._orders_base b ON b.order_id = o.order_id
        WHERE b.channel_type = 'trade' AND o.customer_id IS NULL""") == (0,)


def test_the_header_discount_never_reaches_a_line(built):
    with_header, = _one(built, "SELECT count(*) FROM sim_sales.orders "
                               "WHERE order_discount_amount > 0")
    assert with_header > 0
    # No line of a header-discounted order carries a discount of its own.
    assert _one(built, """
        SELECT count(*) FROM sim_sales.orders o
        JOIN sim_sales.order_lines l ON l.order_id = o.order_id
        WHERE o.order_discount_amount > 0 AND l.line_discount_amount > 0""") == (0,)
    # The subtotal is the line sum, so the header discount is nowhere in it.
    assert _one(built, """
        SELECT count(*) FROM (
            SELECT o.order_id FROM sim_sales.orders o
            JOIN sim_sales.order_lines l ON l.order_id = o.order_id
            GROUP BY o.order_id, o.subtotal_amount
            HAVING abs(o.subtotal_amount - sum(l.line_total)) > 0.0001)""") == (0,)
    assert _one(built, """
        SELECT count(*) FROM sim_sales.orders
        WHERE abs(grand_total - (subtotal_amount - order_discount_amount
                                 + tax_amount + shipping_amount)) > 0.0001""") == (0,)
    # The attribution rows say where the header discount went without moving
    # it: they add back up to the header amount.
    assert _one(built, """
        SELECT count(*) FROM (
            SELECT o.order_id FROM sim_sales.orders o
            JOIN sim_sales.order_lines l ON l.order_id = o.order_id
            JOIN sim_sales.order_line_discounts d
              ON d.order_line_id = l.order_line_id
            WHERE o.order_discount_amount > 0
            GROUP BY o.order_id, o.order_discount_amount
            HAVING abs(o.order_discount_amount - sum(d.discount_amount)) > 0.0001)""") == (0,)


def test_returned_quantities_are_positive(built):
    n, = _one(built, "SELECT count(*) FROM sim_sales.return_lines")
    assert n > 0
    assert _one(built, "SELECT count(*) FROM sim_sales.return_lines "
                       "WHERE qty <= 0") == (0,)
    assert _one(built, "SELECT count(*) FROM sim_sales.returns "
                       "WHERE refund_amount < 0") == (0,)


def test_money_stays_whole_and_tender_ties_out(built):
    assert _one(built, """
        SELECT count(*) FROM (
            SELECT p.order_id FROM sim_sales.payments p
            JOIN sim_sales.orders o ON o.order_id = p.order_id
            GROUP BY p.order_id, o.grand_total
            HAVING abs(o.grand_total - sum(p.amount)
                       FILTER (WHERE p.status <> 'failed')) > 0.0001)""") == (0,)
    # Nothing is captured after world-today, whatever the range holds.
    assert _one(built, "SELECT count(*) FROM sim_sales.payments "
                       "WHERE captured_at > TIMESTAMP '2026-06-15 00:00:00'") == (0,)


def test_currency_waits_for_the_market_launch(built):
    assert _one(built, "SELECT count(*) FROM sim_sales.orders "
                       "WHERE order_datetime < DATE '2025-04-07' "
                       "AND currency_code <> 'USD'") == (0,)
    mix = dict(_all(built, "SELECT currency_code, count(*) FROM sim_sales.orders "
                           "GROUP BY 1"))
    assert set(mix) <= {"USD", "GBP", "EUR", "MXN"}, mix
    assert mix["USD"] / sum(mix.values()) > 0.5


def test_the_sample_storyline_joins_end_to_end(built):
    """Chapter 02 section 2: Priya Raman buys three Aurora deck cushions on
    the web and returns one nine days later."""
    row = _one(built, """
        SELECT c.first_name, c.last_name, o.order_number, l.variant_id,
               l.qty::DOUBLE, o.order_discount_amount::DOUBLE,
               l.line_discount_amount::DOUBLE,
               date_diff('day', o.order_datetime, r.requested_at),
               rl.qty::DOUBLE, rl.restock_warehouse_id, rl.refund_amount::DOUBLE,
               s.session_id, ct.cart_status, sq.query_text
        FROM sim_sales.orders o
        JOIN sim_customer.customers c ON c.customer_id = o.customer_id
        JOIN sim_sales.order_lines l ON l.order_id = o.order_id
        JOIN sim_sales.returns r ON r.order_id = o.order_id
        JOIN sim_sales.return_lines rl ON rl.return_id = r.return_id
                                      AND rl.order_line_id = l.order_line_id
        JOIN sim_ecommerce.carts ct ON ct.converted_order_id = o.order_id
        JOIN sim_ecommerce.web_sessions s ON s.session_id = ct.session_id
        JOIN sim_ecommerce.search_queries sq ON sq.session_id = s.session_id
        WHERE o.order_id = 8840127""")
    assert row == ("Priya", "Raman", "W-2026-8840127", 550231, 3.0,
                   22.49, 0.0, 9, 1.0, 7, 22.49, 77120449, "converted",
                   "slate deck cushion")
    # The variant and the return warehouse are the ones the spec names, and
    # the discount is attributed to the line while the line keeps none of it.
    assert _one(built, """
        SELECT d.promotion_id, d.coupon_id, d.discount_amount::DOUBLE, cp.coupon_code
        FROM sim_sales.order_line_discounts d
        JOIN sim_sales.coupons cp ON cp.coupon_id = d.coupon_id
        WHERE d.order_line_id = 19338451""") == (742, 3018, 22.49, "SPRING25")


def test_drawn_keys_never_reach_the_storyline_ids(built):
    """Every planted key sits outside the block a draw can produce, so no
    volume change can collide with one."""
    for table, column, planted in (
        ("sim_sales.orders", "order_id", 8840127),
        ("sim_sales.order_lines", "order_line_id", 19338451),
        ("sim_sales.payments", "payment_id", 5510887),
        ("sim_sales.returns", "return_id", 440218),
        ("sim_ecommerce.web_sessions", "session_id", 77120449),
        ("sim_ecommerce.carts", "cart_id", 3320117),
    ):
        n, = _one(built, f"SELECT count(*) FROM {table} WHERE {column} = {planted}")
        assert n == 1, f"{table}.{column}"


# --- the two generator properties -----------------------------------------

def _fingerprint(db: Path) -> dict:
    tables = _all(db, "SELECT table_schema, table_name FROM information_schema.tables "
                      "WHERE table_schema LIKE 'sim\\_%' ESCAPE '\\' "
                      "AND table_name NOT LIKE '\\_%' ESCAPE '\\' ORDER BY 1, 2")
    con = duckdb.connect(str(db), read_only=True)
    try:
        return {f"{s}.{t}": con.execute(
            f"SELECT count(*), coalesce(sum(hash(x)), 0) FROM {s}.{t} x").fetchone()
            for s, t in tables}
    finally:
        con.close()


def test_the_same_config_gives_the_same_rows(tmp_path):
    first = _fingerprint(_build(tmp_path, "a"))
    second = _fingerprint(_build(tmp_path, "b"))
    assert first == second
    assert len(first) >= 19


def test_shortening_the_range_leaves_every_earlier_day_alone(tmp_path):
    """Per-day independence. A day's rows come from the seed, the table and
    the date, so dropping the last day of the range must not move one row of
    any day before it."""
    short_end = (dt.date.fromisoformat(END) - dt.timedelta(days=1)).isoformat()
    full = _build(tmp_path, "full")
    short = _build(tmp_path, "short", end=short_end)

    for table, event in (("sim_sales.orders", "order_datetime"),
                         ("sim_sales.returns", "requested_at"),
                         ("sim_ecommerce.web_sessions", "started_at")):
        sql = (f"SELECT {event}::DATE AS ds, count(*), coalesce(sum(hash(x)), 0) "
               f"FROM {table} x WHERE {event}::DATE <= DATE '{short_end}' "
               f"GROUP BY 1 ORDER BY 1")
        assert _all(full, sql) == _all(short, sql), table

    # And the day that was dropped is the only one missing.
    assert _one(short, "SELECT max(order_datetime)::DATE FROM sim_sales.orders "
                       "WHERE order_id <> 8840127") == (dt.date.fromisoformat(short_end),)
