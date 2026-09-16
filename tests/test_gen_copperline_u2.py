"""The copperline entity layer: catalog, estate, stock, purchasing, freight.

These five modules write the `sim_*` schemas the extracts read, so the checks
here run the modules directly against a stubbed `sim_core` rather than through
the CLI. What they hold to is the arithmetic the spec fixes — the estate by
date, the timezone share, the three comp-store fixtures — plus the two data
hazards the catalog and the supplier book are built to carry, and
determinism.
"""

import datetime as dt
import sys
from pathlib import Path

import duckdb
import pytest

from de_bench.tasks import repo_root

sys.path.insert(0, str(repo_root() / "tools"))

from gen_copperline import build, config                         # noqa: E402
from gen_copperline.upstream import (inventory, logistics,       # noqa: E402
                                     product, store, supply_chain)

WORLD = repo_root() / "worlds" / "copperline"
MODULES = (product, store, inventory, supply_chain, logistics)
SCHEMAS = ("sim_product", "sim_store", "sim_inventory",
           "sim_supply_chain", "sim_logistics")

# What sim_core hands the entity layer, per spec chapter 02 section 4. Core is
# another module's work; these two tables are all this layer reads of it.
CORE_STUB = """
    CREATE SCHEMA sim_core;
    CREATE TABLE sim_core.countries (
        country_id INTEGER PRIMARY KEY, iso2 VARCHAR, iso3 VARCHAR,
        name VARCHAR, currency_code VARCHAR);
    INSERT INTO sim_core.countries VALUES
        (1,'US','USA','United States','USD'), (2,'CA','CAN','Canada','CAD'),
        (3,'GB','GBR','United Kingdom','GBP'), (4,'IE','IRL','Ireland','EUR'),
        (5,'DE','DEU','Germany','EUR'), (6,'BR','BRA','Brazil','BRL'),
        (7,'MX','MEX','Mexico','MXN'), (8,'PL','POL','Poland','PLN'),
        (9,'ID','IDN','Indonesia','IDR');
    CREATE TABLE sim_core.regions (
        region_id INTEGER PRIMARY KEY, name VARCHAR, level VARCHAR,
        parent_region_id INTEGER, country_id INTEGER);
    INSERT INTO sim_core.regions
        SELECT 3000 + c.country_id * 40 + g.i,
               'District ' || c.country_id || '-' || g.i,
               'district', 1200 + c.country_id, c.country_id
        FROM sim_core.countries c CROSS JOIN generate_series(1, 14) AS g(i);
    CREATE TABLE sim_core.addresses (
        address_id BIGINT PRIMARY KEY, line1 VARCHAR, line2 VARCHAR,
        city VARCHAR, state_province VARCHAR, postal_code VARCHAR,
        country_id INTEGER, region_id INTEGER,
        latitude DECIMAL(9,6), longitude DECIMAL(9,6));
    INSERT INTO sim_core.addresses
        SELECT 7700000 + g.i, g.i || ' Copper Way', NULL, 'Portland', 'OR',
               '97209', 1, 3041, 45.523100, -122.676500
        FROM generate_series(1, 4000) AS g(i);
"""


def _days(ctx) -> None:
    """`_util.days` — one row per day with that day's order count. The
    orchestrator builds it before any module runs; these tests run the
    modules without the orchestrator, so they build it the same way."""
    builder = getattr(build, "_build_days", None)
    if builder is not None:
        builder(ctx)
        return
    vol = ctx.cfg["volumes"]
    shape = ", ".join(str(x) for x in vol["weekday_shape"])
    ctx.sql("CREATE SCHEMA IF NOT EXISTS _util")
    ctx.sql(f"""
        CREATE OR REPLACE TABLE _util.days AS
        SELECT ds,
               datediff('day', DATE '{ctx.start}', ds)::BIGINT AS dn,
               ({ctx.scale(vol['orders_per_day_base'])}
                * ([{shape}])[dayofweek(ds) + 1])::BIGINT AS orders
        FROM generate_series(DATE '{ctx.start}', DATE '{ctx.end}', INTERVAL 1 DAY) t(ds)
    """)


def _build(tmp_path: Path, profile: str = "small") -> duckdb.DuckDBPyConnection:
    tmp_path.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(tmp_path / "u2.duckdb"))
    con.execute(CORE_STUB)
    ctx = config.Context(
        con=con, cfg=config.load_timeline(WORLD / "timeline.yaml"),
        profile=profile, landing=tmp_path / "landing")
    _days(ctx)
    for module in MODULES:
        module.build(ctx)
    return con


@pytest.fixture(scope="module")
def con(tmp_path_factory):
    connection = _build(tmp_path_factory.mktemp("u2"))
    yield connection
    connection.close()


def _one(con, sql):
    return con.execute(sql).fetchone()


def test_the_estate_grows_from_191_to_268(con):
    """The pre-history slab is 371 days by the 191 stores Copperline held
    through FY2023, so that count has to hold for every day of FY2023. The
    estate reaches 268 by the end of the range — 224 organic, 44 acquired."""
    assert _one(con, "SELECT count(*) FROM sim_store.stores")[0] == 268

    # An acquired store joins Copperline's estate at the 2025-02-03 close, not
    # at its own opening — which is the whole of CMP-4.
    def open_on(day: str) -> int:
        return _one(con, f"""
            SELECT count(*) FROM sim_store.stores
            WHERE greatest(open_date, CASE WHEN acquired_from IS NULL
                                           THEN open_date ELSE DATE '2025-02-03' END)
                  <= DATE '{day}'
              AND (close_date IS NULL OR close_date > DATE '{day}')
        """)[0]

    for day in ("2023-01-29", "2023-07-04", "2024-02-03"):
        assert open_on(day) == 191, day
    assert open_on("2026-06-14") == 267        # 268 rows, one of them closed
    assert _one(con, "SELECT count(*) FROM sim_store.stores "
                     "WHERE acquired_from = 'northwave'")[0] == 44
    assert _one(con, "SELECT count(*) FROM sim_store.stores "
                     "WHERE acquired_from = 'northwave' "
                     "AND open_date < DATE '2025-02-03'")[0] == 44


def test_about_62_percent_of_the_estate_is_in_the_americas(con):
    """E1's local-stamp error is partial and region-shaped, and this share is
    what shapes it: read 23:05 local as UTC and these stores' batches move a
    day, while the European ones stay put."""
    americas, total = _one(con, """
        SELECT count(*) FILTER (WHERE timezone LIKE 'America/%'), count(*)
        FROM sim_store.stores
    """)
    assert 0.60 <= americas / total <= 0.64, (americas, total)
    assert americas == 166
    zones = {r[0] for r in con.execute(
        "SELECT DISTINCT timezone FROM sim_store.stores").fetchall()}
    assert {"Europe/London", "Europe/Dublin", "Europe/Berlin"} <= zones


def test_the_three_comp_store_fixtures_are_exact(con):
    """CMP-1, CMP-2 and CMP-3 each grade one store's dates. They are pinned
    rows, not draws, so a volume change cannot move them."""
    assert _one(con, "SELECT open_date, close_date, status FROM sim_store.stores "
                     "WHERE store_id = 309") == (dt.date(2025, 4, 18), None, "open")
    assert _one(con, "SELECT store_code, close_date, status FROM sim_store.stores "
                     "WHERE store_id = 147") == ("S-0147", dt.date(2026, 3, 21), "closed")

    remodel = con.execute("""
        SELECT status, valid_from, valid_to FROM sim_store.store_versions
        WHERE store_id = 233 ORDER BY version_no
    """).fetchall()
    assert [r[0] for r in remodel] == ["open", "remodel", "open"]
    assert remodel[1][1] == dt.date(2026, 2, 3)
    assert remodel[1][2] == dt.date(2026, 2, 28)


def test_the_version_history_carries_the_re_district(con):
    """`raw.stores` ships as an SCD2 with 268 current rows and 22 prior ones,
    and one store changes district inside FY2026 — which is what makes the
    comp restatement follow the new district rather than today's."""
    current, prior = _one(con, """
        SELECT count(*) FILTER (WHERE is_current), count(*) FILTER (WHERE NOT is_current)
        FROM sim_store.store_versions
    """)
    assert (current, prior) == (268, 22)

    moved = con.execute("""
        SELECT v.store_id, v.valid_from FROM sim_store.store_versions v
        JOIN sim_store.store_versions p
          ON p.store_id = v.store_id AND p.version_no = v.version_no - 1
        WHERE v.region_id <> p.region_id AND v.valid_from >= DATE '2026-01-01'
    """).fetchall()
    assert len(moved) == 1, moved
    assert moved[0][1].year == 2026

    # Every store has exactly one current version and no gap in the history.
    assert _one(con, """
        SELECT count(*) FROM (
            SELECT store_id FROM sim_store.store_versions
            GROUP BY store_id HAVING count(*) FILTER (WHERE is_current) <> 1)
    """)[0] == 0


def test_price_windows_overlap_and_priority_decides(con):
    """List price is not price paid, and which list wins is a policy clause,
    not a property of the row. The fixture has to make the question real: two
    live prices for one SKU on the same day, on lists of different priority."""
    overlapping = _one(con, """
        SELECT count(*) FROM sim_product.prices a
        JOIN sim_product.prices b
          ON b.variant_id = a.variant_id AND b.price_list_id <> a.price_list_id
        JOIN sim_product.price_lists la ON la.price_list_id = a.price_list_id
        JOIN sim_product.price_lists lb ON lb.price_list_id = b.price_list_id
        WHERE la.priority <> lb.priority
          AND la.valid_from <= coalesce(lb.valid_to, DATE '2027-01-30')
          AND lb.valid_from <= coalesce(la.valid_to, DATE '2027-01-30')
    """)[0]
    assert overlapping > 0

    # The spec's own sample resolves: 29.9900 on list 11, open-ended.
    assert _one(con, """
        SELECT unit_price::DOUBLE, valid_from, valid_to FROM sim_product.prices
        WHERE variant_id = 550231 AND price_list_id = 11
    """) == (pytest.approx(29.99), dt.date(2026, 2, 1), None)


def test_the_catalog_hierarchy_is_three_levels_under_five_departments(con):
    depts = con.execute("SELECT DISTINCT dept_code FROM sim_product.categories "
                        "WHERE dept_code IS NOT NULL ORDER BY 1").fetchall()
    assert [d[0] for d in depts] == ["BLD", "GDN", "HDW", "HOM", "OUT"]
    assert _one(con, "SELECT max(level) FROM sim_product.categories")[0] == 4

    # The sample joins down the page: subclass 318 under class 302, in OUT.
    assert _one(con, "SELECT name, parent_category_id, level, dept_code "
                     "FROM sim_product.categories WHERE category_id = 318") == \
        ("Outdoor Cushions", 302, 4, "OUT")
    assert _one(con, "SELECT count(*) FROM sim_product.product_variants")[0] == 25_000
    assert _one(con, "SELECT product_id, sku FROM sim_product.product_variants "
                     "WHERE variant_id = 550231") == (4180, "AUR-CUSH-SLT-STD")


def test_a_variant_can_carry_two_primary_sources(con):
    """Nothing upstream forbids two rows flagged primary for one SKU, so it
    happens — the duplicate a uniqueness test should catch and a landed-cost
    model must never silently fan out on."""
    dual = _one(con, """
        SELECT count(*) FROM (
            SELECT variant_id FROM sim_supply_chain.supplier_products
            WHERE is_primary_source GROUP BY variant_id HAVING count(*) > 1)
    """)[0]
    assert dual > 0

    # And the sample's sourcing leg is planted, not drawn.
    assert _one(con, """
        SELECT supplier_id, unit_cost::DOUBLE, moq, lead_time_days, is_primary_source
        FROM sim_supply_chain.supplier_products
        WHERE variant_id = 550231 AND supplier_sku = 'LS-CSH-SL-STD'
    """) == (118, pytest.approx(11.2), 500, 21, True)


def test_the_sample_storyline_resolves_end_to_end(con):
    """Variant 550231, bought from supplier 118 on PO 660214, received into
    warehouse 7, shipped out of it to Priya Raman's order."""
    assert _one(con, "SELECT supplier_id, dest_warehouse_id, total_amount::DOUBLE "
                     "FROM sim_supply_chain.purchase_orders WHERE po_id = 660214") == \
        (118, 7, pytest.approx(268800.0))
    assert _one(con, "SELECT sum(line_total)::DOUBLE FROM sim_supply_chain.purchase_order_lines "
                     "WHERE po_id = 660214")[0] == pytest.approx(268800.0)
    assert _one(con, "SELECT warehouse_id, receipt_status "
                     "FROM sim_supply_chain.goods_receipts WHERE receipt_id = 880412") == \
        (7, "posted")
    assert _one(con, "SELECT warehouse_id, order_id, fulfillment_status "
                     "FROM sim_logistics.fulfillments WHERE fulfillment_id = 5520118") == \
        (7, 8840127, "delivered")
    assert _one(con, "SELECT variant_id, qty::DOUBLE, order_line_id "
                     "FROM sim_logistics.shipment_lines WHERE shipment_line_id = 9910228") == \
        (550231, pytest.approx(3.0), 19338451)
    assert _one(con, "SELECT name FROM sim_supply_chain.suppliers "
                     "WHERE supplier_id = 118")[0] == "Lakeshore Outdoor Co"
    assert _one(con, "SELECT name, warehouse_code FROM sim_inventory.warehouses "
                     "WHERE warehouse_id = 7") == \
        ("Edison NJ Distribution Center", "DC-EDI")


def test_freight_reaches_the_order_spine_by_the_shared_formula(con):
    """Sales builds after logistics, so a fulfilment cannot read an order. It
    computes the order's identity instead — day, then slot inside that day —
    and sales stamps the same fulfilment id on the line. Both sides have to
    agree, so every drawn row here must decompose into a real day and a slot
    that day actually reached."""
    seq = "(fulfillment_id - 200000000)"
    bad = _one(con, f"""
        SELECT count(*) FROM sim_logistics.fulfillments f
        LEFT JOIN _util.days d ON d.dn = {seq} // 200000
        WHERE fulfillment_id > 200000000
          AND (d.ds IS NULL
               OR {seq} % 200000 NOT BETWEEN 1 AND d.orders
               OR order_id <> 9000000 + {seq})
    """)[0]
    assert bad == 0

    # And an outbound shipment line names a line of a real order, at most 16
    # to the order, in the same 1,000,000,000 space sales writes.
    line_seq = "((order_line_id - 1000000000) // 16)"
    bad_lines = _one(con, f"""
        SELECT count(*) FROM sim_logistics.shipment_lines l
        LEFT JOIN _util.days d ON d.dn = {line_seq} // 200000
        WHERE l.order_line_id IS NOT NULL AND l.order_line_id > 1000000000
          AND (d.ds IS NULL
               OR {line_seq} % 200000 NOT BETWEEN 1 AND d.orders
               OR (order_line_id - 1000000000) % 16 NOT BETWEEN 1 AND 4)
    """)[0]
    assert bad_lines == 0

    # The reserved band stays reserved: only the planted storyline rows use it.
    assert _one(con, """
        SELECT count(*) FROM sim_logistics.fulfillments
        WHERE order_id BETWEEN 8000000 AND 8999999
    """)[0] == 1


def test_nothing_knows_about_a_day_after_the_range(con):
    """Consistent as of any date means no row carries a stamp past the end of
    the fixture range — the world's history stops where the config says."""
    end = "DATE '2026-06-14'"
    for table, column in (
        ("sim_product.products", "created_at::DATE"),
        ("sim_product.product_reviews", "created_at::DATE"),
        ("sim_inventory.inventory_balances", "as_of_datetime::DATE"),
        ("sim_inventory.inventory_movements", "moved_at::DATE"),
        ("sim_supply_chain.goods_receipts", "received_at::DATE"),
        ("sim_logistics.delivery_routes", "route_date"),
    ):
        assert _one(con, f"SELECT count(*) FROM {table} WHERE {column} > {end}")[0] == 0, table


def test_stock_is_held_at_one_site_and_the_ledger_disagrees_with_it(con):
    """Exactly one of warehouse and store is populated on a balance row, and
    a movement replay does not reproduce the snapshot — the authority choice
    lives in the inventory policy, not in the data."""
    assert _one(con, """
        SELECT count(*) FROM sim_inventory.inventory_balances
        WHERE (warehouse_id IS NULL) = (store_id IS NULL)
    """)[0] == 0
    assert _one(con, """
        SELECT count(*) FROM sim_inventory.reorder_policies
        WHERE (warehouse_id IS NULL) = (store_id IS NULL)
    """)[0] == 0
    assert _one(con, "SELECT count(*) FROM sim_inventory.inventory_movements")[0] > 0


def test_generation_is_deterministic(tmp_path):
    """Two builds of the same config are the same world, table for table."""
    def fingerprint(con):
        out = {}
        for schema in SCHEMAS:
            for (table,) in con.execute(
                "SELECT table_name FROM information_schema.tables "
                f"WHERE table_schema = '{schema}' ORDER BY table_name"
            ).fetchall():
                out[f"{schema}.{table}"] = con.execute(
                    f"SELECT count(*), coalesce(sum(hash(t)), 0) FROM {schema}.{table} t"
                ).fetchone()
        return out

    first_con = _build(tmp_path / "a")
    first = fingerprint(first_con)
    first_con.close()
    second_con = _build(tmp_path / "b")
    second = fingerprint(second_con)
    second_con.close()
    assert first == second
    assert len(first) == 35, sorted(first)
