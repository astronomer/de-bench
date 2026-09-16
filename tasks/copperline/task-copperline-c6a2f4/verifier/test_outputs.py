"""FIN-446 — does the patched margin build publish a day of `marts.category_margin`
that costs every line once and drops nothing?

Never ships to the agent. It runs beside the scored tree, with the tree root on
`PYTHONPATH`, so it imports the patched module the same way a DAG would.

**Why a verifier rather than a DAG run.** The world ships the landed half of the
warehouse only — `AGENTS.md`, "The warehouse": `staging.*` and `marts.*` are
derived, and so is every `int` model, so none of the three relations
`build_margin` reads is on disk until the 04:00 dbt build has made them.
`marts.category_margin` has no DDL anywhere in the workspace either. Standing
all of that up costs a 143-model dbt run for one group-by. Instead the fixture
below makes the four relations the build touches, out of `raw.*`, with the same
projection the shipped models make of them, and calls the patched build
directly. It also removes the second reason a run would say nothing: a trial's
patch never carries anything under `include/data`, so a partition an agent
published by hand is a partition no check can read.

**Where the fixture comes from.** Every statement in `FIXTURE` is the shipped
dbt model with the jinja resolved and nothing else changed:

    int.int_order_lines_discounted   models/shared/intermediate/int_order_lines_discounted.sql
    int.int_net_sales_lines          models/shared/intermediate/int_net_sales_lines.sql
    int.int_order_lines_costed       models/shared/intermediate/int_order_lines_costed.sql
    marts.dim_product                models/commerce/dim_product.sql

Two deliberate differences, both recorded here because they are the only places
the fixture is not a transcription:

1. `order_line_key` is `md5(order_id || '-' || line_no)` rather than
   `dbt_utils.surrogate_key`. Nothing under test reads the value; what matters
   is that the key is unique per line and that the two line models agree on it,
   and the first test asserts both.
2. `dim_product` is built from `raw.pim_product_versions` directly — the current
   version of each SKU — rather than through the two dbt snapshots, which need a
   `dbt snapshot` run this world has never had. The column the build reads,
   `category_id`, is the same value either way.

The three line relations are materialised over the graded days only. That keeps
a build under test to milliseconds and keeps a wrong join bounded: a match on
something that is not the line key fans out inside the fixture instead of
against nine million rows, and still lands nowhere near the expected result.

**No authored numbers.** The day's cost is computed from `raw.order_lines` and
`raw.orders` in the same session, with the extension the costed model states.
The day's net sales is the shared model's own total for the day, which is what
`tie_to_sales` holds the mart against every night. The row ceiling is the
catalog's own category count. Nothing here is typed in.

**Three days, on purpose.** All three are ordinary trading days and the ticket
names no date at all, so a fix cannot be scoped to a day somebody pointed at.
Three rather than one for a second reason as well: a join on the order and the
SKU, rather than on the line key, is right on a day where no order repeats a SKU
and wrong on a day where one does. Measured, that answer passes 2026-01-13 and
2026-04-21 and fails 2025-12-09.

    2026-01-13
    2025-12-09
    2026-04-21
"""

from __future__ import annotations

import duckdb
import pytest

# The scored tree is on PYTHONPATH (scoring.build_score_env), so the house
# library resolves the warehouse the same way every DAG in the world does —
# `DUCKDB_PATH` against the root that holds `include/`, whatever the working
# directory happens to be.
from include.lib import warehouse

DB = str(warehouse.warehouse_path())
TABLE = "marts.category_margin"

# Every name here is qualified onto the live warehouse, for the reason
# `include/lib/warehouse.py` gives: the frozen `nwv` copy sits first on the
# search path and carries `raw.orders` and `raw.order_lines` under the same
# names, so an unqualified read would answer with September 2025's Northwave
# numbers.
ORDERS = warehouse.qualify("raw.orders")
ORDER_LINES = warehouse.qualify("raw.order_lines")
INT_SCHEMA = warehouse.qualify('"int"')
MARTS_SCHEMA = warehouse.qualify("marts")
MART = warehouse.qualify(TABLE)
DIM_PRODUCT = warehouse.qualify("marts.dim_product")
NET_LINES = f"{INT_SCHEMA}.int_net_sales_lines"
COSTED_LINES = f"{INT_SCHEMA}.int_order_lines_costed"
DISCOUNTED_LINES = f"{INT_SCHEMA}.int_order_lines_discounted"

#: The days the fix is measured on. See the module docstring.
GRADED = ("2026-01-13", "2025-12-09", "2026-04-21")
DAYS_SQL = ", ".join(f"DATE '{day}'" for day in GRADED)

#: The floor `fin_margin_daily`'s own `check_rows` puts under the day.
ROW_FLOOR = 20

DISCOUNTED_SQL = f"""
CREATE OR REPLACE TABLE {DISCOUNTED_LINES} AS
WITH headers AS (
    SELECT order_id, local_order_date AS order_date, channel, market_code, store_id,
           CAST(order_discount_cents AS BIGINT) AS order_discount_cents
    FROM {ORDERS}
    WHERE NOT coalesce(is_test, false) AND deleted_at IS NULL
      AND local_order_date IN ({DAYS_SQL})
),
lines AS (
    SELECT md5(l.order_id || '-' || CAST(l.line_no AS VARCHAR)) AS order_line_key,
           l.order_id, l.order_line_id, CAST(l.line_no AS INTEGER) AS line_no,
           l.sku, CAST(l.qty AS DECIMAL(12,3)) AS qty,
           CAST(l.unit_price_cents AS BIGINT) AS unit_price_cents,
           CAST(l.unit_cost_cents AS BIGINT) AS unit_cost_cents,
           CAST(l.line_discount_cents AS BIGINT) AS line_discount_cents,
           CAST(l.tax_cents AS BIGINT) AS tax_cents,
           CAST(l.line_total_cents AS BIGINT) AS line_total_cents
    FROM {ORDER_LINES} l
    WHERE l.order_id IN (SELECT order_id FROM headers)
),
joined AS (
    SELECT l.*, h.order_date, h.channel, h.market_code, h.store_id, h.order_discount_cents,
           sum(l.line_total_cents) OVER (PARTITION BY l.order_id) AS order_line_total_cents
    FROM lines l JOIN headers h ON h.order_id = l.order_id
),
floors AS (
    SELECT *, CASE WHEN order_line_total_cents > 0
                   THEN CAST((CAST(order_discount_cents AS DECIMAL(38,6)) * line_total_cents)
                             / order_line_total_cents AS DECIMAL(38,6))
                   ELSE CAST(0 AS DECIMAL(38,6)) END AS exact_share
    FROM joined
),
remainders AS (
    SELECT *, CAST(floor(exact_share) AS BIGINT) AS floor_cents,
           order_discount_cents - sum(CAST(floor(exact_share) AS BIGINT))
               OVER (PARTITION BY order_id) AS cents_to_hand_out,
           row_number() OVER (PARTITION BY order_id
               ORDER BY exact_share - floor(exact_share) DESC, line_total_cents DESC, line_no)
               AS remainder_rank
    FROM floors
)
SELECT order_line_key, order_id, order_line_id, line_no, order_date, channel, market_code,
       store_id, sku, qty, unit_price_cents, tax_cents, line_discount_cents, line_total_cents,
       CASE WHEN remainder_rank <= cents_to_hand_out THEN floor_cents + 1
            ELSE floor_cents END                                AS header_discount_cents,
       line_total_cents - CASE WHEN remainder_rank <= cents_to_hand_out THEN floor_cents + 1
                               ELSE floor_cents END             AS discounted_line_cents,
       line_discount_cents + CASE WHEN remainder_rank <= cents_to_hand_out THEN floor_cents + 1
                                  ELSE floor_cents END          AS total_discount_cents
FROM remainders
"""

NET_SQL = f"""
CREATE OR REPLACE TABLE {NET_LINES} AS
WITH returns_by_line AS (
    SELECT order_id, order_line_id, sum(CAST(refund_cents AS BIGINT)) AS refund_cents,
           sum(CAST(qty AS DECIMAL(12,3))) AS returned_qty,
           min(CAST(initiated_at AS DATE)) AS first_return_date,
           max(CAST(initiated_at AS DATE)) AS last_return_date,
           count(*) AS return_count
    FROM {warehouse.qualify('raw.returns')} GROUP BY 1, 2
),
orders AS (
    SELECT order_id, order_status, brand, source_system, loyalty_id, customer_ref
    FROM {ORDERS} WHERE NOT coalesce(is_test, false) AND deleted_at IS NULL
)
SELECT d.order_line_key, d.order_id, d.order_line_id, d.line_no, d.order_date, d.channel,
       d.market_code, d.store_id, d.sku, o.brand, o.source_system, o.order_status,
       o.customer_ref, o.loyalty_id,
       d.qty, coalesce(r.returned_qty, 0) AS returned_qty,
       d.qty - coalesce(r.returned_qty, 0) AS net_qty,
       d.line_total_cents AS gross_line_cents, d.line_discount_cents, d.header_discount_cents,
       d.total_discount_cents, d.discounted_line_cents,
       coalesce(r.refund_cents, 0) AS refund_cents,
       least(coalesce(r.refund_cents, 0), d.discounted_line_cents) AS returned_cents,
       d.discounted_line_cents - least(coalesce(r.refund_cents, 0), d.discounted_line_cents)
           AS net_sales_cents,
       d.tax_cents, r.first_return_date, r.last_return_date,
       coalesce(r.return_count, 0) AS return_count,
       r.order_id IS NOT NULL AS has_return
FROM {DISCOUNTED_LINES} d
JOIN orders o ON o.order_id = d.order_id
LEFT JOIN returns_by_line r ON r.order_id = d.order_id AND r.order_line_id = d.order_line_id
"""

# The costed model carries no `order_line_id`. That is the shipped model's own
# column list and it is left exactly as it is: the ordinal is not a key, so the
# model that publishes the cost does not publish the ordinal.
COSTED_SQL = f"""
CREATE OR REPLACE TABLE {COSTED_LINES} AS
WITH orders AS (
    SELECT order_id, local_order_date AS order_date, channel FROM {ORDERS}
    WHERE NOT coalesce(is_test, false) AND deleted_at IS NULL
),
lines AS (
    SELECT md5(l.order_id || '-' || CAST(l.line_no AS VARCHAR)) AS order_line_key,
           l.order_id, CAST(l.line_no AS INTEGER) AS line_no, l.sku,
           CAST(l.qty AS DECIMAL(12,3)) AS qty,
           CAST(l.unit_cost_cents AS BIGINT) AS unit_cost_cents,
           CAST(l.line_total_cents AS BIGINT) AS line_total_cents
    FROM {ORDER_LINES} l
    WHERE l.order_id IN (SELECT order_id FROM {DISCOUNTED_LINES})
)
SELECT l.order_line_key, l.order_id, l.line_no, o.order_date, o.channel, l.sku, l.qty,
       l.unit_cost_cents, l.unit_cost_cents IS NOT NULL AS cost_is_stated,
       CASE WHEN l.unit_cost_cents IS NOT NULL
            THEN CAST(round(CAST(l.qty AS DECIMAL(18,4)) * l.unit_cost_cents, 0) AS BIGINT)
       END AS extended_cost_cents,
       l.line_total_cents
FROM lines l JOIN orders o ON o.order_id = l.order_id
"""

DIM_PRODUCT_SQL = f"""
CREATE OR REPLACE TABLE {DIM_PRODUCT} AS
WITH current_version AS (
    SELECT sku, product_name, category_id, brand, supplier_id, list_price_cents, status
    FROM (SELECT *, row_number() OVER (PARTITION BY sku
                    ORDER BY updated_at DESC, change_id DESC) AS rn
          FROM {warehouse.qualify('raw.pim_product_versions')}
          WHERE operation <> 'delete') WHERE rn = 1
),
categories AS (
    SELECT category_id, name AS category_name, dept_code, parent_id
    FROM {warehouse.qualify('raw.product_categories')} WHERE valid_to IS NULL
)
SELECT v.sku AS product_key, v.sku, v.product_name, v.brand, v.supplier_id, v.status,
       v.status = 'active' AS is_active, v.category_id, c.category_name, c.dept_code,
       c.parent_id AS parent_category_id, v.list_price_cents
FROM current_version v LEFT JOIN categories c ON c.category_id = v.category_id
"""

MART_SQL = f"""
CREATE TABLE IF NOT EXISTS {MART} (
    ds DATE, category_id VARCHAR, net_sales_cents BIGINT,
    landed_cost_cents BIGINT, merch_margin_cents BIGINT)
"""

#: The day's cost of goods, read off the order book with the extension the
#: costed model states — quantity times the stated unit cost, rounded half up to
#: the cent, once. This is the expected result and there is nothing else behind
#: it.
TRUTH_COST = f"""
SELECT coalesce(sum(CAST(round(CAST(l.qty AS DECIMAL(18,4)) * l.unit_cost_cents, 0)
                        AS BIGINT)), 0)
FROM {ORDER_LINES} l
JOIN {ORDERS} o ON o.order_id = l.order_id
WHERE o.local_order_date = ?
  AND NOT coalesce(o.is_test, false)
  AND o.deleted_at IS NULL
"""

#: The day's net sales, as the shared model publishes it. `tie_to_sales` holds
#: the mart against this figure every night, and the ticket restates it.
TRUTH_NET = f"""
SELECT coalesce(sum(net_sales_cents), 0) FROM {NET_LINES} WHERE order_date = ?
"""


def query(sql: str, params: list | None = None) -> list[tuple]:
    """One read, on its own connection. DuckDB takes one writer and the build
    under test opens its own, so nothing here may hold the file open."""
    con = duckdb.connect(DB)
    try:
        return con.execute(sql, params or []).fetchall()
    finally:
        con.close()


@pytest.fixture(scope="session", autouse=True)
def warehouse_ready():
    """The four derived relations the build reads, and the table it writes."""
    con = duckdb.connect(DB)
    try:
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {INT_SCHEMA}")
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {MARTS_SCHEMA}")
        for statement in (DISCOUNTED_SQL, NET_SQL, COSTED_SQL, DIM_PRODUCT_SQL, MART_SQL):
            con.execute(statement)
    finally:
        con.close()


def build(ds: str) -> None:
    """Run the patched build for one day, the way the DAG's `build` task does."""
    from projects.finance.lib import revenue

    revenue.build_margin(ds, TABLE)


def built(ds: str) -> list[tuple]:
    return query(
        f"SELECT category_id, net_sales_cents, landed_cost_cents, merch_margin_cents "
        f"FROM {MART} WHERE ds = ? ORDER BY category_id",
        [ds],
    )


def test_the_two_line_models_still_key_the_line_the_way_this_grades():
    """The fixture's own guard, before anything is graded.

    The whole join is that `order_line_key` is the line and the ordinal is not.
    If a later change makes the ordinal unique, or splits the key between the
    two models, every test below is grading a difference that no longer exists
    and this one fails first.
    """
    lines, keys, ordinals = query(
        f"SELECT count(*), count(DISTINCT order_line_key), count(DISTINCT order_line_id) "
        f"FROM {NET_LINES}"
    )[0]
    assert keys == lines, f"{lines} lines resolve to {keys} keys; the key is not the key"
    assert ordinals < 100, (
        f"order_line_id holds {ordinals} distinct values; it is not an ordinal any more"
    )
    unmatched = query(
        f"SELECT count(*) FROM {NET_LINES} s "
        f"LEFT JOIN {COSTED_LINES} c ON c.order_line_key = s.order_line_key "
        "WHERE c.order_line_key IS NULL"
    )[0][0]
    assert unmatched == 0, f"{unmatched} net sales lines have no costed line on the key"


def test_the_unit_cost_and_the_extended_cost_are_far_apart():
    """The guard on the cost column. A line's cost is its quantity times the
    stated unit cost, and summing the unit cost instead leaves net sales
    untouched. If the two ever came out the same, that answer would pass by
    accident rather than by being right.
    """
    for ds in GRADED:
        extended = query(TRUTH_COST, [ds])[0][0]
        unit = query(
            f"SELECT coalesce(sum(unit_cost_cents), 0) FROM {COSTED_LINES} WHERE order_date = ?",
            [ds],
        )[0][0]
        assert extended > unit * 2, (
            f"FAIL: {ds}: the extended cost is {extended} and the unit cost is {unit}; "
            "the two answers are too close for this day to grade the column"
        )


def test_the_catalog_still_misses_most_of_the_book():
    """The guard on the left join. The order book sells a wider SKU range than
    the catalog carries, which is why dropping the misses is convictable. If the
    catalog ever covered the book, an inner join would tie and this test says so
    rather than letting the rest grade nothing.
    """
    for ds in GRADED:
        whole, matched = query(
            f"SELECT coalesce(sum(s.net_sales_cents), 0), "
            f"       coalesce(sum(s.net_sales_cents) FILTER (WHERE p.sku IS NOT NULL), 0) "
            f"FROM {NET_LINES} s LEFT JOIN {DIM_PRODUCT} p ON p.sku = s.sku "
            "WHERE s.order_date = ?",
            [ds],
        )[0]
        assert whole > 0, f"FAIL: {ds}: the day sold nothing, so nothing is graded"
        assert matched * 4 < whole, (
            f"FAIL: {ds}: the catalog carries {matched} of {whole} cents; the day no longer "
            "tells a left join from an inner one"
        )


@pytest.mark.parametrize("ds", GRADED)
def test_the_day_ties_to_the_shared_net_sales_model(ds: str):
    """The ticket's first rule, and the check `fin_margin_daily` runs every
    night. A join that fans out moves this figure, and so does an inner join to
    the catalog.
    """
    build(ds)
    total = query(
        f"SELECT coalesce(sum(net_sales_cents), 0) FROM {MART} WHERE ds = ?", [ds]
    )[0][0]
    expected = query(TRUTH_NET, [ds])[0][0]
    assert total == expected, (
        f"FAIL: {ds}: the day's net sales come out at {total} cents and the shared model "
        f"says {expected}"
    )


@pytest.mark.parametrize("ds", GRADED)
def test_the_day_costs_every_line_once(ds: str):
    """The substance. The day's cost of goods, against the order book.

    This is the figure nothing in the DAG can see. The tie is written against
    net sales, so a cost that counts one unit where the line sold four passes
    every check the pipeline runs and takes the margin several times too wide.
    """
    build(ds)
    total = query(
        f"SELECT coalesce(sum(landed_cost_cents), 0) FROM {MART} WHERE ds = ?", [ds]
    )[0][0]
    expected = query(TRUTH_COST, [ds])[0][0]
    assert total == expected, (
        f"FAIL: {ds}: the day's cost of goods comes out at {total} cents and the order "
        f"book says {expected}"
    )


@pytest.mark.parametrize("ds", GRADED)
def test_the_margin_on_a_row_is_that_row_s_arithmetic(ds: str):
    """`merch_margin_cents` is the row's own net sales less the row's own cost,
    on every row. A day whose totals happen to land while the rows do not is not
    a day anybody can publish.
    """
    build(ds)
    wrong = [row for row in built(ds) if row[3] != row[1] - row[2]]
    assert not wrong, (
        f"FAIL: {ds}: {len(wrong)} row(s) carry a margin that is not net sales less cost: "
        f"{wrong[:5]}"
    )


@pytest.mark.parametrize("ds", GRADED)
def test_the_day_is_one_row_a_category(ds: str):
    """The ticket's grain rule, and the floor the DAG's own `check_rows` puts
    under the day.

    The ceiling is the catalog's own category count plus one, for the bucket the
    uncatalogued lines land in. A build that groups the misses by something
    finer than a category — the SKU, the order — goes over it.
    """
    build(ds)
    rows = built(ds)
    groups = query(
        f"SELECT count(*) FROM (SELECT category_id FROM {MART} WHERE ds = ? GROUP BY 1)", [ds]
    )[0][0]
    ceiling = query(f"SELECT count(DISTINCT category_id) + 1 FROM {DIM_PRODUCT}")[0][0]
    assert len(rows) >= ROW_FLOOR, (
        f"FAIL: {ds}: {len(rows)} row(s) for the day, and {ROW_FLOOR} is the floor "
        "check_rows puts under it"
    )
    assert groups == len(rows), (
        f"FAIL: {ds}: {len(rows)} rows over {groups} categories; a category is in the day twice"
    )
    assert len(rows) <= ceiling, (
        f"FAIL: {ds}: {len(rows)} rows against {ceiling} categories in the catalog; the day "
        "is grouped by something finer than a category"
    )


def test_rebuilding_the_same_day_leaves_one_copy():
    """A repair that has to be run twice must not double the day."""
    ds = GRADED[-1]
    build(ds)
    once = built(ds)
    build(ds)
    assert built(ds) == once, f"FAIL: {ds}: the second build changed the day"
