"""PRO-1140 — does the promotion cost page carry what the promotions took?

Never ships to the agent. It runs beside the scored tree, with the tree root on
`PYTHONPATH`, so it imports the delivered module the same way a DAG would and
resolves the warehouse the way every DAG in the world does.

**No authored numbers.** Every expected result is computed from
`raw.promo_applications`, `raw.promotions`, `raw.orders` and `raw.order_lines`
in the same session. Nothing here holds a cent, a row count or a promotion id
typed by hand, so a rebuild of the world cannot make this verifier stale.

**Why a module and not a DAG run.** There is no job for this page yet — the
ticket asks for the module the Monday job will call. The page is also built
from `raw.*` alone, which the world ships landed, so nothing has to be
materialised first. The one thing the fixture stands up is the mart itself,
which no DDL in the workspace creates.

**Three days, on purpose.** The ticket names no date at all: it says "the last
year" and "the 6th". So all three days below are days the ticket never reaches,
and one of them sits seven months back, well outside the fortnight a reader
would take "the reviews start on the 6th" to mean.

**What the two guard queries are for.** `ELIGIBLE` is the answer the world's
own promotion models give: `int_promo_exposure` joins each promotion to the
orders it says it was eligible for — inside its window, in one of its markets,
over its minimum — and every application outside that join is dropped along
with its money. `UNFILTERED` is the answer a page built without reading
`stg_sales__orders` gives: staff test orders and rows the OMS deleted left in.
The first test proves each graded day can still tell all three apart, so a
green run means the delivered page agreed with the feed rather than with one of
the two wrong readings.
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
TABLE = "marts.promo_cost_daily"

# Every name is qualified onto the live warehouse, for the reason
# `include/lib/warehouse.py` gives: the frozen `nwv` copy sits first on the
# search path and carries `raw.orders` and `raw.order_lines` under the same
# names, so an unqualified read would answer with September 2025's Northwave
# numbers.
APPS = warehouse.qualify("raw.promo_applications")
PROMOS = warehouse.qualify("raw.promotions")
ORDERS = warehouse.qualify("raw.orders")
LINES = warehouse.qualify("raw.order_lines")
MARTS_SCHEMA = warehouse.qualify("marts")
MART = warehouse.qualify(TABLE)

#: The days the page is measured on. See the module docstring.
IN_YEAR = "2026-05-12"
EARLIER = "2026-03-17"
LAST_AUTUMN = "2025-11-19"
GRADED = (IN_YEAR, EARLIER, LAST_AUTUMN)

#: The columns the ticket names, in the order it names them, less `ds`.
COLUMNS = ("promo_id", "promo_code", "orders", "applications",
           "header_cents", "line_cents", "discount_cents")

#: The two filters `stg_sales__orders` applies and nothing else in the tree
#: does. The view's own header says why: `is_test` rows are staff transactions
#: against the live OMS, and the OMS deletes rather than cancels.
POPULATION = "NOT coalesce(o.is_test, false) AND o.deleted_at IS NULL"

#: What the promotion feed says each promotion took on the day. This is the
#: expected result and there is nothing else behind it.
TRUTH = f"""
SELECT a.promo_id,
       p.promo_code,
       count(DISTINCT a.order_id)::BIGINT AS orders,
       count(*)::BIGINT                   AS applications,
       sum(CASE WHEN a.order_line_id IS NULL
                THEN a.discount_cents ELSE 0 END)::BIGINT AS header_cents,
       sum(CASE WHEN a.order_line_id IS NOT NULL
                THEN a.discount_cents ELSE 0 END)::BIGINT AS line_cents,
       sum(a.discount_cents)::BIGINT      AS discount_cents
FROM {APPS} a
JOIN {ORDERS} o ON o.order_id = a.order_id
JOIN {PROMOS} p ON p.promo_id = a.promo_id
WHERE o.local_order_date = ?
  AND {POPULATION}
GROUP BY 1, 2
ORDER BY 1
"""

#: The world's own promotion models' answer: only the applications the
#: promotion register says the order was eligible for. Kept here to prove the
#: graded days can tell the two apart.
ELIGIBLE = f"""
SELECT a.promo_id,
       p.promo_code,
       count(DISTINCT a.order_id)::BIGINT,
       count(*)::BIGINT,
       sum(CASE WHEN a.order_line_id IS NULL
                THEN a.discount_cents ELSE 0 END)::BIGINT,
       sum(CASE WHEN a.order_line_id IS NOT NULL
                THEN a.discount_cents ELSE 0 END)::BIGINT,
       sum(a.discount_cents)::BIGINT
FROM {APPS} a
JOIN {ORDERS} o ON o.order_id = a.order_id
JOIN {PROMOS} p ON p.promo_id = a.promo_id
WHERE o.local_order_date = ?
  AND {POPULATION}
  AND o.local_order_date BETWEEN CAST(p.starts_at AS DATE) AND CAST(p.ends_at AS DATE)
  AND list_contains(string_split(p.market_codes, ','), o.market_code)
  AND o.subtotal_cents >= coalesce(p.min_order_cents, 0)
GROUP BY 1, 2
ORDER BY 1
"""

#: The answer a page built without reading `stg_sales__orders` gives.
UNFILTERED = TRUTH.replace(f"  AND {POPULATION}\n", "")

#: The tie the ticket states: the day's discounts, off the orders they came
#: from, over the same population.
TIE = f"""
SELECT (SELECT coalesce(sum(o.order_discount_cents), 0)
        FROM {ORDERS} o
        WHERE o.local_order_date = ? AND {POPULATION})
     + (SELECT coalesce(sum(l.line_discount_cents), 0)
        FROM {LINES} l
        JOIN {ORDERS} o ON o.order_id = l.order_id
        WHERE o.local_order_date = ? AND {POPULATION})
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
    """The mart the page is published into. No DDL in the workspace creates it,
    so a delivered build that creates it for itself finds it already there and
    a delivered build that does not is graded on its rows rather than on where
    it put its `CREATE TABLE`."""
    con = duckdb.connect(DB)
    try:
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {MARTS_SCHEMA}")
        con.execute(
            f"CREATE TABLE IF NOT EXISTS {MART} ("
            "ds DATE, promo_id VARCHAR, promo_code VARCHAR, orders BIGINT, "
            "applications BIGINT, header_cents BIGINT, line_cents BIGINT, "
            "discount_cents BIGINT)"
        )
    finally:
        con.close()


def build(ds: str):
    """Run the delivered build for one day, the way a job would."""
    from projects.growth.lib import promo_cost

    return promo_cost.build_daily(ds, TABLE)


def built(ds: str) -> list[tuple]:
    return query(
        f"SELECT {', '.join(COLUMNS)} FROM {MART} WHERE ds = ? ORDER BY promo_id",
        [ds],
    )


def check_day(ds: str) -> None:
    expected = query(TRUTH, [ds])
    assert expected, f"FAIL: {ds}: no promotion took anything, so nothing is graded"
    build(ds)
    actual = built(ds)
    assert actual == expected, (
        f"FAIL: {ds}: the page holds {len(actual)} rows totalling "
        f"{sum(r[6] for r in actual)} cents; the promotion feed says "
        f"{len(expected)} rows totalling {sum(r[6] for r in expected)} cents"
    )


def test_the_graded_days_can_tell_the_readings_apart():
    """The fixture's own guard. Each graded day has to separate the feed's
    answer from the eligibility-filtered one and from the unfiltered one, or
    the day proves nothing."""
    for ds in GRADED:
        truth = query(TRUTH, [ds])
        assert query(ELIGIBLE, [ds]) != truth, (
            f"FAIL: {ds}: every application on this day is eligible under its "
            "own promotion, so the day cannot grade the finding"
        )
        assert query(UNFILTERED, [ds]) != truth, (
            f"FAIL: {ds}: no test or deleted order carries a promotion on this "
            "day, so the day cannot grade the population"
        )


def test_a_day_in_the_review_year_matches_the_feed():
    check_day(IN_YEAR)


def test_a_day_two_months_earlier_matches_the_feed():
    check_day(EARLIER)


def test_a_day_seven_months_back_matches_the_feed():
    check_day(LAST_AUTUMN)


def test_the_day_ties_to_the_orders_the_discounts_came_off():
    """The ticket's second rule, stated separately because it is the number the
    review is checked against."""
    build(IN_YEAR)
    page = query(
        f"SELECT coalesce(sum(discount_cents), 0) FROM {MART} WHERE ds = ?",
        [IN_YEAR],
    )[0][0]
    orders = query(TIE, [IN_YEAR, IN_YEAR])[0][0]
    assert page == orders, (
        f"FAIL: {IN_YEAR}: the page adds to {page} cents and the day's orders "
        f"carry {orders} cents of discount"
    )


def test_header_and_line_add_to_the_discount_on_every_row():
    """The ticket's first rule."""
    build(EARLIER)
    broken = query(
        f"SELECT promo_id, header_cents, line_cents, discount_cents FROM {MART} "
        "WHERE ds = ? AND header_cents + line_cents <> discount_cents",
        [EARLIER],
    )
    assert not broken, f"FAIL: {EARLIER}: {len(broken)} rows do not add up: {broken[:3]}"


def test_one_row_per_promotion_a_day():
    """The grain the ticket states. A page that lands a promotion twice adds
    the promotion to itself in every reader downstream."""
    build(LAST_AUTUMN)
    doubled = query(
        f"SELECT promo_id, count(*) FROM {MART} WHERE ds = ? "
        "GROUP BY 1 HAVING count(*) > 1",
        [LAST_AUTUMN],
    )
    assert not doubled, f"FAIL: {LAST_AUTUMN}: {len(doubled)} promotions land twice"


def test_rebuilding_a_day_leaves_one_copy():
    """A page that has to be run twice must not double the day."""
    build(EARLIER)
    once = built(EARLIER)
    build(EARLIER)
    assert built(EARLIER) == once, f"FAIL: {EARLIER}: the second build changed the day"


def test_building_a_second_day_leaves_the_first_alone():
    """The build owns the day it is given. A `CREATE OR REPLACE TABLE` passes
    every test above and empties the mart every night."""
    build(IN_YEAR)
    kept = built(IN_YEAR)
    build(LAST_AUTUMN)
    assert built(IN_YEAR) == kept, (
        f"FAIL: building {LAST_AUTUMN} moved {IN_YEAR} from {len(kept)} rows "
        f"to {len(built(IN_YEAR))}"
    )


def test_the_build_reports_what_it_wrote():
    """The ticket asks for the number of rows written back out of
    `build_daily`, because the job that calls it logs that number."""
    written = build(EARLIER)
    assert written == len(built(EARLIER)), (
        f"FAIL: {EARLIER}: the build reported {written} rows and the day holds "
        f"{len(built(EARLIER))}"
    )
