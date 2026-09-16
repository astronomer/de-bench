"""MER-311 — does the SKU page carry the order-level discount, spread the way
the platform already spreads it?

Never ships to the agent. It runs beside the scored tree, with the tree root on
`PYTHONPATH`, so it imports the new module the same way a DAG would.

**No authored numbers.** Every expected result is computed from `raw.orders` and
`raw.order_lines` in the same session. The rule the oracle applies is the one
the world has written down and nothing else:
`dbt/copperline_analytics/models/shared/intermediate/int_order_lines_discounted.sql`
— pro rata by tax-exclusive line total, in whole cents, largest remainder, and
no allocation against an order whose lines sum to zero. The SQL below is that
model's arithmetic, spelled the same way, against the landed tables.

**Why a verifier rather than a DAG run.** There is no DAG. MER-311 asks for the
figure and says the wiring is the next ticket, so the graded surface is
`build_day` and the table it writes.

**Three days, on purpose.**

    2026-04-09  inside the week the ticket's buyer pulled, and a day it does not
                name
    2026-02-17  seven weeks before that week
    2026-05-19  six weeks after it, and named nowhere

A page built only for the buyers' week passes the first and fails the other two.
None of the three falls in a reserved window (2026-06-01..09, 2025-08-31..
2025-10-04) or in the never-grade tail (2026-06-10..14).

**Why the header share carries a one-cent tolerance and the day total does
not.** Largest remainder hands the leftover cents to the lines with the biggest
fraction, and two lines of one order can hold the same fraction. The shared
model breaks that by line total and then by line number; another order is
defensible, and it moves at most one cent between two SKUs of the same order.
So the per-SKU money is compared within a cent, the day's total is compared
exactly, and `test_no_graded_number_turns_on_the_tie_break` measures the
ambiguity rather than assuming it.
"""

from __future__ import annotations

import functools

import duckdb
import pytest

# The scored tree is on PYTHONPATH (scoring.build_score_env), so the house
# library resolves the warehouse the same way every DAG in the world does —
# `DUCKDB_PATH` against the root that holds `include/`, whatever the working
# directory happens to be.
from include.lib import warehouse

DB = str(warehouse.warehouse_path())
TABLE = "marts.product_margin_daily"

# Every name is qualified onto the live warehouse, for the reason
# `include/lib/warehouse.py` gives: the frozen `nwv` copy sits first on the
# search path and carries `raw.orders` and `raw.order_lines` under the same
# names, so an unqualified read would answer with September 2025's Northwave
# numbers.
ORDERS = warehouse.qualify("raw.orders")
LINES = warehouse.qualify("raw.order_lines")
MARTS_SCHEMA = warehouse.qualify("marts")
MART = warehouse.qualify(TABLE)

#: The days the page is measured on. See the module docstring.
IN_WEEK = "2026-04-09"
BEFORE = "2026-02-17"
LATER = "2026-05-19"
GRADED = (IN_WEEK, BEFORE, LATER)

#: The eight columns MER-311 names, in the order the tests read them.
COLUMNS = [
    "sku",
    "line_count",
    "gross_line_cents",
    "header_discount_cents",
    "discounted_line_cents",
    "landed_cost_cents",
    "line_margin_cents",
]

#: The money columns where the tie-break can move a cent between two SKUs of one
#: order. Compared within a cent; their day totals are still compared exactly.
WITHIN_A_CENT = ("header_discount_cents", "discounted_line_cents", "line_margin_cents")

#: The columns the tie-break cannot touch.
EXACT = ("line_count", "gross_line_cents", "landed_cost_cents")

#: The order spine: `stg_sales__orders` drops test orders and rows the OMS
#: deleted, and drops nothing else. Every population in this file is that one.
LIVE = f"""
SELECT order_id, local_order_date AS order_date, subtotal_cents, order_discount_cents
FROM {ORDERS}
WHERE local_order_date = ?
  AND NOT coalesce(is_test, false)
  AND deleted_at IS NULL
"""


def _allocation(rank_by: str) -> str:
    """The shared model's allocation, against the landed tables.

    `rank_by` is the ORDER BY that hands out the leftover cents. The shipped
    model's is the default; the second one exists only so the guard test can
    measure how much of the answer the choice decides.
    """
    return f"""
WITH live AS ({LIVE}),
joined AS (
    SELECT l.order_id,
           l.line_no,
           l.sku,
           l.qty,
           l.unit_cost_cents,
           l.line_total_cents,
           o.order_discount_cents,
           sum(l.line_total_cents) OVER (PARTITION BY l.order_id)
                                                   AS order_line_total_cents
    FROM {LINES} l
    JOIN live o ON o.order_id = l.order_id
),
floors AS (
    SELECT *,
           CASE
               WHEN order_line_total_cents > 0
               THEN cast(
                   (cast(order_discount_cents AS DECIMAL(38, 6)) * line_total_cents)
                   / order_line_total_cents
                   AS DECIMAL(38, 6))
               ELSE cast(0 AS DECIMAL(38, 6))
           END                                     AS exact_share
    FROM joined
),
remainders AS (
    SELECT *,
           cast(floor(exact_share) AS BIGINT)      AS floor_cents,
           order_discount_cents
               - sum(cast(floor(exact_share) AS BIGINT)) OVER (PARTITION BY order_id)
                                                   AS cents_to_hand_out,
           row_number() OVER (PARTITION BY order_id ORDER BY {rank_by})
                                                   AS remainder_rank
    FROM floors
),
allocated AS (
    SELECT sku,
           line_total_cents,
           CASE
               WHEN remainder_rank <= cents_to_hand_out THEN floor_cents + 1
               ELSE floor_cents
           END                                     AS header_cents,
           cast(round(cast(qty AS DECIMAL(18, 4)) * unit_cost_cents, 0) AS BIGINT)
                                                   AS extended_cost_cents
    FROM remainders
)
SELECT sku,
       count(*)::BIGINT                                        AS line_count,
       sum(line_total_cents)::BIGINT                           AS gross_line_cents,
       sum(header_cents)::BIGINT                               AS header_discount_cents,
       (sum(line_total_cents) - sum(header_cents))::BIGINT     AS discounted_line_cents,
       sum(extended_cost_cents)::BIGINT                        AS landed_cost_cents,
       (sum(line_total_cents) - sum(header_cents)
        - sum(extended_cost_cents))::BIGINT                    AS line_margin_cents
FROM allocated
GROUP BY sku
ORDER BY sku
"""


#: The shared model's own tie-break: biggest fraction first, then the bigger
#: line, then the line number.
HOUSE_RANK = "exact_share - floor(exact_share) DESC, line_total_cents DESC, line_no"

#: A defensible second one. Only the guard test uses it.
OTHER_RANK = "exact_share - floor(exact_share) DESC, line_no"

TRUTH = _allocation(HOUSE_RANK)
TRUTH_OTHER = _allocation(OTHER_RANK)

#: What the buyers' spreadsheet does: no allocation at all. Kept so the guard
#: test can prove each graded day tells the two answers apart.
SPREADSHEET = f"""
WITH live AS ({LIVE})
SELECT l.sku, sum(l.line_total_cents)::BIGINT
FROM {LINES} l
JOIN live o ON o.order_id = l.order_id
GROUP BY l.sku
ORDER BY l.sku
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
    """The mart MER-311 asks for.

    The shipped warehouse carries `raw` and `ops` and nothing above them, so
    something has to make the schema and the table. Making it here means a build
    that ships its own DDL and a build that expects the table to be there are
    both graded on the figures, which is what the ticket is about.
    """
    con = duckdb.connect(DB)
    try:
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {MARTS_SCHEMA}")
        con.execute(
            f"CREATE TABLE IF NOT EXISTS {MART} ("
            "ds DATE, sku VARCHAR, line_count BIGINT, gross_line_cents BIGINT, "
            "header_discount_cents BIGINT, discounted_line_cents BIGINT, "
            "landed_cost_cents BIGINT, line_margin_cents BIGINT)"
        )
    finally:
        con.close()


def build(ds: str) -> None:
    """Run the delivered build for one day, the way MER-312's DAG would."""
    from projects.commerce.lib import product_margin

    product_margin.build_day(ds, TABLE)


def built(ds: str) -> list[tuple]:
    columns = ", ".join(COLUMNS)
    return query(f"SELECT {columns} FROM {MART} WHERE ds = ? ORDER BY sku", [ds])


def as_rows(rows: list[tuple]) -> dict[str, dict[str, int]]:
    return {row[0]: dict(zip(COLUMNS[1:], row[1:])) for row in rows}


@functools.lru_cache(maxsize=None)
def truth(ds: str, sql: str = TRUTH) -> tuple[tuple, ...]:
    """The order book's answer for a day. Cached: it reads nothing the build
    under test can write, and a full pass asks for it five times over."""
    return tuple(query(sql, [ds]))


def check_day(ds: str) -> None:
    """One graded day: the SKU set, then every column on every SKU."""
    expected = as_rows(truth(ds))
    assert expected, f"FAIL: {ds}: no orders landed that day, so nothing is graded"

    build(ds)
    rows = built(ds)
    actual = as_rows(rows)

    assert len(rows) == len(actual), (
        f"FAIL: {ds}: the page holds {len(rows)} rows over {len(actual)} SKUs, so "
        "it is not one row per SKU"
    )

    missing = sorted(set(expected) - set(actual))
    extra = sorted(set(actual) - set(expected))
    assert not missing and not extra, (
        f"FAIL: {ds}: the page holds {len(actual)} SKUs and the order book holds "
        f"{len(expected)}; missing {missing[:5]}, unasked-for {extra[:5]}"
    )

    wrong: list[str] = []
    for sku in sorted(expected):
        want, got = expected[sku], actual[sku]
        for column in EXACT:
            if got[column] != want[column]:
                wrong.append(f"{sku}.{column} is {got[column]}, the order book says {want[column]}")
        for column in WITHIN_A_CENT:
            if got[column] is None or abs(int(got[column]) - want[column]) > 1:
                wrong.append(f"{sku}.{column} is {got[column]}, the order book says {want[column]}")
        if len(wrong) > 6:
            break
    assert not wrong, f"FAIL: {ds}: " + "; ".join(wrong[:6])

    for column in COLUMNS[1:]:
        got = sum(row[column] for row in actual.values())
        want = sum(row[column] for row in expected.values())
        assert got == want, (
            f"FAIL: {ds}: the day's {column} adds up to {got} and the order book "
            f"says {want}"
        )


def test_the_graded_days_can_tell_the_two_answers_apart():
    """The fixture's own guard, in four parts.

    Each graded day has to carry orders with a header discount, or the buyers'
    spreadsheet and a correct page agree and the day proves nothing. Each day
    also has to carry the population the order spine drops, or the filter is
    ungraded. Every order needs lines and a stated cost on each, or the oracle's
    arithmetic is standing on something the world no longer holds.
    """
    for ds in GRADED:
        book = as_rows(truth(ds))
        spreadsheet = dict(query(SPREADSHEET, [ds]))
        assert book, f"FAIL: {ds}: no orders landed that day"
        moved = [
            sku for sku in book
            if book[sku]["discounted_line_cents"] != spreadsheet.get(sku)
        ]
        assert len(moved) > 100, (
            f"FAIL: {ds}: the allocation moves only {len(moved)} SKUs off the "
            "spreadsheet's answer, so the day barely grades the ticket"
        )

        dropped = query(
            f"SELECT count(*) FILTER (WHERE coalesce(is_test, false)), "
            f"count(*) FILTER (WHERE deleted_at IS NOT NULL) "
            f"FROM {ORDERS} WHERE local_order_date = ?",
            [ds],
        )[0]
        assert dropped[0] and dropped[1], (
            f"FAIL: {ds}: the day carries {dropped[0]} test orders and "
            f"{dropped[1]} deleted ones; the spine's filter needs both to grade"
        )

        unpriced = query(
            f"""WITH live AS ({LIVE})
                SELECT count(*) FROM {LINES} l
                JOIN live o ON o.order_id = l.order_id
                WHERE l.unit_cost_cents IS NULL""",
            [ds],
        )[0][0]
        assert unpriced == 0, (
            f"FAIL: {ds}: {unpriced} lines carry no unit cost; this oracle sums "
            "the stated cost and would be grading a rule the ticket never states"
        )


def test_every_order_on_a_graded_day_carries_lines_to_allocate_against():
    """An order with no lines, or with lines summing to zero, has nowhere to put
    its discount. The shared model says such an order gets no allocation; the
    graded days hold none of them, so nothing here turns on that branch."""
    for ds in GRADED:
        stray = query(
            f"""WITH live AS ({LIVE}),
                     rolled AS (
                         SELECT o.order_id,
                                coalesce(sum(l.line_total_cents), 0) AS line_total
                         FROM live o
                         LEFT JOIN {LINES} l ON l.order_id = o.order_id
                         GROUP BY o.order_id
                     )
                SELECT count(*) FROM rolled WHERE line_total <= 0""",
            [ds],
        )[0][0]
        assert stray == 0, (
            f"FAIL: {ds}: {stray} orders have no line value to allocate against, "
            "so this day grades a branch the ticket does not state"
        )


def test_no_graded_number_turns_on_the_tie_break():
    """The shared model breaks a fraction tie by line total and then by line
    number. Another order is defensible. This measures what that choice decides
    on the graded days: it must move no SKU by more than the one cent the
    comparison allows, and must move no day total at all."""
    for ds in GRADED:
        house = as_rows(truth(ds))
        other = as_rows(truth(ds, TRUTH_OTHER))
        assert set(house) == set(other)
        for column in COLUMNS[1:]:
            gaps = [abs(house[sku][column] - other[sku][column]) for sku in house]
            assert max(gaps) <= 1, (
                f"FAIL: {ds}: the tie-break moves {column} by {max(gaps)} cents on "
                "a SKU, which is more than this file's comparison allows"
            )
            assert sum(house[sku][column] for sku in house) == sum(
                other[sku][column] for sku in other
            ), f"FAIL: {ds}: the tie-break moves the day's {column}"


def test_a_day_inside_the_week_the_buyers_pulled():
    check_day(IN_WEEK)


def test_a_day_seven_weeks_before_that_week():
    check_day(BEFORE)


def test_a_day_the_ticket_never_mentions():
    check_day(LATER)


def test_the_day_ties_to_the_order_book():
    """MER-311's first rule, written the way the ticket writes it and not the
    way the oracle computes it: the day's `discounted_line_cents` is the order
    book's own subtotal less its own discounts, over the orders the spine
    keeps."""
    for ds in (IN_WEEK, LATER):
        build(ds)
        page = query(
            f"SELECT coalesce(sum(discounted_line_cents), 0) FROM {MART} WHERE ds = ?",
            [ds],
        )[0][0]
        book = query(
            f"SELECT coalesce(sum(subtotal_cents - order_discount_cents), 0) "
            f"FROM ({LIVE})",
            [ds],
        )[0][0]
        assert page == book, (
            f"FAIL: {ds}: the page adds up to {page} cents and the order book "
            f"says the day was worth {book}"
        )


def test_rebuilding_the_same_day_leaves_one_copy():
    """MER-311's third rule. A page that has to be run twice must not double."""
    build(BEFORE)
    once = built(BEFORE)
    build(BEFORE)
    assert built(BEFORE) == once, f"FAIL: {BEFORE}: the second build changed the day"


def test_building_one_day_leaves_the_others_alone():
    """The other half of the third rule. A build that replaces the table rather
    than the day loses every day already on it."""
    build(BEFORE)
    before = built(BEFORE)
    build(LATER)
    assert built(BEFORE) == before, (
        f"FAIL: building {LATER} changed what {BEFORE} holds"
    )
    assert built(LATER), f"FAIL: {LATER}: the build wrote nothing"
