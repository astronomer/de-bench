"""CLS-208 — the nightly close, driven from the price book down to the publish.

Never ships to the agent. It runs beside the scored tree with the tree root on
`PYTHONPATH`, so it imports the patched `nightly_close` the way the dag
processor does and calls each step's own callable with that step's own
`op_kwargs`. The order comes off the patched graph rather than off a list here:
a repair that moves the publish above the ties is run in the order it wired.

**Why a verifier rather than a DAG run.** The close opens with four file sensors
and the landing tree only carries the last few weeks, so `airflow dags test` on
an ordinary trading day never reaches the pricing half whatever the agent does
to it. The ticket says so and says to drive the pricing half by hand.

**No authored numbers.** Every expected result is read out of `raw.orders`,
`raw.order_lines` and `raw.pim_product_versions` in the same session that runs
the steps, so an edit to the tree cannot move the answer.

**What is graded, and what deliberately is not.** `booked_cents`, the order
population, the grain and the price book — never `net_sales_cents` and never
`merch_margin_cents`. Those two come out of `close_line_economics`, which is
CLS-207's statement and outside this ticket. This file has to give the same
verdict whether or not CLS-207 has landed, and booked is the figure both tickets
agree does not move.

**Two order days.** 2026-01-14 and 2026-04-22: ordinary trading days, outside
the world's reserved windows, named nowhere in the ticket, and not the days
CLS-207 grades. Both carry staff transactions and soft-deleted orders, so both
rulings the ticket states change the answer. The guard tests say so before
anything about the answer is graded.

**Three passes and no more.** Everything the tests read is collected in one
session fixture: the close is driven over `FIRST`, over `FIRST` again and over
`SECOND`, and the two checks are then put in front of a day broken on purpose.
Driving it once per assertion would cost several more copies of a 2.7 GB
warehouse, which is what `warehouse.connect(read_only=True)` does after a write.

The state the close is given is the state the enrichment leaves: the two empty
schemas, the mart's own DDL, and `staging.enrich_base` for the date. The ticket
puts all three outside this ticket and says they are already there.
"""

from __future__ import annotations

import duckdb
import pytest

# The scored tree is on PYTHONPATH, so the house library resolves the warehouse
# the way every DAG in the world does.
from include.lib import warehouse

DB = str(warehouse.warehouse_path())

# Qualified onto the live warehouse, for the reason include/lib/warehouse.py
# gives: the frozen `nwv` copy sits first on the search path.
ORDERS = warehouse.qualify("raw.orders")
LINES = warehouse.qualify("raw.order_lines")
PIM = warehouse.qualify("raw.pim_product_versions")
STAGING = warehouse.qualify("staging")
MARTS = warehouse.qualify("marts")
MART = warehouse.qualify("marts.order_economics")
BOOK = warehouse.qualify("staging.close_price_book")
PRICED = warehouse.qualify("staging.close_priced")
ORDER_ECON = warehouse.qualify("staging.close_order_economics")
ENRICH_BASE = warehouse.qualify("staging.enrich_base")

#: The two order days the close is driven over. See the module docstring.
FIRST = "2026-01-14"
SECOND = "2026-04-22"
GRADED = (FIRST, SECOND)

#: The steps the ticket names, price book to publish. Only the membership is
#: named here; the order they run in is read off the patched graph.
CLOSE_STEPS = (
    "load_price_book",
    "apply_list_price",
    "apply_promotions",
    "apply_gift_cards",
    "check_pricing_ties",
    "build_line_economics",
    "build_order_economics",
    "check_economics_grain",
    "tie_to_orders",
    "publish_economics",
)

#: `contracts/order_economics.yml`: one row per order, six columns.
MART_DDL = f"""
CREATE TABLE IF NOT EXISTS {MART} (
    order_id VARCHAR,
    order_date DATE,
    channel VARCHAR,
    booked_cents BIGINT,
    net_sales_cents BIGINT,
    merch_margin_cents BIGINT
)
"""

#: What the enrichment leaves for the date: the day's orders, staff transactions
#: dropped, soft-deleted orders kept and flagged. Written here rather than by
#: running the tree's own `enrich_base.sql`, so that the close's population is an
#: input to this file and not something the tree under test decides.
ENRICH_BASE_SQL = f"""
CREATE OR REPLACE TABLE {ENRICH_BASE} AS
SELECT order_id, customer_ref, loyalty_id, brand, channel, store_id,
       market_code, local_order_date, event_time_utc, event_time_local,
       order_status, currency_code, fx_rate_ppm, subtotal_cents,
       order_discount_cents, tax_cents, shipping_cents, grand_total_cents,
       gift_card_applied_cents, order_ref, source_system,
       deleted_at IS NOT NULL AS deleted_at_source
FROM {ORDERS}
WHERE local_order_date = CAST(? AS DATE)
  AND NOT is_test
"""

#: The day the close owes, out of the order book. `booked_cents` is the order on
#: the order date, gross, which `int_orders_enriched` takes off `subtotal_cents`;
#: the population is `enrich_base`'s.
EXPECTED_DAY = f"""
SELECT order_id, channel, CAST(subtotal_cents AS BIGINT)
FROM {ORDERS}
WHERE local_order_date = CAST(? AS DATE) AND NOT is_test
ORDER BY order_id
"""

PUBLISHED_DAY = f"""
SELECT order_id, channel, CAST(booked_cents AS BIGINT)
FROM {MART}
WHERE order_date = CAST(? AS DATE)
ORDER BY order_id
"""


def query(sql: str, params: list | None = None) -> list[tuple]:
    """One statement on its own connection. DuckDB takes one writer and the
    steps under test open their own, so nothing here may hold the file."""
    con = duckdb.connect(DB)
    try:
        return con.execute(sql, params or []).fetchall()
    finally:
        con.close()


def one(sql: str, params: list | None = None):
    return query(sql, params)[0][0]


def module():
    """The patched DAG module, imported the way the dag processor imports it."""
    from projects.commerce.dags import nightly_close

    return nightly_close


def ordered_steps() -> list[str]:
    """The graded steps in the order the patched graph runs them.

    A step that reaches more of the other graded steps runs earlier. Two that
    reach neither are independent of each other, so the order between them
    cannot matter.
    """
    tasks = module().dag.task_dict
    present = [t for t in CLOSE_STEPS if t in tasks]

    def below(task_id: str) -> set[str]:
        seen: set[str] = set()
        stack = [task_id]
        while stack:
            for nxt in tasks[stack.pop()].downstream_task_ids:
                if nxt not in seen:
                    seen.add(nxt)
                    stack.append(nxt)
        return seen

    reach = {t: below(t) & set(present) for t in present}
    return sorted(present, key=lambda t: (-len(reach[t]), t))


def call(task_id: str, ds: str):
    """One step, through its own callable and its own kwargs, with the date
    filled in wherever the graph templates it."""
    task = module().dag.task_dict[task_id]
    kwargs = {
        key: (ds if isinstance(value, str) and "{{" in value else value)
        for key, value in (task.op_kwargs or {}).items()
    }
    return task.python_callable(**kwargs)


def probe(task_id: str, ds: str) -> tuple[str, object]:
    """One step, and whether it stopped the close or came back with a number."""
    try:
        return ("returned", call(task_id, ds))
    except Exception as exc:  # noqa: BLE001 — whether it raised is the point
        return ("raised", repr(exc))


def close(ds: str) -> None:
    """The pricing half for one date, as the graph would run it."""
    query(ENRICH_BASE_SQL, [ds])
    for task_id in ordered_steps():
        call(task_id, ds)


def published(ds: str) -> list[tuple]:
    return query(PUBLISHED_DAY, [ds])


def expected(ds: str) -> list[tuple]:
    return query(EXPECTED_DAY, [ds])


def book_rows() -> list[tuple]:
    return query(f"SELECT sku, list_price_cents FROM {BOOK}")


def feed_prices(ds: str) -> set[tuple]:
    """Every (SKU, price) the PIM feed had stamped by the close date."""
    return set(
        query(
            f"""SELECT sku, list_price_cents FROM {PIM}
                WHERE CAST(updated_at AS DATE) <= CAST(? AS DATE)""",
            [ds],
        )
    )


def grain(ds: str) -> tuple:
    return query(
        f"""SELECT count(*), count(DISTINCT order_id) FROM {MART}
            WHERE order_date = CAST(? AS DATE)""",
        [ds],
    )[0]


@pytest.fixture(scope="session")
def closed():
    """Drive the close three times and keep what each pass left behind."""
    con = duckdb.connect(DB)
    try:
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {STAGING}")
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {MARTS}")
        con.execute(MART_DDL)
        con.execute(f"DELETE FROM {MART}")
    finally:
        con.close()

    out: dict = {}

    # Pass one. The date has never been closed before, which is the state every
    # date in this warehouse is in.
    close(FIRST)
    out["first_pass"] = published(FIRST)
    out["book"] = {FIRST: book_rows()}

    # The pricing tie, on a whole day and then on the same day with one line
    # moved by a cent. The bump is undone by the next pass, which rebuilds
    # `staging.close_priced` before it reaches the tie.
    out["pricing_clean"] = probe("check_pricing_ties", FIRST)
    victim_line = one(f"SELECT order_id FROM {PRICED} ORDER BY order_id LIMIT 1")
    query(
        f"UPDATE {PRICED} SET line_total_cents = line_total_cents + 1 WHERE order_id = ?",
        [victim_line],
    )
    out["pricing_broken"] = probe("check_pricing_ties", FIRST)
    out["victim_line"] = victim_line

    # Pass two: the same date again.
    close(FIRST)
    out["second_pass"] = published(FIRST)

    # Pass three: a different date, fourteen weeks later.
    close(SECOND)
    out["second_day"] = published(SECOND)
    out["first_after_second"] = published(FIRST)
    out["book"][SECOND] = book_rows()
    out["grain"] = {ds: grain(ds) for ds in GRADED}

    # The grain check and the revenue tie, in front of a day broken on purpose.
    # The break is made in the build and in the mart, so a check that reads
    # either one has to see it.
    victim = one(f"SELECT order_id FROM {ORDER_ECON} ORDER BY order_id LIMIT 1")
    query(f"INSERT INTO {ORDER_ECON} SELECT * FROM {ORDER_ECON} WHERE order_id = ?", [victim])
    query(f"INSERT INTO {MART} SELECT * FROM {MART} WHERE order_id = ?", [victim])
    out["grain_doubled"] = probe("check_economics_grain", SECOND)

    query(f"DELETE FROM {ORDER_ECON} WHERE order_id = ?", [victim])
    query(f"DELETE FROM {MART} WHERE order_id = ?", [victim])
    out["tie_short"] = probe("tie_to_orders", SECOND)
    out["victim"] = victim
    return out


# --- the world, before anything about the answer is graded -------------------


@pytest.mark.parametrize("ds", GRADED)
def test_the_header_subtotal_is_still_the_lines_total(ds: str):
    """What makes both of the close's ties well defined.

    `booked_cents` is `subtotal_cents`, and the close builds it by adding up
    `line_total_cents`. If the order book ever stops agreeing with itself about
    that, everything below is grading something else, and this says so first.
    """
    broken = one(
        f"""SELECT count(*) FROM (
                SELECT o.order_id
                FROM {ORDERS} o
                JOIN {LINES} l ON l.order_id = o.order_id
                WHERE o.local_order_date = CAST(? AS DATE)
                GROUP BY o.order_id, o.subtotal_cents, o.tax_cents
                HAVING sum(l.line_total_cents) <> o.subtotal_cents
                    OR sum(l.tax_cents) <> o.tax_cents)""",
        [ds],
    )
    assert broken == 0, (
        f"FAIL: {ds}: {broken} order(s) no longer equal their own lines; the seam "
        "this task grades is gone"
    )


@pytest.mark.parametrize("ds", GRADED)
def test_the_rulings_the_ticket_states_still_move_the_day(ds: str):
    """The day carries staff transactions and soft-deleted orders, and its grand
    total is not its booked total. Without all three the ticket's rulings grade
    nothing and a wrong reading passes by luck."""
    tests, deleted, booked, grand_total = query(
        f"""SELECT count(*) filter (WHERE is_test),
                   count(*) filter (WHERE deleted_at IS NOT NULL AND NOT is_test),
                   sum(subtotal_cents) filter (WHERE NOT is_test),
                   sum(grand_total_cents) filter (WHERE NOT is_test)
            FROM {ORDERS} WHERE local_order_date = CAST(? AS DATE)""",
        [ds],
    )[0]
    assert tests > 0, f"FAIL: {ds} holds no staff transactions; the population ruling grades nothing"
    assert deleted > 0, f"FAIL: {ds} holds no soft-deleted orders; R-6 grades nothing"
    assert booked != grand_total, f"FAIL: {ds}: booked and grand total agree at {booked}"


@pytest.mark.parametrize("ds", GRADED)
def test_the_pim_feed_can_still_price_the_day(ds: str):
    """The feed the price book has to come off carries prices stamped on or
    before the close date, for enough SKUs to grade."""
    skus = one(
        f"""SELECT count(DISTINCT sku) FROM {PIM}
            WHERE operation = 'upsert'
              AND CAST(updated_at AS DATE) <= CAST(? AS DATE)""",
        [ds],
    )
    assert skus >= 1500, f"FAIL: {ds}: the feed prices {skus} SKUs; too few to grade a book"


# --- the answer ---------------------------------------------------------------


@pytest.mark.parametrize("ds", GRADED)
def test_the_close_publishes_the_day_the_order_book_says(ds: str, closed):
    """The whole ticket, at order grain.

    The orders the ticket rules are the close's — staff transactions out,
    soft-deleted orders in, per R-6 — and no others; and `booked_cents` is the
    order on the order date, gross.
    """
    got = closed["first_after_second"] if ds == FIRST else closed["second_day"]
    want = expected(ds)
    assert len(got) == len(want), (
        f"FAIL: {ds}: the close published {len(got)} rows and the order book holds "
        f"{len(want)} orders for the date"
    )
    wrong = [(g, w) for g, w in zip(got, want) if g != w]
    assert not wrong, (
        f"FAIL: {ds}: {len(wrong)} row(s) disagree with the order book "
        f"(published, expected): {wrong[:5]}"
    )


@pytest.mark.parametrize("ds", GRADED)
def test_the_published_day_is_one_row_an_order(ds: str, closed):
    """`contracts/order_economics.yml`, and `contracts/merch-dashboards.md`: two
    dashboards reconcile a total at order grain and a doubled day still
    renders."""
    rows, orders = closed["grain"][ds]
    assert rows == orders, f"FAIL: {ds}: {rows} rows over {orders} orders in the mart"


def test_closing_a_date_twice_leaves_the_day_it_left_the_first_time(closed):
    """`CONVENTIONS.md`, "Writing to the warehouse", rule 2. The close is rerun
    by hand far more often than it is scheduled, and a bare INSERT puts the day
    in again every time."""
    assert closed["second_pass"] == closed["first_pass"], (
        f"FAIL: {FIRST}: the first pass published {len(closed['first_pass'])} rows "
        f"and the second left {len(closed['second_pass'])}"
    )


def test_closing_one_date_leaves_the_other_alone(closed):
    """A publish that clears the table rather than the date passes the rerun test
    above and loses every day already closed."""
    assert closed["first_after_second"] == closed["first_pass"], (
        f"FAIL: closing {SECOND} left {len(closed['first_after_second'])} rows of "
        f"{FIRST}, which had {len(closed['first_pass'])}"
    )


@pytest.mark.parametrize("ds", GRADED)
def test_the_price_book_is_priced_as_at_the_close_date(ds: str, closed):
    """`staging.close_price_book`: one row per SKU, priced as at the close date.

    The book has to come out of the step with a list price on it. Every price it
    carries has to be one the PIM feed stamped for that SKU on or before the
    close date, which is what convicts a book built from today's price — the
    dimension the statement used to read keeps one row per SKU and no history at
    all. Which of the feed's rows wins on a day is not graded: the tie-break is
    spelled twice under `dbt/` and the two spellings agree on this data.
    """
    rows = closed["book"][ds]
    skus = {sku for sku, _ in rows}
    assert len(rows) == len(skus), (
        f"FAIL: {ds}: the book holds {len(rows)} rows over {len(skus)} SKUs; every "
        "join to it fans the priced lines out"
    )
    priced = [(sku, price) for sku, price in rows if price is not None]
    assert len(priced) >= 1500, (
        f"FAIL: {ds}: the book prices {len(priced)} SKUs and the feed can price at "
        "least 1500 by that date"
    )
    stamped = feed_prices(ds)
    strays = [row for row in priced if row not in stamped]
    assert not strays, (
        f"FAIL: {ds}: {len(strays)} SKU(s) are priced at a figure the feed had not "
        f"stamped for them by that date (sku, price): {strays[:5]}"
    )


def test_the_pricing_tie_can_tell_a_broken_order_from_a_whole_one(closed):
    """`check_pricing_ties`, on a whole day and then on the same day with one
    line moved by a cent.

    The shipped statement has never come back at nought: it held a tax-exclusive
    line total against a tax-inclusive header figure and took the order-level
    discount off one side only. A repair that reaches nought by counting nothing
    passes the first half of this and fails the second.
    """
    clean = closed["pricing_clean"]
    broken = closed["pricing_broken"]
    # A whole day must come back, not raise. What it returns is the repair's
    # choice: the break count at nought, None, or the rows it checked — a trace
    # audit found a fair repair that raises on any break and returns the checked
    # count, which the first version of this test convicted for being nonzero.
    assert clean[0] == "returned", (
        f"FAIL: {FIRST}: the pricing tie came back {clean[0]} {clean[1]!r} on a day "
        "whose lines add up to their headers"
    )
    # What still convicts the shipped tie: it returns a huge false-break count on
    # a whole day AND sails on through the broken one. A repair earns the pass by
    # reading nought when the day is whole, or by stopping when it is not.
    assert (not clean[1]) or broken[0] == "raised", (
        f"FAIL: {FIRST}: the pricing tie counts {clean[1]!r} on a whole day and a "
        f"broken day does not stop it — that is the shipped behaviour, not a repair"
    )
    caught = broken[0] == "raised" or (broken[0] == "returned" and broken[1])
    assert caught, (
        f"FAIL: {FIRST}: a line on order {closed['victim_line']} was moved by a cent "
        f"and the pricing tie came back {broken[0]} {broken[1]!r}"
    )


def test_the_grain_check_and_the_revenue_tie_still_stop_the_close(closed):
    """Both checks, in front of a day broken on purpose.

    Deleting a check to get a green run is the cheapest wrong answer there is,
    and none of the tests above can tell a gutted check from a working one.
    """
    doubled = closed["grain_doubled"]
    assert doubled[0] == "raised", (
        f"FAIL: {SECOND}: order {closed['victim']} is in the day twice and the grain "
        f"check came back {doubled[0]} {doubled[1]!r}"
    )
    short = closed["tie_short"]
    assert short[0] == "raised", (
        f"FAIL: {SECOND}: order {closed['victim']} is missing from the day and the "
        f"revenue tie came back {short[0]} {short[1]!r}"
    )
