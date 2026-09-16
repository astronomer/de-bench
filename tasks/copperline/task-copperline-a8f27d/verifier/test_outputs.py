"""TAX-258 — does the working paper put the right cents on the tax mart's gap?

Never ships to the agent. It runs beside the scored tree and reads `RESPONSE.md`
from the root of it.

**No authored numbers.** Every expected result is computed from `raw.orders`,
`raw.invoices` and `raw.invoice_lines` in the same session. `marts.tax_daily` is
re-derived here from the model's own SQL, clause for clause, so the figures
follow the model rather than a reading of it.

**What the model does.**

`marts.tax_daily` builds two rollups and adds them. `order_tax` groups
`int_orders_enriched` by day, market, **channel** and currency. `invoice_tax`
groups `fct_ar_invoices` by day, market, **entity** and currency. A `days` CTE
unions the two on day, market and currency, and the final select left joins both
rollups back onto it on those three columns and sums.

Three things follow, and the ticket asks for all three.

1. **The two books are the same money.** Every one of the goods invoices carries
   an `order_id`, the link is one invoice to one order, and the invoice's line
   tax equals `raw.orders.tax_cents` on every one of them. So the trade book's
   tax is on both sides of the addition.

2. **The join fans the invoice side out.** `order_tax` holds one row per channel
   trading that day in that market and currency, and the join is on three
   columns that do not include the channel. So a day with four channels meets
   the day's one invoice row four times, and `sum(i.invoice_tax_cents)` counts
   that invoice tax four times. The order side is not fanned out in return,
   because a market has exactly one entity and `invoice_tax` has one row per
   day, market and currency.

3. **The blank-currency invoices reach no row.** `raw.invoices.currency_code` is
   NULL on the US and CA book before the currency cutover. `days` carries those
   keys, because a UNION treats two NULLs as the same key, but the left join back
   does not, because `NULL = NULL` is not true. So that invoice tax lands in the
   mart nowhere at all.

`dim_geography` is left joined on `market_code` and cannot fan anything out: it
is one row per market by construction, grouped from the market calendar. It is
left out of the emulation for that reason, and the first test asserts the market
codes it would supply are unique.

**The honest figure.** The order tax, counted once, over the order spine the
ticket names, plus the invoice tax that is nobody's order — the plan invoices.

**How the figures are read.** `RESPONSE.md` is prose, so this file takes every
number out of it and asks whether one of them lands on each expected result. It
does not care which line a figure sits on, whether it carries a sign, a comma, a
currency mark or a decimal point, or — for the money figures — whether the writer
worked in cents or in dollars. What it cares about is that the figure was
measured.
"""

from __future__ import annotations

import re

import duckdb
import pytest

# The scored tree is on PYTHONPATH (scoring.build_score_env), so the house
# library resolves the warehouse and the tree root the same way every DAG in
# the world does.
from include.lib import warehouse, workspace_root

DB = str(warehouse.warehouse_path())

# Every name is qualified onto the live warehouse, for the reason
# `include/lib/warehouse.py` gives: the frozen `nwv` copy sits first on the
# search path and carries `raw.invoices` and `raw.orders` under the same names,
# so an unqualified read would answer with the Northwave book.
ORDERS = warehouse.qualify("raw.orders")
INVOICES = warehouse.qualify("raw.invoices")
LINES = warehouse.qualify("raw.invoice_lines")
CALENDAR = warehouse.qualify("raw.market_calendar")

RESPONSE = workspace_root() / "RESPONSE.md"

#: The market-default currency table, copied from int_orders_enriched. The model
#: states it inline and nothing else in the tree holds it, so the emulation
#: states it inline too.
MARKET_CURRENCY = """
    SELECT * FROM (VALUES
        ('US','USD'),('CA','CAD'),('GB','GBP'),('IE','EUR'),('DE','EUR'),
        ('MX','MXN'),('BR','BRL'),('PL','PLN'),('ID','IDR')
    ) AS t(market_code, default_currency_code)
"""

#: The two models tax_daily reads, cut down to the columns that carry tax.
#: `int_orders_enriched` takes its orders from `stg_sales__orders`, which drops
#: the staff test rows and the soft deletes; its currency is the order's own or
#: the market default. `fct_ar_invoices` rolls the lines up per invoice.
SOURCES = f"""
int_orders_enriched AS (
    SELECT o.order_id,
           o.local_order_date                                   AS order_date,
           o.channel,
           o.market_code,
           coalesce(o.currency_code, m.default_currency_code)   AS currency_code,
           cast(o.tax_cents AS BIGINT)                          AS tax_cents
    FROM {ORDERS} o
    LEFT JOIN ({MARKET_CURRENCY}) m ON m.market_code = o.market_code
    WHERE NOT coalesce(o.is_test, FALSE) AND o.deleted_at IS NULL
),
line_rollup AS (
    SELECT invoice_id, sum(tax_cents) AS line_tax_cents
    FROM {LINES}
    GROUP BY 1
),
fct_ar_invoices AS (
    SELECT i.invoice_id,
           i.invoice_date,
           i.market_code                    AS geography_key,
           i.entity_code,
           i.currency_code                  AS currency_key,
           i.order_id,
           coalesce(l.line_tax_cents, 0)    AS line_tax_cents
    FROM {INVOICES} i
    LEFT JOIN line_rollup l ON l.invoice_id = i.invoice_id
)
"""

#: marts.tax_daily, clause for clause, tax columns only.
MART_CTES = f"""
{SOURCES},
order_tax AS (
    SELECT order_date AS ds, market_code, channel, currency_code,
           sum(tax_cents) AS order_tax_cents,
           count(*)       AS order_count
    FROM int_orders_enriched
    GROUP BY 1, 2, 3, 4
),
invoice_tax AS (
    SELECT invoice_date AS ds, geography_key AS market_code, entity_code,
           currency_key AS currency_code,
           sum(line_tax_cents) AS invoice_tax_cents,
           count(*)            AS invoice_count
    FROM fct_ar_invoices
    GROUP BY 1, 2, 3, 4
),
days AS (
    SELECT ds, market_code, currency_code FROM order_tax
    UNION
    SELECT ds, market_code, currency_code FROM invoice_tax
),
mart AS (
    SELECT d.ds, d.market_code, d.currency_code,
           coalesce(sum(o.order_tax_cents), 0)   AS order_tax_cents,
           coalesce(sum(o.order_count), 0)       AS order_count,
           coalesce(sum(i.invoice_tax_cents), 0) AS invoice_tax_cents,
           coalesce(sum(i.invoice_count), 0)     AS invoice_count,
           coalesce(sum(o.order_tax_cents), 0)
             + coalesce(sum(i.invoice_tax_cents), 0) AS tax_cents
    FROM days d
    LEFT JOIN order_tax o
           ON o.ds = d.ds AND o.market_code = d.market_code
          AND o.currency_code = d.currency_code
    LEFT JOIN invoice_tax i
           ON i.ds = d.ds AND i.market_code = d.market_code
          AND i.currency_code = d.currency_code
    GROUP BY 1, 2, 3
),
honest_orders AS (
    SELECT market_code, sum(tax_cents) AS order_tax_cents
    FROM int_orders_enriched GROUP BY 1
),
plan_tax AS (
    SELECT geography_key AS market_code, sum(line_tax_cents) AS plan_tax_cents
    FROM fct_ar_invoices WHERE order_id IS NULL GROUP BY 1
)
"""

MART_TOTALS = f"""
WITH {MART_CTES}
SELECT sum(tax_cents)          AS mart_tax,
       sum(order_tax_cents)    AS mart_order_side,
       sum(invoice_tax_cents)  AS mart_invoice_side,
       sum(order_count)        AS mart_order_count,
       sum(invoice_count)      AS mart_invoice_count,
       (SELECT sum(order_tax_cents) FROM honest_orders) AS order_tax,
       (SELECT sum(plan_tax_cents)  FROM plan_tax)      AS plan_tax
FROM mart
"""

BY_MARKET = f"""
WITH {MART_CTES}
SELECT m.market_code,
       sum(m.tax_cents)                                            AS mart_tax,
       max(h.order_tax_cents) + coalesce(max(p.plan_tax_cents), 0) AS honest_tax,
       sum(m.tax_cents)
         - (max(h.order_tax_cents) + coalesce(max(p.plan_tax_cents), 0)) AS gap
FROM mart m
LEFT JOIN honest_orders h ON h.market_code = m.market_code
LEFT JOIN plan_tax p ON p.market_code = m.market_code
GROUP BY 1
ORDER BY 4 DESC
"""

#: The invoice book on its own, split the three ways the ticket asks about.
BOOKS = f"""
WITH l AS (
    SELECT invoice_id, sum(tax_cents) AS line_tax_cents FROM {LINES} GROUP BY 1
)
SELECT count(*)                                                        AS invoices,
       sum(coalesce(l.line_tax_cents, 0))                              AS book_tax,
       count(*) FILTER (WHERE i.order_id IS NOT NULL)                  AS goods_invoices,
       sum(coalesce(l.line_tax_cents, 0))
           FILTER (WHERE i.order_id IS NOT NULL)                       AS goods_tax,
       count(*) FILTER (WHERE i.order_id IS NULL)                      AS plan_invoices,
       sum(coalesce(l.line_tax_cents, 0))
           FILTER (WHERE i.order_id IS NULL)                           AS plan_tax,
       count(*) FILTER (WHERE i.currency_code IS NULL)                 AS blank_invoices,
       sum(coalesce(l.line_tax_cents, 0))
           FILTER (WHERE i.currency_code IS NULL)                      AS blank_tax
FROM {INVOICES} i
LEFT JOIN l ON l.invoice_id = i.invoice_id
"""

#: The link between the books: one invoice, one order, the same tax.
AGAINST_ORDERS = f"""
WITH l AS (
    SELECT invoice_id, sum(tax_cents) AS line_tax_cents FROM {LINES} GROUP BY 1
)
SELECT count(*)                                                    AS goods_invoices,
       count(DISTINCT i.order_id)                                  AS orders,
       count(*) FILTER (WHERE l.line_tax_cents = o.tax_cents)      AS tax_ties,
       count(*) FILTER (WHERE i.invoice_date = o.local_order_date) AS date_ties,
       count(*) FILTER (WHERE o.channel = 'trade')                 AS trade_orders
FROM {INVOICES} i
JOIN l ON l.invoice_id = i.invoice_id
JOIN {ORDERS} o ON o.order_id = i.order_id
"""

#: dim_geography is grouped from the market calendar, so it is one row per
#: market. Asserted rather than assumed, because a second row would fan the
#: whole mart out again and this emulation leaves the join out.
GEOGRAPHY = f"""
SELECT count(*) AS rows, count(DISTINCT market_code) AS markets
FROM (SELECT market_code FROM {CALENDAR} GROUP BY 1)
"""


def query(sql: str) -> list[tuple]:
    """One read, on its own connection.

    Opened writable rather than read-only: DuckDB refuses a read-only open on a
    file that still has a write-ahead log to replay, and an agent that queried
    the warehouse while working leaves one.
    """
    con = duckdb.connect(DB)
    try:
        return con.execute(sql).fetchall()
    finally:
        con.close()


def _row(sql: str, keys: tuple[str, ...]) -> dict:
    return {key: int(value) for key, value in zip(keys, query(sql)[0])}


@pytest.fixture(scope="session")
def measured() -> dict:
    """The mart as the model would publish it, and the books underneath it."""
    mart = _row(MART_TOTALS, ("mart_tax", "mart_order_side", "mart_invoice_side",
                              "mart_order_count", "mart_invoice_count",
                              "order_tax", "plan_tax"))
    books = _row(BOOKS, ("invoices", "book_tax", "goods_invoices", "goods_tax",
                         "plan_invoices", "plan_tax", "blank_invoices", "blank_tax"))
    link = _row(AGAINST_ORDERS, ("goods_invoices", "orders", "tax_ties",
                                 "date_ties", "trade_orders"))
    geography = _row(GEOGRAPHY, ("rows", "markets"))
    markets = [(row[0], int(row[1]), int(row[2]), int(row[3])) for row in query(BY_MARKET)]
    mart["honest_tax"] = mart["order_tax"] + mart["plan_tax"]
    return {"mart": mart, "books": books, "link": link,
            "geography": geography, "markets": markets}


#: A number in prose: an optional sign and currency mark, digits that may carry
#: comma or underscore grouping, and an optional decimal part. A space is not a
#: grouping mark here on purpose — treating one as a grouping mark would glue two
#: figures written side by side into a third that is neither.
NUMBER = re.compile(r"[-+]?[$£€]?\d[\d,_]*(?:\.\d+)?")

#: A figure written with a magnitude word rather than in full.
SCALED = re.compile(r"([-+]?\d[\d,_]*(?:\.\d+)?)\s*(bn|billion|m|mn|million|k|thousand)\b",
                    re.IGNORECASE)
SCALE = {"k": 1e3, "thousand": 1e3, "m": 1e6, "mn": 1e6, "million": 1e6,
         "bn": 1e9, "billion": 1e9}


def _literals() -> list[float]:
    """Every number `RESPONSE.md` states, as a positive value, as written."""
    assert RESPONSE.exists(), (
        f"FAIL: {RESPONSE} was not written; the ticket asks for the working "
        "paper at the root of the working tree"
    )
    text = RESPONSE.read_text(encoding="utf-8", errors="replace")
    values: list[float] = []
    for match in NUMBER.finditer(text):
        cleaned = re.sub(r"[,_\s$£€  ]", "", match.group(0))
        try:
            values.append(abs(float(cleaned)))
        except ValueError:
            continue
    for match in SCALED.finditer(text):
        try:
            values.append(abs(float(match.group(1).replace(",", "").replace("_", ""))
                              * SCALE[match.group(2).lower()]))
        except ValueError:
            continue
    return values


@pytest.fixture(scope="session")
def figures() -> list[float]:
    """The money figures, in cents.

    Each literal is offered twice, once as written and once multiplied by a
    hundred, so a writer who worked in dollars is not convicted for it. Signs
    are dropped: which way round the writer put the subtraction is graded by the
    checks on the prose, not by arithmetic here.
    """
    values = _literals()
    return sorted({value for value in values} | {value * 100 for value in values})


@pytest.fixture(scope="session")
def counts() -> list[float]:
    """The figures as written, with no dollar reading.

    A count of invoices is not money, and doubling the bag would let a stray
    1,727 somewhere on the page stand in for 172,674.
    """
    return sorted(set(_literals()))


def near(values: list[float], target: int, tolerance: float) -> bool:
    """Does the working paper state a figure within `tolerance` of `target`?"""
    window = abs(target) * tolerance
    return any(abs(value - abs(target)) <= window for value in values)


def test_the_world_still_holds_the_double_count(measured):
    """The oracle checks the world before it grades anything.

    Six things have to hold or this file grades a distinction the data no longer
    makes: the two books are one to one, they carry the same tax, the mart's
    invoice side is fanned out, the blank-currency book has population, the plan
    book has population, and the mart comes out over twice the honest figure.
    """
    link = measured["link"]
    books = measured["books"]
    mart = measured["mart"]
    assert link["goods_invoices"] == books["goods_invoices"], (
        f"FAIL: {books['goods_invoices'] - link['goods_invoices']} goods invoices "
        "name an order the order feed does not hold, so the two books no longer "
        "meet one to one"
    )
    assert link["orders"] == link["goods_invoices"], (
        f"FAIL: {link['goods_invoices']} goods invoices resolve to "
        f"{link['orders']} orders, so an invoice no longer bills one order"
    )
    assert link["tax_ties"] == link["goods_invoices"], (
        f"FAIL: the invoice lines tie to raw.orders.tax_cents on only "
        f"{link['tax_ties']} of {link['goods_invoices']} goods invoices, so the "
        "two sides of the mart are no longer the same money"
    )
    assert link["date_ties"] == link["goods_invoices"], (
        f"FAIL: the invoice date is the order date on only {link['date_ties']} of "
        f"{link['goods_invoices']} goods invoices, so the double count no longer "
        "lands on one day"
    )
    assert mart["mart_invoice_side"] > books["book_tax"] * 2, (
        f"FAIL: the mart's invoice side is {mart['mart_invoice_side']} against a "
        f"book of {books['book_tax']}, so the channel fan-out this ticket is "
        "about has gone"
    )
    assert mart["mart_order_side"] == mart["order_tax"], (
        f"FAIL: the mart's order side is {mart['mart_order_side']} against "
        f"{mart['order_tax']} of order tax, so the order side is fanned out too "
        "and the ticket's account of the model is wrong"
    )
    assert books["blank_tax"] > 0 and books["plan_tax"] > 0, (
        "FAIL: the blank-currency book or the plan book has emptied, so one of "
        "the two directions the ticket asks about has no population"
    )
    assert mart["mart_tax"] > 2 * mart["honest_tax"], (
        f"FAIL: the mart comes to {mart['mart_tax']} against an honest "
        f"{mart['honest_tax']}, so the desk's 'about two and a half times' is no "
        "longer the world"
    )
    geography = measured["geography"]
    assert geography["rows"] == geography["markets"], (
        "FAIL: dim_geography would carry more than one row for a market, so it "
        "fans the mart out and this emulation is short"
    )


def test_the_mart_total_is_measured(measured, figures):
    """What the model as written would publish, over the whole range.

    A quarter of a per cent. There is no defensible variance in this figure —
    the model is a written statement and its output is one number — and the
    window is there for a writer who worked in dollars. It is deliberately
    tighter than the other money tests: reading the order spine unfiltered moves
    this figure by 0.30 per cent and the ticket names the population in as many
    words.
    """
    mart = measured["mart"]
    assert near(figures, mart["mart_tax"], 0.0025), (
        f"FAIL: the working paper states no figure within 0.25% of "
        f"{mart['mart_tax']} cents, the tax marts.tax_daily would publish over "
        f"the whole range ({mart['mart_order_side']} from the order side and "
        f"{mart['mart_invoice_side']} from the invoice side)"
    )


def test_the_honest_total_is_measured(measured, figures):
    """The output tax the estate charged, each sale counted once.

    The order tax over the spine the ticket names, plus the plan invoices, which
    are nobody's order. A writer who gives the order tax alone is 0.07 per cent
    away and passes here; the plan-invoice test is what convicts a paper that
    never separated the plan book out. Reading the spine unfiltered is 0.61 per
    cent away and does not pass.
    """
    mart = measured["mart"]
    assert near(figures, mart["honest_tax"], 0.0025), (
        f"FAIL: the working paper states no figure within 0.25% of "
        f"{mart['honest_tax']} cents, the output tax the estate charged "
        f"({mart['order_tax']} of order tax plus {mart['plan_tax']} on the plan "
        "invoices)"
    )


def test_the_overlap_between_the_books_is_measured(measured, figures):
    """The tax that sits on a goods invoice and on its order at once.

    Half a per cent, which is room for the writer who gives the invoice book
    whole rather than the goods half — the plan invoices are 0.14 per cent of it
    — and no room for a sample, a year or one market.
    """
    books = measured["books"]
    assert near(figures, books["goods_tax"], 0.005), (
        f"FAIL: the working paper states no figure within 0.5% of "
        f"{books['goods_tax']} cents, the tax carried by both the goods invoices "
        "and the orders they bill"
    )


def test_the_overlapping_invoices_are_counted(measured, counts):
    """How many invoices carry that overlap. The ticket asks for it beside the
    amount, and it is what says the overlap is the whole goods book rather than
    a handful of rows."""
    books = measured["books"]
    assert near(counts, books["goods_invoices"], 0.005), (
        f"FAIL: the working paper states no figure within 0.5% of "
        f"{books['goods_invoices']}, the invoices whose tax is also their "
        "order's"
    )


def test_the_blank_currency_tax_is_measured(measured, figures):
    """The invoice tax that reaches no row of the mart.

    The mart joins on the currency and the pre-cutover US and CA invoices carry
    none, so the left join never matches them. This is the direction that makes
    the invoice side smaller, and it is the figure a paper that only found the
    double count cannot produce.
    """
    books = measured["books"]
    assert near(figures, books["blank_tax"], 0.005), (
        f"FAIL: the working paper states no figure within 0.5% of "
        f"{books['blank_tax']} cents, the invoice tax on the "
        f"{books['blank_invoices']} invoices that carry no currency and so join "
        "no row of the mart"
    )


def test_the_plan_invoice_tax_is_measured(measured, figures):
    """The invoice tax that is genuinely ours to add: the invoices that bill no
    order. It is small, which is the point — a paper that adds the invoice book
    to the order book to get an honest figure is out by the whole goods book,
    and this is what the addition was actually worth."""
    books = measured["books"]
    assert near(figures, books["plan_tax"], 0.005), (
        f"FAIL: the working paper states no figure within 0.5% of "
        f"{books['plan_tax']} cents, the tax on the {books['plan_invoices']} "
        "invoices that bill no order"
    )


def test_the_worst_market_is_measured(measured, figures):
    """The market the mart overstates by the most, in cents.

    A writer who published the market table the ticket asks for has the
    subtraction on the page whether or not they wrote the third column, so the
    two sides of it are taken in place of the difference itself.
    """
    market, mart_tax, honest_tax, gap = measured["markets"][0]
    stated = near(figures, gap, 0.005) or (
        near(figures, mart_tax, 0.005) and near(figures, honest_tax, 0.005)
    )
    assert stated, (
        f"FAIL: the working paper states no figure within 0.5% of {gap} cents, "
        f"the amount the mart overstates the {market} book by, and does not "
        f"state its two sides ({mart_tax} and {honest_tax}) either"
    )
