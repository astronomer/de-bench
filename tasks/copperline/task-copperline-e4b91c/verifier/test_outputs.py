"""TAX-241 — does the working paper put the right cents on the tax gap?

Never ships to the agent. It runs beside the scored tree and reads `RESPONSE.md`
from the root of it.

**No authored numbers.** Every expected result is computed from `raw.invoices`,
`raw.invoice_lines` and `raw.orders` in the same session. The six figures graded
here are the ones the AR export's own arithmetic produces, and they are
re-derived from the landed feeds on every run.

**What the two numbers are.**

`raw.invoices.tax_cents` is a flat rate. On a goods invoice it is
`round(total_cents * 0.0833)`; on a plan invoice it is
`round(net_cents * 0.0833)`. One of the two holds on every row of the table,
in every market and in every year, so the header carries no tax calculation at
all — it carries one twelfth of the invoice, rounded.

`raw.invoice_lines.tax_cents` is the tax the order charged. It sums, invoice by
invoice, to `raw.orders.tax_cents` on every goods invoice there is, and the
per-market effective rates it produces are the market rates: about 5.4 per cent
across the US book, 20 per cent on a British line, 23 on an Irish one. That is
the corroboration the ticket asks for, and it comes from a feed the AR export
does not write.

So the group difference is one flat rate standing in for nine, and it does not
fail in one direction: the US book is overstated by more than the group
difference, and Canada, Great Britain, Ireland, Germany, Brazil, Mexico and
Poland are all understated. A working paper that gives the group figure and
stops has hidden the larger half of the error.

**The residual.** Replace the header tax with the line tax and the header still
does not equal its own lines. `raw.invoices.total_cents` is
`raw.orders.grand_total_cents`, which is net of the order-level discount, and no
invoice line carries that discount. The gap is `raw.orders.order_discount_cents`
to the cent, on the invoices that have one.

**How the figures are read.** `RESPONSE.md` is prose, so this file takes every
number out of it and asks whether one of them lands on each expected result. It
does not care which line a figure sits on, whether it carries a sign, a comma, a
currency mark or a decimal point, or — for the money figures — whether the
writer worked in cents or in dollars. What it cares about is that the figure was
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
# search path and carries `raw.invoices` under the same name, so an unqualified
# read would answer with 114,031 Northwave invoices instead of this book.
INVOICES = warehouse.qualify("raw.invoices")
LINES = warehouse.qualify("raw.invoice_lines")
ORDERS = warehouse.qualify("raw.orders")

RESPONSE = workspace_root() / "RESPONSE.md"

#: The header against its own lines, over the whole export. `residual` is what
#: the header total leaves after the lines and the tax the lines charged, and it
#: is negative because the header is the smaller side.
BOOK = f"""
WITH l AS (
    SELECT invoice_id,
           sum(tax_cents)        AS line_tax_cents,
           sum(line_total_cents) AS line_net_cents
    FROM {LINES}
    GROUP BY 1
)
SELECT count(*)                                                    AS invoices,
       sum(v.tax_cents)                                            AS header_tax,
       sum(l.line_tax_cents)                                       AS line_tax,
       sum(v.tax_cents) - sum(l.line_tax_cents)                    AS difference,
       sum(v.total_cents - l.line_net_cents - l.line_tax_cents)    AS residual,
       count(*) FILTER (
           WHERE v.total_cents <> l.line_net_cents + l.line_tax_cents
       )                                                           AS residual_invoices
FROM {INVOICES} v
JOIN l ON l.invoice_id = v.invoice_id
"""

#: The same, cut by market, worst overstatement first.
BY_MARKET = f"""
WITH l AS (
    SELECT invoice_id,
           sum(tax_cents)        AS line_tax_cents,
           sum(line_total_cents) AS line_net_cents
    FROM {LINES}
    GROUP BY 1
)
SELECT v.market_code,
       sum(v.tax_cents)                          AS header_tax,
       sum(l.line_tax_cents)                     AS line_tax,
       sum(v.tax_cents) - sum(l.line_tax_cents)  AS difference
FROM {INVOICES} v
JOIN l ON l.invoice_id = v.invoice_id
GROUP BY 1
ORDER BY 4 DESC
"""

#: The flat rate, counted. Either base holds on every row or the header is no
#: longer the plug this ticket is about.
RULE = f"""
SELECT count(*)                                                            AS invoices,
       count(*) FILTER (WHERE tax_cents = round(total_cents * 0.0833))     AS on_total,
       count(*) FILTER (WHERE tax_cents = round(net_cents * 0.0833))       AS on_net
FROM {INVOICES}
"""

#: The corroboration, and the source of the residual. Both come off the order
#: feed, which the AR export does not write.
AGAINST_ORDERS = f"""
WITH l AS (
    SELECT invoice_id, sum(tax_cents) AS line_tax_cents
    FROM {LINES}
    GROUP BY 1
)
SELECT count(*)                                                        AS goods_invoices,
       count(*) FILTER (WHERE l.line_tax_cents = o.tax_cents)          AS tax_ties,
       count(*) FILTER (WHERE v.total_cents = o.grand_total_cents)     AS total_ties,
       sum(o.order_discount_cents)                                     AS order_discount
FROM {INVOICES} v
JOIN l ON l.invoice_id = v.invoice_id
JOIN {ORDERS} o ON o.order_id = v.order_id
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


@pytest.fixture(scope="session")
def measured() -> dict:
    """The two totals, the difference, the residual and the market split."""
    keys = ("invoices", "header_tax", "line_tax", "difference", "residual",
            "residual_invoices")
    book = {key: int(value) for key, value in zip(keys, query(BOOK)[0])}
    markets = [(row[0], int(row[1]), int(row[2]), int(row[3]))
               for row in query(BY_MARKET)]
    rule = {key: int(value) for key, value
            in zip(("invoices", "on_total", "on_net"), query(RULE)[0])}
    orders = {key: int(value) for key, value
              in zip(("goods_invoices", "tax_ties", "total_ties", "order_discount"),
                     query(AGAINST_ORDERS)[0])}
    return {**book, "markets": markets, "rule": rule, "orders": orders}


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

    A count of invoices is not money, and doubling the bag would let a stray 381
    somewhere on the page stand in for 38,108.
    """
    return sorted(set(_literals()))


def near(values: list[float], target: int, tolerance: float) -> bool:
    """Does the working paper state a figure within `tolerance` of `target`?"""
    window = abs(target) * tolerance
    return any(abs(value - abs(target)) <= window for value in values)


def test_the_world_still_holds_the_two_numbers_apart(measured):
    """The oracle checks the world before it grades anything.

    Five things have to hold or this file grades a distinction the data no
    longer makes: the header is a flat rate on every row, the lines are the
    charge the order feed corroborates, the header total is the order's grand
    total, the error runs both ways across the markets, and the residual is the
    order-level discount.
    """
    rule = measured["rule"]
    assert rule["on_total"] + rule["on_net"] == rule["invoices"], (
        f"FAIL: {rule['invoices'] - rule['on_total'] - rule['on_net']} invoices "
        "carry a header tax that is neither 8.33% of the total nor 8.33% of the "
        "net, so the header is no longer the flat rate this ticket is about"
    )
    assert rule["on_total"] > 0 and rule["on_net"] > 0, (
        "FAIL: one of the two books has gone, so the plan invoices no longer "
        "make the point the ticket asks about"
    )
    orders = measured["orders"]
    assert orders["tax_ties"] == orders["goods_invoices"], (
        f"FAIL: the invoice lines tie to raw.orders.tax_cents on only "
        f"{orders['tax_ties']} of {orders['goods_invoices']} goods invoices, so "
        "the order feed no longer corroborates the line detail"
    )
    assert orders["total_ties"] == orders["goods_invoices"], (
        f"FAIL: the header total is raw.orders.grand_total_cents on only "
        f"{orders['total_ties']} of {orders['goods_invoices']} goods invoices"
    )
    assert measured["residual"] == -orders["order_discount"], (
        f"FAIL: the residual is {measured['residual']} and the order-level "
        f"discount is {orders['order_discount']}, so the two are no longer the "
        "same money"
    )
    assert measured["residual_invoices"] > 0, (
        "FAIL: no invoice carries a residual, so there is nothing left over to "
        "explain"
    )
    over = [row for row in measured["markets"] if row[3] > 0]
    under = [row for row in measured["markets"] if row[3] < 0]
    assert over and under, (
        "FAIL: the flat rate no longer overstates one market and understates "
        "another, so the market split has stopped being the finding"
    )
    assert over[0][3] > measured["difference"], (
        f"FAIL: the worst overstated market is {over[0][3]} cents against a "
        f"group difference of {measured['difference']}, so the cancellation the "
        "ticket asks about is gone"
    )


def test_the_header_tax_is_measured(measured, figures):
    """The tax the headers say, over the whole export.

    Half a per cent, which is room for the writer who gives the goods book on
    its own — the plan invoices are 0.12 per cent of it — and no room at all for
    a sample, a year or one market.
    """
    assert near(figures, measured["header_tax"], 0.005), (
        f"FAIL: the working paper states no figure within 0.5% of "
        f"{measured['header_tax']} cents, the tax the invoice headers say"
    )


def test_the_line_tax_is_measured(measured, figures):
    """The tax the lines say, over the whole export. This is the number that
    goes on the working paper, and it is the one the order feed agrees with."""
    assert near(figures, measured["line_tax"], 0.005), (
        f"FAIL: the working paper states no figure within 0.5% of "
        f"{measured['line_tax']} cents, the tax the invoice lines say"
    )


def test_the_difference_is_measured(measured, figures):
    """The subtraction the ticket asks for in as many words. It is the same
    figure whether the writer took the whole export or the goods book alone,
    because the plan invoices carry the flat rate on both sides and cancel."""
    assert near(figures, measured["difference"], 0.005), (
        f"FAIL: the working paper states no figure within 0.5% of "
        f"{measured['difference']} cents, the difference between the two totals"
    )


def test_the_worst_market_is_measured(measured, figures):
    """The market the flat rate overstates by the most, in cents.

    A writer who published the market table the ticket asks for has the
    subtraction on the page whether or not they wrote the third column, so the
    two sides of it are taken in place of the difference itself.
    """
    market, header_tax, line_tax, difference = measured["markets"][0]
    stated = near(figures, difference, 0.005) or (
        near(figures, header_tax, 0.005) and near(figures, line_tax, 0.005)
    )
    assert stated, (
        f"FAIL: the working paper states no figure within 0.5% of {difference} "
        f"cents, the amount the headers overstate the {market} book by, and "
        f"does not state its two sides ({header_tax} and {line_tax}) either"
    )


def test_the_residual_is_measured(measured, figures):
    """What is left when the right tax is on the invoice: the order-level
    discount, which reaches no invoice line.

    Not the gap between the header net and the line net, which is this figure
    plus the tax difference and answers a question the ticket did not ask.
    """
    assert near(figures, measured["residual"], 0.005), (
        f"FAIL: the working paper states no figure within 0.5% of "
        f"{abs(measured['residual'])} cents, the money on the invoice header "
        "that reaches no invoice line"
    )


def test_the_residual_invoices_are_counted(measured, counts):
    """How many invoices carry that residual. The ticket asks for it beside the
    amount, and it is the figure that says the residual is a population rather
    than a rounding tail."""
    assert near(counts, measured["residual_invoices"], 0.005), (
        f"FAIL: the working paper states no figure within 0.5% of "
        f"{measured['residual_invoices']}, the invoices whose header does not "
        "come to its own lines once the right tax is on it"
    )
