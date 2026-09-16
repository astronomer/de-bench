"""DQ-96 — does the write-up put the right number on each part of the store gap?

Never ships to the agent. It runs beside the scored tree and reads `RESPONSE.md`
from the root of it.

**No authored numbers.** Every expected result is computed from
`raw.pos_sales_header` and `raw.orders` in the same session. The five figures
graded here are the ones the tie's own arithmetic produces, and they are
re-derived from the landed feeds on every run.

**What the tie compares.** `agg_daily_store_sales` rolls the till up on
`business_date` and the order spine up on `order_date`, and reports

    variance_cents = till_net_cents - (booked_cents - order_discount_cents)

on the store-days that carry both sides. `till_net_cents` is
`sum(net_cents) filter (where not is_void)` over `stg_store__pos_sales_header`,
which is `raw.pos_sales_header` unchanged. `booked_cents` is
`raw.orders.subtotal_cents` and `order_discount_cents` is its own column, over
the orders `stg_sales__orders` keeps — store channel, a store on the row, not
`is_test`, no `deleted_at`. So the whole comparison re-derives from the two raw
tables, and it does: rebuilt this way at the small profile it reproduces the
model's own row count, failing-row count and total to the cent.

**The three parts, and why they add up.** Writing T for the till transactions of
a store-day and O for the orders the spine keeps,

    variance = sum over T, not void, of (gross - discount + tax)
             - sum over O of (gross - discount)

             = TAX      the tax on the transactions the till kept, which the
                        order side of the comparison never had
             - VOID     the voided transactions, whose money the till drops and
                        the order book keeps
             + DROPPED  the orders the till has and the spine does not: the test
                        orders and the soft-deleted ones

and the identity holds exactly, which `test_the_world_still_separates_the_causes`
asserts before anything else is graded.

**What is not here.** Two of the three reasons the model header gives put nothing
into the total, and no test below looks for a figure on them, because the honest
figure is zero: the till's `business_date` and the order's `order_date` are the
same date on every store transaction there is, and neither side of the
comparison reads `event_time_utc` at all. The write-up has to say so; that is
graded by the checks on the prose, not here.

**How the figures are read.** `RESPONSE.md` is prose, so this file takes every
number out of it and asks whether one of them lands on each expected result. It
does not care which line a figure sits on, whether it carries a sign, a comma,
a currency mark or a decimal point, or whether the writer put it in cents or in
dollars. What it cares about is that the figure was measured.
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
# search path and carries `raw.orders` under the same name, so an unqualified
# read would answer with September 2025's Northwave numbers.
HEADER = warehouse.qualify("raw.pos_sales_header")
ORDERS = warehouse.qualify("raw.orders")

RESPONSE = workspace_root() / "RESPONSE.md"

#: The till and the order book, rolled up the way `agg_daily_store_sales` rolls
#: them, with the three parts of the gap carried on the row beside the gap.
TIE = f"""
WITH till AS (
    SELECT h.business_date AS ds,
           h.store_id,
           count(*)                                                     AS txn_count,
           count(*) FILTER (WHERE h.void_flag)                          AS void_count,
           coalesce(sum(h.net_cents) FILTER (WHERE NOT h.void_flag), 0) AS till_net_cents,
           coalesce(sum(h.tax_cents) FILTER (WHERE NOT h.void_flag), 0) AS kept_tax_cents,
           coalesce(sum(h.gross_cents - h.discount_cents)
                    FILTER (WHERE h.void_flag), 0)                      AS void_cents
    FROM {HEADER} h
    GROUP BY 1, 2
),
book AS (
    SELECT o.local_order_date AS ds,
           o.store_id,
           count(*)                                            AS order_count,
           sum(o.subtotal_cents - o.order_discount_cents)       AS booked_net_cents
    FROM {ORDERS} o
    WHERE o.channel = 'store' AND o.store_id IS NOT NULL
      AND NOT coalesce(o.is_test, false) AND o.deleted_at IS NULL
    GROUP BY 1, 2
),
dropped AS (
    SELECT o.local_order_date AS ds,
           o.store_id,
           count(*)                                            AS dropped_count,
           sum(o.subtotal_cents - o.order_discount_cents)       AS dropped_cents
    FROM {ORDERS} o
    WHERE o.channel = 'store' AND o.store_id IS NOT NULL
      AND (coalesce(o.is_test, false) OR o.deleted_at IS NOT NULL)
    GROUP BY 1, 2
),
day AS (
    SELECT t.ds, t.store_id, t.txn_count, t.void_count, t.kept_tax_cents,
           t.void_cents,
           coalesce(d.dropped_cents, 0)      AS dropped_cents,
           coalesce(d.dropped_count, 0)      AS dropped_count,
           t.till_net_cents - b.booked_net_cents AS variance_cents
    FROM till t
    JOIN book b ON b.ds = t.ds AND b.store_id = t.store_id
    LEFT JOIN dropped d ON d.ds = t.ds AND d.store_id = t.store_id
)
SELECT count(*)                                        AS store_days,
       count(*) FILTER (WHERE abs(variance_cents) > 100) AS failing_store_days,
       sum(variance_cents)                             AS total_cents,
       sum(kept_tax_cents)                             AS tax_cents,
       sum(void_cents)                                 AS void_cents,
       sum(dropped_cents)                              AS dropped_cents,
       sum(void_count)                                 AS void_txns,
       sum(dropped_count)                              AS dropped_orders
FROM day
"""

#: Every till transaction that carries an order id, against the order it names.
#: This is the evidence that the trading-day reason puts nothing in: the day is
#: the same date on both sides, transaction by transaction.
SAME_DAY = f"""
SELECT count(*)                                              AS matched,
       count(*) FILTER (WHERE h.business_date <> o.local_order_date) AS day_differs
FROM {HEADER} h
JOIN {ORDERS} o ON o.order_id = h.order_id
"""


def query(sql: str) -> tuple:
    """One read, on its own connection.

    Opened writable rather than read-only: DuckDB refuses a read-only open on a
    file that still has a write-ahead log to replay, and an agent that queried
    the warehouse while working leaves one.
    """
    con = duckdb.connect(DB)
    try:
        return con.execute(sql).fetchone()
    finally:
        con.close()


@pytest.fixture(scope="session")
def measured() -> dict:
    """The gap and its three parts, from the two raw feeds."""
    row = query(TIE)
    keys = ("store_days", "failing_store_days", "total", "tax", "void",
            "dropped", "void_txns", "dropped_orders")
    return {key: int(value) for key, value in zip(keys, row)}


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


@pytest.fixture(scope="session")
def figures() -> list[float]:
    """Every figure `RESPONSE.md` states, as a positive number of cents.

    Each literal is offered twice, once as written and once multiplied by a
    hundred, so a writer who worked in dollars is not convicted for it. Signs
    are dropped: which way round the writer put the subtraction is graded by
    the checks on the prose, not by arithmetic here.
    """
    assert RESPONSE.exists(), (
        f"FAIL: {RESPONSE} was not written; the ticket asks for the write-up "
        "at the root of the working tree"
    )
    text = RESPONSE.read_text(encoding="utf-8", errors="replace")
    literals: list[float] = []
    for match in NUMBER.finditer(text):
        cleaned = re.sub(r"[,_\s$£€  ]", "", match.group(0))
        try:
            literals.append(abs(float(cleaned)))
        except ValueError:
            continue
    for match in SCALED.finditer(text):
        try:
            literals.append(abs(float(match.group(1).replace(",", "").replace("_", ""))
                                * SCALE[match.group(2).lower()]))
        except ValueError:
            continue
    return sorted({value for value in literals} | {value * 100 for value in literals})


def near(figures: list[float], target: int, tolerance: float) -> bool:
    """Does the write-up state a figure within `tolerance` of `target`?"""
    window = abs(target) * tolerance
    return any(abs(value - abs(target)) <= window for value in figures)


def pair_near(figures: list[float], target: int, tolerance: float) -> bool:
    """Does it state two figures that add to `target`?

    A writer who splits a cause into its two halves has said more than the
    ticket asked for, not less. Only figures below the target itself pair, so
    the search stays over the halves of it rather than over the whole file.
    """
    window = abs(target) * tolerance
    halves = [value for value in figures if 0 < value < abs(target)]
    return any(abs(halves[i] + halves[j] - abs(target)) <= window
               for i in range(len(halves)) for j in range(i + 1, len(halves)))


def test_the_world_still_separates_the_causes(measured):
    """The oracle checks the world before it grades anything.

    Four things have to hold or this file grades a distinction the data no
    longer makes. The three parts have to be non-empty. They have to add to the
    gap exactly. The tie has to fail on the estate rather than on a tail. And
    the trading-day reason has to be the nothing the write-up is asked to show
    it is.
    """
    assert measured["tax"] > 0, "FAIL: the till carries no tax, so the gap has no tax in it"
    assert measured["void"] > 0, "FAIL: no transaction in the feed is voided"
    assert measured["dropped"] > 0, (
        "FAIL: the order spine drops no store order, so the third part of the "
        "gap is empty"
    )
    assert measured["tax"] - measured["void"] + measured["dropped"] == measured["total"], (
        "FAIL: the three parts do not add to the gap "
        f"({measured['tax']} - {measured['void']} + {measured['dropped']} "
        f"<> {measured['total']})"
    )
    assert measured["failing_store_days"] > 0.9 * measured["store_days"], (
        f"FAIL: the tie fails on {measured['failing_store_days']} of "
        f"{measured['store_days']} store-days, so it is no longer the "
        "estate-wide failure the ticket describes"
    )
    matched, day_differs = query(SAME_DAY)
    assert matched > 0, "FAIL: no till transaction carries an order id"
    assert day_differs == 0, (
        f"FAIL: {day_differs} till transactions sit on a different date from "
        "the order they name, so the trading-day reason is no longer worth "
        "nothing and the write-up should not be asked to say it is"
    )


def test_the_gap_is_measured(measured, figures):
    """The total, in cents, over the whole range.

    A tenth of a per cent either way, which is the room between the gap over
    every store-day that has both sides and the gap over the failing ones
    alone. It is not room for a sample: a week, a region or a single store is
    out by orders of magnitude, and so is the sum of the absolute variances,
    which the ticket rules out by asking for one signed total.
    """
    assert near(figures, measured["total"], 0.01), (
        f"FAIL: the write-up states no figure within 1% of {measured['total']} "
        "cents, which is the gap over the whole range"
    )


def test_the_failing_store_days_are_counted(measured, figures):
    """How many store-days the tie fails on. The ticket asks for it beside the
    total, and it is the figure that says the failure is the estate rather than
    a tail of bad nights."""
    assert near(figures, measured["failing_store_days"], 0.01), (
        f"FAIL: the write-up states no figure within 1% of "
        f"{measured['failing_store_days']}, the store-days the tie fails on"
    )


def test_the_tax_is_measured(measured, figures):
    """The part the model header never names, and the one that carries the
    money: `till_net_cents` is gross less discount PLUS tax, and the order side
    of the comparison is gross less discount and stops there.

    Three and a half per cent, which covers every population a careful reader
    might put the tax over — the transactions the till kept, every transaction
    including the voided ones, or the orders the spine kept. The three are
    within two per cent of each other and all three are a measurement.
    """
    assert near(figures, measured["tax"], 0.035), (
        f"FAIL: the write-up states no figure within 3.5% of {measured['tax']} "
        "cents, the tax the till side of the comparison carries and the order "
        "side does not"
    )


def test_the_voided_transactions_are_measured(measured, figures):
    """The one reason the header gives that does carry money: the till drops a
    voided transaction and the order book keeps it.

    Twelve per cent, because the void can be counted two ways and both are
    right. The till drops the whole transaction, tax included; the order side
    of the comparison never had the tax. Counting the void with its tax and
    counting it without differ by about a tenth, and a writer who has measured
    either has measured it.
    """
    assert near(figures, measured["void"], 0.12), (
        f"FAIL: the write-up states no figure within 12% of "
        f"{measured['void']} cents, the money the till drops for a voided "
        "transaction while the order book keeps it"
    )


def test_the_dropped_orders_are_measured(measured, figures):
    """The second part the header never names: the till holds every transaction
    the registers sent, and `stg_sales__orders` drops the test orders and the
    soft-deleted ones before the order side is summed.

    Either one figure for both, or a figure for each — the ticket asks for a
    cause to have its own line and these are two shapes of the same one, so a
    reply that splits them is not convicted for it.
    """
    assert (near(figures, measured["dropped"], 0.12)
            or pair_near(figures, measured["dropped"], 0.12)), (
        f"FAIL: the write-up states no figure within 12% of "
        f"{measured['dropped']} cents, the orders the till holds and the order "
        f"spine drops ({measured['dropped_orders']} of them)"
    )
