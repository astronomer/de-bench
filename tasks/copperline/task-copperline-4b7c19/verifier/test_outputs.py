"""PAY-233 — does the day's attribution table name the order that paid?

Never ships to the agent. It runs beside the scored tree, with the tree root on
`PYTHONPATH`, so it imports the patched module the same way the DAG does.

**Why a verifier rather than a DAG run.** The subject is one query. Standing the
DAG up buys nothing the direct call does not, and it costs a scheduler. The
verifier calls `attribution.build_daily(ds)` itself, which also removes the
shortcut: a day somebody typed into the table by hand is a day the next build
replaces before anything is read.

**No authored numbers.** Every expected result is computed from
`raw.pay_meridian_settlements`, `raw.payment_intents` and `raw.orders` in the
same session. The three of them are read-only landing tables, so a tree edit
cannot move the answer.

**Three days, and the ticket names none of them.**

    2025-12-09   refunds and chargebacks on the day
    2026-01-22   the same, a quarter later
    2026-04-14   the same again, and the widest gap between the two answers

Every one of them sits outside the world's reserved windows and outside the
processor overlap quarter, so nothing here depends on which processor was
authoritative.

**What the four queries below are for.** `TRUTH` is the answer. The other three
are the answers the reference invites, kept here so the first test can prove
each graded day can tell them apart. See `checks.yaml` for what convicts each.
"""

from __future__ import annotations

import duckdb
import pytest

# The scored tree is on PYTHONPATH (scoring.build_score_env), so the house
# library resolves the warehouse the way every DAG in the world does —
# `DUCKDB_PATH` against the root that holds `include/`, whatever the working
# directory happens to be.
from include.lib import warehouse

DB = str(warehouse.warehouse_path())

# Every name is qualified onto the live warehouse, for the reason
# `include/lib/warehouse.py` gives: the frozen `nwv` copy sits first on the
# search path and carries `raw.orders` under the same name, so an unqualified
# read would answer with September 2025's Northwave numbers.
ORDERS = warehouse.qualify("raw.orders")
INTENTS = warehouse.qualify("raw.payment_intents")
EVENTS = warehouse.qualify("raw.pay_meridian_settlements")
TARGET = warehouse.qualify("marts.settlement_attribution")

#: The days the build is measured on. See the module docstring.
GRADED = ("2025-12-09", "2026-01-22", "2026-04-14")

#: The six columns the ticket asks for, in the order everything here compares
#: them in. A build that carries more columns than these is not convicted for
#: it; a build missing one of them fails to read at all.
COLUMNS = "event_id, order_id, event_type, amount_cents"

#: The answer. The event's `intent_id` is exact — one intent per attempt — and
#: the attempt reaches its order on the reference plus the attempt's own clock,
#: which is the day the order was placed. Seven days either side is
#: `int_orders_enriched`'s window and is well inside the fifty-day recycling
#: cadence, so exactly one order answers.
TRUTH = f"""
SELECT s.event_id, o.order_id, s.event_type, s.amount_cents
FROM {EVENTS} s
JOIN {INTENTS} i ON i.intent_id = s.intent_id
JOIN {ORDERS} o ON o.order_ref = i.order_ref
 AND i.created_at::DATE BETWEEN o.local_order_date - 7 AND o.local_order_date + 7
WHERE s.event_time_utc::DATE = ?
"""

#: The reference joined straight to the order. Seven digits over the order key
#: recycle every fifty days, so one event matches about seven orders.
REF_ONLY = f"""
SELECT s.event_id, o.order_id, s.event_type, s.amount_cents
FROM {EVENTS} s
JOIN {ORDERS} o ON o.order_ref = s.order_ref
WHERE s.event_time_utc::DATE = ?
"""

#: The repair that fixes the fan-out and keeps the error: the reference, then
#: the order nearest the EVENT's own date. Right for an authorization, which
#: happens the day of the order. Wrong for every refund and every chargeback,
#: which happen weeks later, and wrong in a way that conserves the day's row
#: count and the day's money to the cent.
NEAREST = f"""
SELECT event_id, order_id, event_type, amount_cents FROM (
  SELECT s.event_id, o.order_id, s.event_type, s.amount_cents,
         row_number() OVER (
             PARTITION BY s.event_id
             ORDER BY abs(date_diff('day', o.local_order_date,
                                    s.event_time_utc::DATE)), o.order_id) AS pick
  FROM {EVENTS} s
  JOIN {ORDERS} o ON o.order_ref = s.order_ref
  WHERE s.event_time_utc::DATE = ?
) WHERE pick = 1
"""

#: The world's own order-to-payment link, reused: `int_orders_enriched` picks
#: ONE attempt per order and hangs the payment off it. Every event belonging to
#: another attempt of a retried order falls out.
BEST_INTENT = f"""
WITH candidate AS (
  SELECT o.order_id, i.intent_id,
         row_number() OVER (
             PARTITION BY o.order_id
             ORDER BY abs(date_diff('day', o.local_order_date, i.created_at::DATE)),
                      CASE i.outcome WHEN 'succeeded' THEN 0 ELSE 1 END,
                      i.attempt_no) AS pick
  FROM {ORDERS} o
  JOIN {INTENTS} i ON i.order_ref = o.order_ref
   AND i.created_at::DATE BETWEEN o.local_order_date - 7 AND o.local_order_date + 7
)
SELECT s.event_id, c.order_id, s.event_type, s.amount_cents
FROM candidate c
JOIN {EVENTS} s ON s.intent_id = c.intent_id
WHERE c.pick = 1 AND s.event_time_utc::DATE = ?
"""

FEED_COUNT = f"SELECT count(*) FROM {EVENTS} WHERE event_time_utc::DATE = ?"


def query(sql: str, params: list | None = None) -> list[tuple]:
    """One read, on its own connection. DuckDB takes one writer and the build
    under test opens its own, so nothing here may hold the file open."""
    con = duckdb.connect(DB)
    try:
        return con.execute(sql, params or []).fetchall()
    finally:
        con.close()


def rows(sql: str, ds: str) -> set[tuple]:
    return {tuple(row) for row in query(sql, [ds])}


def build(ds: str) -> int:
    """Run the patched build for one day, the way `build_attribution` does."""
    from projects.commerce.lib import attribution

    return attribution.build_daily(ds)


def built(ds: str) -> set[tuple]:
    return {
        tuple(row)
        for row in query(f"SELECT {COLUMNS} FROM {TARGET} WHERE ds = ?", [ds])
    }


def blame(ds: str, actual: set[tuple], expected: set[tuple]) -> str:
    wrong = sorted(actual - expected)[:3]
    lost = sorted(expected - actual)[:3]
    return (
        f"FAIL: {ds}: {len(actual - expected)} rows the order spine does not "
        f"have and {len(expected - actual)} of its rows missing. "
        f"first wrong {wrong}; first missing {lost}"
    )


def check_day(ds: str) -> None:
    expected = rows(TRUTH, ds)
    assert expected, f"FAIL: {ds}: no settlement events that day, so nothing is graded"
    build(ds)
    actual = built(ds)
    assert actual == expected, blame(ds, actual, expected)


def test_each_graded_day_can_tell_the_answers_apart():
    """The fixture's own guard.

    A graded day is only worth grading if the reference-joined answers differ
    from the bridge's on it. If the world ever stops recycling the reference,
    or stops retrying payments, this fails here rather than passing a task that
    measures nothing.
    """
    for ds in GRADED:
        truth = rows(TRUTH, ds)
        assert truth, f"FAIL: {ds}: no settlement events that day"
        assert len(rows(REF_ONLY, ds)) > len(truth), (
            f"FAIL: {ds}: the reference does not fan out here, so this day "
            "cannot grade the join"
        )
        for name, sql in (("nearest-order", NEAREST), ("one-attempt-per-order", BEST_INTENT)):
            assert rows(sql, ds) != truth, (
                f"FAIL: {ds}: the {name} answer already agrees with the bridge, "
                "so this day cannot grade the fix"
            )


def test_the_first_graded_day_names_the_right_orders():
    check_day(GRADED[0])


def test_the_second_graded_day_names_the_right_orders():
    check_day(GRADED[1])


def test_the_third_graded_day_names_the_right_orders():
    check_day(GRADED[2])


def test_every_event_of_the_day_lands_exactly_once():
    """The ticket's grain rule. One row per event, every event the feed carries
    for the day, and an order on all of them."""
    ds = GRADED[2]
    build(ds)
    written = query(
        f"SELECT count(*), count(DISTINCT event_id), "
        f"count(*) FILTER (WHERE order_id IS NULL) FROM {TARGET} WHERE ds = ?",
        [ds],
    )[0]
    feed = query(FEED_COUNT, [ds])[0][0]
    assert written[0] == written[1], (
        f"FAIL: {ds}: {written[0]} rows for {written[1]} events"
    )
    assert not written[2], f"FAIL: {ds}: {written[2]} rows carry no order"
    assert written[0] == feed, (
        f"FAIL: {ds}: {written[0]} rows against {feed} events in the feed"
    )


def test_the_returned_count_is_the_rows_written():
    """`build_daily` reports what it wrote. The DAG's log is the only place a
    short day is visible before the spreadsheet is."""
    ds = GRADED[1]
    written = build(ds)
    assert written == len(built(ds)), (
        f"FAIL: {ds}: build_daily returned {written} and the table holds "
        f"{len(built(ds))} rows"
    )


def test_rebuilding_a_day_leaves_one_copy():
    """A day rebuilt for late rows must replace itself, not double."""
    ds = GRADED[0]
    build(ds)
    once = built(ds)
    build(ds)
    assert built(ds) == once, f"FAIL: {ds}: the second build changed the day"


def test_building_one_day_leaves_the_others_alone():
    """The write is scoped to the day the run owns. A build that empties the
    table first takes every other day with it."""
    first, second = GRADED[0], GRADED[1]
    build(first)
    kept = built(first)
    build(second)
    assert built(first) == kept, (
        f"FAIL: building {second} changed {first}, so the write is not scoped "
        "to the day"
    )
