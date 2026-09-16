"""RTN-284 — does a channel's row hold the returns that channel sold?

Never ships to the agent. It runs beside the scored tree, with the tree root on
`PYTHONPATH`, so it imports the patched module the same way the DAG does.

**Why a verifier rather than a DAG run.** The subject is what a run of order
days leaves behind, and a dozen order days through the scheduler is a dozen
scheduler runs to grade one query. The verifier calls
`returns_channel.build_day(ds)` itself, day after day, which grades the same
code in seconds and removes the shortcut: a day typed into the table by hand is
a day the replay replaces before anything reads it.

**No authored numbers.** Every expected result is computed from `raw.returns`,
`raw.orders` and `raw.order_lines` in the same session. All three are read-only
landing tables, so a tree edit cannot move the answer.

**Both attributions, computed here.** `figures()` takes the channel off the
ORDER, which is the answer. The same query with `{channel}` set to
`r.return_channel` is the door attribution — the wrong one, and the one the
feed makes easy — and the same query with the population predicate changed is
each of the other wrong ones. The first test proves the graded days can tell
all of them apart, and proves the ordinal is still not a key.

**Twelve order days, named nowhere.** The ticket names no date at all. The days
run 2025-11-10 to 2025-11-21: after the UTC cutover of 2025-11-03, before the
Black Friday spike of 2025-11-28, outside both reserved ranges, outside the
2025 processor overlap quarter and outside FY2026-P04.
"""

from __future__ import annotations

import datetime as dt

import duckdb

# The scored tree is on PYTHONPATH (scoring.build_score_env), so the house
# library resolves the warehouse the way every DAG in the world does —
# `DUCKDB_PATH` against the root that holds `include/`, whatever the working
# directory happens to be.
from include.lib import warehouse

DB = str(warehouse.warehouse_path())

# Every name is qualified onto the live warehouse, for the reason
# `include/lib/warehouse.py` gives: the frozen `nwv` copy sits first on the
# search path, so an unqualified read can answer with another book's numbers.
RETURNS = warehouse.qualify("raw.returns")
ORDERS = warehouse.qualify("raw.orders")
LINES = warehouse.qualify("raw.order_lines")
TARGET = warehouse.qualify("marts.returns_by_channel_day")

#: The order days the replay builds, in order. See the module docstring.
GRADED = ("2025-11-10", "2025-11-11", "2025-11-12", "2025-11-13",
          "2025-11-14", "2025-11-15", "2025-11-16", "2025-11-17",
          "2025-11-18", "2025-11-19", "2025-11-20", "2025-11-21")

#: An order day five months before the graded ones, used once to prove a run
#: writes only the day it was given. Outside both reserved ranges.
UNTOUCHED_DAY = "2025-06-10"

#: The nine columns the ticket asks for. A table carrying more than these is
#: not convicted for it; one missing a column fails to read at all.
COLUMNS = ("ds", "channel", "rmas", "returned_qty", "refund_cents",
           "card_refund_cents", "store_credit_cents", "counter_credit_cents",
           "returned_line_cents")

#: The answer, and every wrong answer, from one shape. `channel` is the
#: attribution under test and `extra` is the population predicate.
_FIGURES = """
    SELECT o.local_order_date                                   AS ds,
           {channel}                                            AS channel,
           count(*)                                             AS rmas,
           sum(r.qty)::DECIMAL(18,3)                            AS returned_qty,
           sum(r.refund_cents)                                  AS refund_cents,
           coalesce(sum(r.refund_cents)
                    FILTER (r.refund_method = 'original'), 0)   AS card_refund_cents,
           coalesce(sum(r.refund_cents)
                    FILTER (r.refund_method = 'store_credit'), 0)
                                                                AS store_credit_cents,
           coalesce(sum(r.refund_cents)
                    FILTER (r.refund_method = 'store_credit'
                            AND r.return_channel = 'store'), 0) AS counter_credit_cents,
           sum(l.line_total_cents)                              AS returned_line_cents
    FROM {returns} r
    JOIN {orders} o ON o.order_id = r.order_id
    JOIN {lines} l ON l.order_id = r.order_id
                  AND l.order_line_id = r.order_line_id
    WHERE o.local_order_date IN ({marks})
      {extra}
    GROUP BY 1, 2
"""

#: The attribution the ticket asks for: the channel that took the sale.
SALE = "o.channel"

#: The attribution the feed makes easy: the door the goods came back through.
DOOR = "r.return_channel"


def _figures_sql(days: tuple[str, ...], channel: str, extra: str) -> str:
    return _FIGURES.format(
        returns=RETURNS, orders=ORDERS, lines=LINES, channel=channel,
        marks=", ".join("?::DATE" for _ in days), extra=extra,
    )


def query(sql: str, params: list | None = None) -> list[tuple]:
    """One read, on its own connection. DuckDB takes one writer and the build
    under test opens its own, so nothing here may hold the file open."""
    con = duckdb.connect(DB)
    try:
        return con.execute(sql, params or []).fetchall()
    finally:
        con.close()


def figures(days: tuple[str, ...] = GRADED, channel: str = SALE,
            extra: str = "AND NOT o.is_test") -> list[tuple]:
    """The expected figures over those order days, sorted."""
    rows = query(_figures_sql(days, channel, extra), list(days))
    return sorted(tuple(row) for row in rows)


def built(days: tuple[str, ...] = GRADED) -> list[tuple]:
    """What the table holds for those order days, sorted. A list rather than a
    set, so a row written twice is visible."""
    marks = ", ".join("?::DATE" for _ in days)
    columns = ", ".join(
        "returned_qty::DECIMAL(18,3)" if name == "returned_qty" else name
        for name in COLUMNS
    )
    rows = query(f"SELECT {columns} FROM {TARGET} WHERE ds IN ({marks})",
                 list(days))
    return sorted(tuple(row) for row in rows)


def run(ds: str) -> int:
    """One order day, the way `build_day` runs in the DAG."""
    from projects.commerce.lib import returns_channel

    return returns_channel.build_day(ds)


def forget(days: tuple[str, ...]) -> None:
    """Forget those order days, so a replay starts where a first run would. The
    table may not exist yet on the first test that calls this."""
    marks = ", ".join("?::DATE" for _ in days)
    con = duckdb.connect(DB)
    try:
        con.execute(f"DELETE FROM {TARGET} WHERE ds IN ({marks})", list(days))
    except duckdb.Error:
        pass
    finally:
        con.close()


def blame(actual: list[tuple], expected: list[tuple]) -> str:
    wrong = sorted(set(actual) - set(expected))[:3]
    missing = sorted(set(expected) - set(actual))[:3]
    return (
        f"FAIL: {len(actual)} rows against {len(expected)} expected. "
        f"{len(set(actual) - set(expected))} row(s) the feed does not support "
        f"and {len(set(expected) - set(actual))} of its own missing. "
        f"first wrong {wrong}; first missing {missing}"
    )


def test_the_graded_days_can_tell_the_answers_apart():
    """The fixture's own guard.

    The graded days are only worth grading if the two attributions, the ordinal,
    the staff rows and the soft deletes are all on them. If the feed ever
    changes shape, or the days drift off the population that carries it, this
    fails here rather than passing a task that measures nothing.
    """
    truth = figures()
    assert truth, "FAIL: no returns over the graded order days"
    assert len({row[0] for row in truth}) == len(GRADED), (
        f"FAIL: {len(GRADED)} order days graded and "
        f"{len({row[0] for row in truth})} carry figures"
    )

    for name, kwargs in (
        ("door", {"channel": DOOR}),
        ("with-test", {"extra": ""}),
        ("no-deleted", {"extra": "AND NOT o.is_test AND o.deleted_at IS NULL"}),
    ):
        assert figures(**kwargs) != truth, (
            f"FAIL: the {name} answer already agrees with the sale attribution "
            "over the graded days, so the span cannot grade the fix"
        )

    # The two vocabularies do not line up, and that is what makes the door
    # attribution loud rather than merely wrong: the order book sells through a
    # channel no return ever comes back through, and the feed carries a door
    # the order book never sells through.
    sold = {row[1] for row in truth}
    doors = {row[1] for row in figures(channel=DOOR)}
    assert sold - doors and doors - sold, (
        f"FAIL: the selling channels {sorted(sold)} and the return doors "
        f"{sorted(doors)} have stopped disagreeing, so a build that reads the "
        "door as the channel is no longer visible in the channel column"
    )

    # The ordinal is not the line, and the cheapest proof of it is the count.
    # A join on `order_line_id` alone matches every order that ever had a line
    # in that position, so `returned_line_cents` cannot survive it. Counting is
    # the guard rather than running the wrong join, which fans a day's
    # authorisations across nine million lines and takes a minute to say what
    # this says in milliseconds.
    ordinals, lines = query(
        f"SELECT count(DISTINCT order_line_id), count(*) FROM {LINES}"
    )[0]
    assert ordinals < 100 < lines, (
        f"FAIL: {ordinals} distinct order_line_id over {lines} lines — the "
        "ordinal has become a key, so the days cannot grade the join"
    )


def test_the_build_puts_a_return_in_the_channel_that_sold_it():
    """The substance. Twelve order days, each built whole, each holding every
    authorisation the feed carries against it, in the channel that took the
    sale."""
    forget(GRADED)
    for day in GRADED:
        run(day)
    expected = figures()
    actual = built()
    assert actual == expected, blame(actual, expected)


def test_running_a_day_twice_leaves_the_same_table():
    """A day that is rebuilt replaces what was there and changes nothing."""
    day = GRADED[-1]
    run(day)
    once = built()
    run(day)
    assert built() == once, f"FAIL: the second run of {day} moved the table"


def test_a_run_leaves_every_other_order_day_alone():
    """A run writes the order day it was given and no other.

    The warehouse takes one writer and `projects/platform/README.md` forbids a
    full rebuild on it, so a build that rewrites the table holds the file while
    every other team queues. The row planted here belongs to an order day five
    months before the earliest graded one. It is copied off a graded row rather
    than typed, so a table carrying extra columns is planted into as happily as
    one that does not.
    """
    con = duckdb.connect(DB)
    try:
        con.execute(f"DELETE FROM {TARGET} WHERE ds = ?::DATE", [UNTOUCHED_DAY])
        con.execute(
            f"INSERT INTO {TARGET} "
            f"SELECT * REPLACE (?::DATE AS ds, 424242 AS rmas) "
            f"FROM {TARGET} WHERE ds = ?::DATE LIMIT 1",
            [UNTOUCHED_DAY, GRADED[0]],
        )
        planted = con.execute(
            f"SELECT count(*) FROM {TARGET} WHERE ds = ?::DATE AND rmas = 424242",
            [UNTOUCHED_DAY],
        ).fetchone()[0]
    finally:
        con.close()
    assert planted == 1, (
        f"FAIL: the table holds no row for {GRADED[0]} to copy, so the graded "
        "days were never written"
    )

    run(GRADED[-2])

    survived = query(
        f"SELECT count(*) FROM {TARGET} WHERE ds = ?::DATE AND rmas = 424242",
        [UNTOUCHED_DAY],
    )[0][0]
    con = duckdb.connect(DB)
    try:
        con.execute(f"DELETE FROM {TARGET} WHERE ds = ?::DATE", [UNTOUCHED_DAY])
    finally:
        con.close()
    assert survived == 1, (
        f"FAIL: the run for {GRADED[-2]} rewrote {UNTOUCHED_DAY}, which it was "
        "not given"
    )
