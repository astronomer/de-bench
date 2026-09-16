"""RTN-266 — does a day's returns row hold every return raised against it?

Never ships to the agent. It runs beside the scored tree, with the tree root on
`PYTHONPATH`, so it imports the patched module the same way the DAG does.

**Why a verifier rather than a DAG run.** The subject is what a run of nights
leaves behind, and a run of nights through the scheduler is seven scheduler
runs to grade one query. The verifier calls `returns_netting.net_delivery(ds)`
itself, night after night in order, which grades the same code in seconds and
removes the shortcut: a day typed into the table by hand is a day the replay
replaces before anything reads it.

**No authored numbers.** Every expected result is computed from `raw.returns`,
`raw.orders` and `raw.order_lines` in the same session. All three are read-only
landing tables, so a tree edit cannot move the answer. The graded days are
computed too — they are whichever order days the seven nights name.

**Seven nights, sixty-four order days.** An authorisation is raised between
three and sixty days after its order, so the nights

    2026-03-20 .. 2026-03-26

name order days running from 2026-01-19 to 2026-03-23. Every one of those days
is rebuilt whole from a feed that holds its whole history, so the answer for a
day is the same whichever night reached it. The span sits outside both reserved
ranges.

**What the queries below are for.** `figures()` is the answer, and the same
query with the population predicate changed is each of the wrong ones: keeping
the staff transactions the rest of commerce drops, or dropping the soft-deleted
orders R-6 says to keep. The first test proves the graded span can tell them
apart, and proves the ordinal is still not a key. See `checks.yaml` for what
convicts each.
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
TARGET = warehouse.qualify("marts.returns_by_order_day")

#: The nights the replay runs, in order. See the module docstring.
NIGHTS = ("2026-03-20", "2026-03-21", "2026-03-22", "2026-03-23",
          "2026-03-24", "2026-03-25", "2026-03-26")

#: An order day months before anything the nights can reach, used once to prove
#: a night writes only the days it named. Outside both reserved ranges.
UNTOUCHED_DAY = "2025-11-05"

#: The seven columns the ticket asks for. A table carrying more than these is
#: not convicted for it; one missing a column fails to read at all.
COLUMNS = ("ds", "rmas", "open_rmas", "returned_qty", "refund_cents",
           "card_refund_cents", "returned_line_cents")

#: The order days one night's authorisations name. `is_test` is out for the
#: same reason it is out of the figures: a day named only by a staff
#: transaction has nothing to publish.
_NAMED = f"""
    SELECT DISTINCT o.local_order_date AS ds
    FROM {RETURNS} r
    JOIN {ORDERS} o ON o.order_id = r.order_id
    WHERE r.initiated_at >= ?::DATE
      AND r.initiated_at < ?::DATE + INTERVAL 1 DAY
      AND NOT o.is_test
    ORDER BY ds
"""

_FIGURES = """
    SELECT o.local_order_date                                   AS ds,
           count(*)                                             AS rmas,
           count(*) FILTER (r.received_at IS NULL)              AS open_rmas,
           sum(r.qty)::DECIMAL(18,3)                            AS returned_qty,
           sum(r.refund_cents)                                  AS refund_cents,
           coalesce(sum(r.refund_cents)
                    FILTER (r.refund_method = 'original'), 0)   AS card_refund_cents,
           sum(l.line_total_cents)                              AS returned_line_cents
    FROM {returns} r
    JOIN {orders} o ON o.order_id = r.order_id
    JOIN {lines} l ON l.order_id = r.order_id
                  AND l.order_line_id = r.order_line_id
    WHERE o.local_order_date IN ({marks})
      {extra}
    GROUP BY 1
"""


def _figures_sql(days: tuple[str, ...], extra: str) -> str:
    return _FIGURES.format(
        returns=RETURNS, orders=ORDERS, lines=LINES,
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


def named_days(nights: tuple[str, ...]) -> tuple[str, ...]:
    """The order days those nights' authorisations name, oldest first."""
    days: set[dt.date] = set()
    for night in nights:
        days |= {row[0] for row in query(_NAMED, [night, night])}
    return tuple(str(day) for day in sorted(days))


#: The graded days: whichever order days the seven nights name. The ticket
#: names none of them and no date at all.
GRADED = named_days(NIGHTS)


def figures(days: tuple[str, ...],
            extra: str = "AND NOT o.is_test") -> list[tuple]:
    """The expected figures over those order days, sorted."""
    rows = query(_figures_sql(days, extra), list(days))
    return sorted(tuple(row) for row in rows)


def built(days: tuple[str, ...]) -> list[tuple]:
    """What the table holds for those order days, sorted. A list rather than a
    set, so a day appended to twice is visible."""
    marks = ", ".join("?::DATE" for _ in days)
    columns = ", ".join(
        "returned_qty::DECIMAL(18,3)" if name == "returned_qty" else name
        for name in COLUMNS
    )
    rows = query(f"SELECT {columns} FROM {TARGET} WHERE ds IN ({marks})",
                 list(days))
    return sorted(tuple(row) for row in rows)


def run(ds: str) -> int:
    """One night, the way `net_delivery` runs in the DAG."""
    from projects.commerce.lib import returns_netting

    return returns_netting.net_delivery(ds)


def replay(nights: tuple[str, ...]) -> None:
    """The nights, in order, oldest first."""
    for night in nights:
        run(night)


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

    The graded span is only worth grading if the reach, the ordinal, the staff
    rows and the soft deletes are all on it. If the feed ever changes shape, or
    the nights drift off the days that carry it, this fails here rather than
    passing a task that measures nothing.
    """
    truth = figures(GRADED)
    assert truth, "FAIL: no returns over the graded order days"
    assert len(truth) == len(GRADED), (
        f"FAIL: {len(GRADED)} order days named and {len(truth)} carry figures"
    )

    earliest = dt.date.fromisoformat(GRADED[0])
    first_night = dt.date.fromisoformat(NIGHTS[0])
    assert (first_night - earliest).days > 30, (
        "FAIL: every day the nights name is inside thirty days of them, so the "
        "span cannot grade the reach"
    )

    for name, kwargs in (
        ("with-test", {"extra": ""}),
        ("no-deleted", {"extra": "AND NOT o.is_test AND o.deleted_at IS NULL"}),
    ):
        assert figures(GRADED, **kwargs) != truth, (
            f"FAIL: the {name} answer already agrees with the feed over the "
            "graded days, so the span cannot grade the fix"
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
        "ordinal has become a key, so the span cannot grade the join"
    )


def test_the_replay_leaves_every_order_day_whole():
    """The substance. Seven nights in order, and every order day they name
    holds every authorisation the feed carries against it."""
    forget(GRADED)
    replay(NIGHTS)
    expected = figures(GRADED)
    actual = built(GRADED)
    assert actual == expected, blame(actual, expected)


def test_running_a_night_twice_leaves_the_same_table():
    """A night that is re-run replaces the days it named and changes nothing."""
    night = NIGHTS[-1]
    run(night)
    once = built(GRADED)
    run(night)
    assert built(GRADED) == once, (
        f"FAIL: the second run of {night} moved the table"
    )


def test_a_night_leaves_the_days_it_named_nothing_for_alone():
    """A night writes the days its own delivery named and no others.

    The warehouse takes one writer and `projects/platform/README.md` forbids a
    full rebuild on it, so a build that rewrites the feed's history holds the
    file while every other team queues. The row planted here belongs to an order
    day four months before the earliest day any of these nights can reach:
    nothing delivered on the night below names it, and only a build that
    rewrites the whole table moves it. It is copied off a graded day rather than
    typed, so a table carrying extra columns is planted into as happily as one
    that does not.
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

    run(NIGHTS[-2])

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
        f"FAIL: the night of {NIGHTS[-2]} rewrote {UNTOUCHED_DAY}, which it "
        "named nothing for"
    )
