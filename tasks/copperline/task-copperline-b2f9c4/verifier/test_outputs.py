"""PAY-251 — does the day's card settlement figure hold the whole day?

Never ships to the agent. It runs beside the scored tree, with the tree root on
`PYTHONPATH`, so it imports the patched module the same way the DAG does.

**Why a verifier rather than a DAG run.** The subject is what a sequence of
nights leaves behind, and a sequence of nights through the scheduler is twelve
scheduler runs to grade two queries. The verifier calls
`card_settlement.load_delivery(ds)` itself, night after night in order, which
grades the same code in seconds and removes the shortcut: a day typed into the
table by hand is a day the next night replaces before anything reads it.

**No authored numbers.** Every expected result is computed from
`raw.pay_meridian_settlements` in the same session. It is a read-only landing
table, so a tree edit cannot move the answer.

**Twelve nights, seven graded days.** Meridian delivers the tail of a
settlement day for five days after it, so a settlement day is complete on the
fifth night after it and no sooner. The replay runs the nights

    2026-04-20 .. 2026-05-01

and grades the settlement days

    2026-04-20 .. 2026-04-26

which are exactly the days every one of whose rows is delivered inside the
replay. The window sits outside the reserved ranges, outside the 2025
processor overlap quarter and outside FY2026-P04, which the close task grades.

**What the queries below are for.** `TRUTH` is the answer. `AS_AT` is the
answer a night could have published. `THREE_DAY` is what a fixed three-day
lookback leaves behind — the number the POS catch-up and the funnel both
carry, for sources that behave differently. `SAME_DAY` is what no lookback at
all leaves behind. The first test proves the graded span can tell them apart.
See `checks.yaml` for what convicts each.
"""

from __future__ import annotations

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
EVENTS = warehouse.qualify("raw.pay_meridian_settlements")
TARGET = warehouse.qualify("marts.card_settlement_daily")

#: The nights the replay runs, in order. See the module docstring.
NIGHTS = ("2026-04-20", "2026-04-21", "2026-04-22", "2026-04-23",
          "2026-04-24", "2026-04-25", "2026-04-26", "2026-04-27",
          "2026-04-28", "2026-04-29", "2026-04-30", "2026-05-01")

#: The settlement days graded: the seven whose every row lands inside the
#: replay. The ticket names none of them and no date at all.
GRADED = NIGHTS[:7]

#: A settlement day far outside the replay, used once to prove a night writes
#: only the days it received rows for.
UNTOUCHED_DAY = "2025-11-05"

#: The six columns the ticket asks for. A table carrying more than these is
#: not convicted for it; one missing a column fails to read at all.
COLUMNS = ("ds", "currency_code", "events", "captured_cents", "refunded_cents",
           "chargeback_cents")

_FIGURES = """
    SELECT event_time_utc::DATE                             AS ds,
           currency_code,
           count(*)                                         AS events,
           coalesce(sum(amount_cents)
                    FILTER (event_type = 'captured'), 0)    AS captured_cents,
           coalesce(sum(amount_cents)
                    FILTER (event_type = 'refunded'), 0)    AS refunded_cents,
           coalesce(sum(amount_cents)
                    FILTER (event_type = 'chargeback'), 0)  AS chargeback_cents
    FROM {events}
    WHERE event_time_utc >= ?::DATE
      AND event_time_utc < ?::DATE + INTERVAL 1 DAY
      {extra}
    GROUP BY 1, 2
"""

#: The answer: every row the feed carries for the day, whenever it arrived.
TRUTH = _FIGURES.format(events=EVENTS, extra="")

#: What a night could honestly have published: the same figures over the rows
#: delivered by the end of that night.
AS_AT = _FIGURES.format(
    events=EVENTS, extra="AND loaded_at < ?::DATE + INTERVAL 1 DAY")

#: What a fixed three-day lookback leaves behind. A day rebuilt on the night
#: it happened and on the two nights after it holds the rows that arrived
#: inside three days and never sees the rest.
THREE_DAY = _FIGURES.format(
    events=EVENTS,
    extra="AND date_diff('day', event_time_utc::DATE, loaded_at::DATE) <= 2")

#: What a build with no lookback at all leaves behind: the day, built once, on
#: the day itself.
SAME_DAY = _FIGURES.format(
    events=EVENTS,
    extra="AND date_diff('day', event_time_utc::DATE, loaded_at::DATE) = 0")


def query(sql: str, params: list | None = None) -> list[tuple]:
    """One read, on its own connection. DuckDB takes one writer and the build
    under test opens its own, so nothing here may hold the file open."""
    con = duckdb.connect(DB)
    try:
        return con.execute(sql, params or []).fetchall()
    finally:
        con.close()


def figures(sql: str, days: tuple[str, ...],
            as_at: str | None = None) -> list[tuple]:
    """The expected figures over a run of settlement days, sorted."""
    rows: list[tuple] = []
    for day in days:
        params = [day, day] + ([as_at] if as_at else [])
        rows += [tuple(row) for row in query(sql, params)]
    return sorted(rows)


def built(days: tuple[str, ...]) -> list[tuple]:
    """What the table holds for those settlement days, sorted. A list rather
    than a set, so a day appended to twice is visible."""
    marks = ", ".join("?" for _ in days)
    rows = query(
        f"SELECT {', '.join(COLUMNS)} FROM {TARGET} WHERE ds IN ({marks})",
        list(days),
    )
    return sorted(tuple(row) for row in rows)


def run(ds: str) -> int:
    """One night, the way `load_delivery` runs in the DAG."""
    from projects.commerce.lib import card_settlement

    return card_settlement.load_delivery(ds)


def replay(nights: tuple[str, ...]) -> None:
    """The nights, in order, oldest first."""
    for night in nights:
        run(night)


def forget(days: tuple[str, ...]) -> None:
    """Forget those settlement days, so a replay starts where a first run
    would. The table may not exist yet on the first test that calls this."""
    marks = ", ".join("?" for _ in days)
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
        f"{len(set(actual) - set(expected))} figure(s) the feed does not "
        f"support and {len(set(expected) - set(actual))} of its own missing. "
        f"first wrong {wrong}; first missing {missing}"
    )


def test_the_graded_days_can_tell_the_answers_apart():
    """The fixture's own guard.

    The graded span is only worth grading if the late tail is on it. If the
    feed ever stops arriving late, or the replay drifts off the days that
    carry it, this fails here rather than passing a task that measures
    nothing.
    """
    truth = figures(TRUTH, GRADED)
    assert truth, "FAIL: no settlement events over the graded days"

    late = query(
        f"SELECT count(*) FROM {EVENTS} "
        "WHERE event_time_utc::DATE BETWEEN ?::DATE AND ?::DATE "
        "AND date_diff('day', event_time_utc::DATE, loaded_at::DATE) > 2",
        [GRADED[0], GRADED[-1]],
    )[0][0]
    assert late, (
        "FAIL: nothing on the graded days arrived more than three days late, "
        "so the span cannot grade the lookback"
    )

    for name, sql in (("three-day", THREE_DAY), ("same-day", SAME_DAY)):
        assert figures(sql, GRADED) != truth, (
            f"FAIL: the {name} answer already agrees with the feed over the "
            "graded days, so the span cannot grade the fix"
        )


def test_the_replay_leaves_every_graded_day_whole():
    """The substance. Twelve nights in order, and every graded settlement day
    holds every row the feed carries for it."""
    forget(GRADED)
    replay(NIGHTS)
    expected = figures(TRUTH, GRADED)
    actual = built(GRADED)
    assert actual == expected, blame(actual, expected)


def test_a_night_publishes_what_had_arrived_by_that_night():
    """A night reports the day as it stood that night, not as it stands now.

    The table is the record of what each night could have published, so a
    replayed night that reads past its own date reports figures nobody held at
    the time.
    """
    early = NIGHTS[:3]
    forget(GRADED)
    replay(early)
    expected = figures(AS_AT, early, as_at=early[-1])
    actual = built(early)
    assert actual == expected, blame(actual, expected)


def test_running_a_night_twice_leaves_the_same_table():
    """A night that is re-run replaces the days it owns and changes nothing."""
    night = NIGHTS[-1]
    run(night)
    once = built(GRADED)
    run(night)
    assert built(GRADED) == once, (
        f"FAIL: the second run of {night} moved the table"
    )


def test_the_returned_count_is_the_rows_written():
    """`load_delivery` reports the rows it wrote across the days it rebuilt.
    The DAG's log is the only place a short night is visible.

    Bounded, not exact: the prompt names no day-selection rule beyond leaving
    the feed's history alone, and a trace audit found a fair repair whose
    late-arrival window rebuilds a superset of the delivery's own days — its
    honest count sits above the delivery-days total. The count must cover at
    least the days the delivery named, and at most the rows this night could
    have written: the table's rows for settlement days on or before the night.
    The prior tests leave days after the night in the table, so a count
    decoupled from the write — a count(*) of the whole table — lands above the
    ceiling and fails, where the earlier whole-table bound let it through.
    A token count (a 1, a rowcount of nothing) still fails low, and the
    untouched-day test below is what convicts a full rebuild.
    """
    night = NIGHTS[-2]
    written = run(night)
    days = tuple(str(row[0]) for row in query(
        f"SELECT DISTINCT event_time_utc::DATE FROM {EVENTS} "
        "WHERE loaded_at >= ?::DATE AND loaded_at < ?::DATE + INTERVAL 1 DAY",
        [night, night],
    ))
    assert days, f"FAIL: the feed delivered nothing on {night}"
    held = len(built(days))
    reachable = query(
        f"SELECT count(*) FROM {TARGET} WHERE ds <= ?::DATE", [night],
    )[0][0]
    assert held <= written <= reachable, (
        f"FAIL: {night}: load_delivery returned {written}; the days its own "
        f"delivery touched hold {held} rows and the table holds {reachable} "
        f"for settlement days this night could have written"
    )


def test_a_night_leaves_the_days_it_received_nothing_for_alone():
    """A night writes the days its own delivery moved and no others.

    The warehouse takes one writer and `projects/platform/README.md` forbids a
    full rebuild on it, so a build that rewrites the feed's history holds the
    file while every other team queues. The row planted here belongs to a
    settlement day months before the replay: nothing delivered on the night
    below can reach it, and only a build that rewrites the whole table moves
    it. It is copied off a graded day rather than typed, so a table carrying
    extra columns is planted into as happily as one that does not.
    """
    con = duckdb.connect(DB)
    try:
        con.execute(f"DELETE FROM {TARGET} WHERE ds = ?::DATE", [UNTOUCHED_DAY])
        con.execute(
            f"INSERT INTO {TARGET} "
            f"SELECT * REPLACE (?::DATE AS ds, 424242 AS events) "
            f"FROM {TARGET} WHERE ds = ?::DATE LIMIT 1",
            [UNTOUCHED_DAY, GRADED[0]],
        )
        planted = con.execute(
            f"SELECT count(*) FROM {TARGET} "
            "WHERE ds = ?::DATE AND events = 424242",
            [UNTOUCHED_DAY],
        ).fetchone()[0]
    finally:
        con.close()
    assert planted == 1, (
        f"FAIL: the table holds no row for {GRADED[0]} to copy, so the graded "
        "days were never written"
    )

    run(NIGHTS[-3])

    survived = query(
        f"SELECT count(*) FROM {TARGET} WHERE ds = ?::DATE AND events = 424242",
        [UNTOUCHED_DAY],
    )[0][0]
    con = duckdb.connect(DB)
    try:
        con.execute(f"DELETE FROM {TARGET} WHERE ds = ?::DATE", [UNTOUCHED_DAY])
    finally:
        con.close()
    assert survived == 1, (
        f"FAIL: the night of {NIGHTS[-3]} rewrote {UNTOUCHED_DAY}, which it "
        "received nothing for"
    )
