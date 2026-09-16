"""GRO-431 — do the published sessions account for one event once?

Never ships to the agent. It runs beside the scored tree, with the tree root on
`PYTHONPATH`, so it imports the patched module the same way a DAG would.

**Why a verifier rather than a DAG run.** `gro_sessionize_daily` is
asset-scheduled and picks its own days from the feed, and
`gro_event_replay_repair` takes the window as a run parameter. Neither can be
pointed at a chosen day by `airflow dags test <dag_id> <ds>`, which is all the
run stage can do. Both build through `projects/growth/lib/sessions.py`, so that
is what this drives — the same call the mapped `build_day` task makes and the
same call the repair makes.

`marts.fct_web_sessions` and `ops.session_daily` are derived, so the shipped
warehouse carries neither (`AGENTS.md`, "The warehouse"). The fixture below
makes both, empty, at the grain the library publishes.

**No authored expected result.** Every number this compares against is read out
of `raw.web_events` in the same session, so a patched tree is graded against the
world's own feed and never against a figure typed here. The one authored figure
is the size of the replay's overlap, which is the figure
`ops/incidents/2026-01-23-event-replay.md` prints and
`tools/gen_copperline/extracts/clickstream.py` builds (`REPLAY_DUPLICATES =
1_102`). It is used as a guard on the world, not as an expected result: if a
rebuild ever moves it, the first test says so rather than letting the rest grade
a feed that no longer holds the incident.

**Six days, of four kinds.**

    2026-01-11  event days that hold repeats: the collector resent events
    2026-01-13  that had already flushed, so each is in the feed at two
                stream positions under one event_id

    2026-01-14  the day the collector dropped whole. Every one of its events
                reached the warehouse on the replay and none of them is a
                repeat, so a repair that discards replayed rows empties it

    2026-01-20  a day inside the window the ticket's incident note names —
                the collector SENT the replay on 20 to 22 January, and none
                of the events it sent happened then. An ordinary day, and a
                repair keyed to the dates the note prints breaks it

    2026-03-10  ordinary days months either side of the incident
    2026-05-19
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

# Every name is qualified onto the live warehouse, for the reason
# include/lib/warehouse.py gives: the frozen `nwv` copy sits first on the search
# path and carries tables under the same names.
EVENTS = warehouse.qualify("raw.web_events")
SESSIONS = warehouse.qualify("marts.fct_web_sessions")
VOLUME = warehouse.qualify("ops.session_daily")
MARTS = warehouse.qualify("marts")
OPS = warehouse.qualify("ops")

#: Days whose events are in the feed twice over. See the module docstring.
REPEATED = ("2026-01-11", "2026-01-13")
#: The day that exists only because of the replay.
RECOVERED_WHOLE = "2026-01-14"
#: Days with no repeat on them at all.
CLEAN = ("2026-01-20", "2026-03-10", "2026-05-19")
GRADED = REPEATED + (RECOVERED_WHOLE,) + CLEAN

#: `ops/incidents/2026-01-23-event-replay.md`, "already present from January".
#: A guard on the world, not an expected result — see the module docstring.
OVERLAP = 1_102

#: The published session grain, `sessions.SESSION_COLUMNS` as the world ships
#: it. The ticket says the grain does not change; the fixture builds the table
#: at this shape and the last test holds the library to it.
GRAIN = (
    ("ds", "DATE"),
    ("session_id", "VARCHAR"),
    ("anonymous_id", "VARCHAR"),
    ("customer_ref", "VARCHAR"),
    ("device", "VARCHAR"),
    ("country_code", "VARCHAR"),
    ("utm_source", "VARCHAR"),
    ("utm_medium", "VARCHAR"),
    ("utm_campaign", "VARCHAR"),
    ("entry_page", "VARCHAR"),
    ("session_start", "TIMESTAMP"),
    ("session_end", "TIMESTAMP"),
    ("events", "BIGINT"),
    ("product_views", "BIGINT"),
    ("add_to_carts", "BIGINT"),
    ("checkouts", "BIGINT"),
    ("orders", "BIGINT"),
    ("order_id", "VARCHAR"),
)

#: One session's true counts for one event day, straight from the feed. The
#: contract's key is `event_id`, so an event delivered twice counts once.
TRUTH = f"""
SELECT session_id,
       count(DISTINCT event_id)                                        AS events,
       count(DISTINCT event_id) FILTER (event_name = 'product_view')   AS product_views,
       count(DISTINCT event_id) FILTER (event_name = 'add_to_cart')    AS add_to_carts,
       count(DISTINCT event_id) FILTER (event_name = 'checkout_start') AS checkouts,
       count(DISTINCT event_id) FILTER (event_name = 'purchase')       AS orders
FROM {EVENTS}
WHERE event_time_utc::DATE = ?
GROUP BY session_id
ORDER BY session_id
"""

BUILT = f"""
SELECT session_id, events, product_views, add_to_carts, checkouts, orders
FROM {SESSIONS}
WHERE ds = ?
ORDER BY session_id
"""


def query(sql: str, params: list | None = None) -> list[tuple]:
    """One read, on its own connection. DuckDB takes one writer and the build
    under test opens its own, so nothing here may hold the file open."""
    con = duckdb.connect(DB)
    try:
        return con.execute(sql, params or []).fetchall()
    finally:
        con.close()


def build(ds: str) -> None:
    """Rebuild one event day, the way the mapped `build_day` task does."""
    from projects.growth.lib import sessions

    sessions.build_sessions(ds)


def feed_events(ds: str) -> int:
    """The events the feed holds for one event day."""
    return query(
        f"SELECT count(DISTINCT event_id) FROM {EVENTS} WHERE event_time_utc::DATE = ?",
        [ds],
    )[0][0]


def repeats(ds: str) -> int:
    """Events on that day that sit at more than one stream position."""
    return query(
        f"SELECT count(*) FROM (SELECT event_id FROM {EVENTS} "
        "WHERE event_time_utc::DATE = ? GROUP BY event_id HAVING count(*) > 1)",
        [ds],
    )[0][0]


def check_day(ds: str) -> None:
    """The whole of the ticket, on one day: every session accounts for its own
    events, once each, and the day's total accounts for the day's feed."""
    expected = query(TRUTH, [ds])
    assert expected, f"FAIL: {ds}: the feed holds no events that day, so nothing is graded"
    build(ds)
    actual = query(BUILT, [ds])
    if actual != expected:
        wrong = [pair for pair in zip(actual, expected) if pair[0] != pair[1]][:3]
        assert False, (
            f"FAIL: {ds}: built {len(actual)} session row(s) against {len(expected)} "
            f"in the feed; first disagreements built/feed {wrong}"
        )
    total = query(
        f"SELECT coalesce(sum(events), 0) FROM {SESSIONS} WHERE ds = ?", [ds]
    )[0][0]
    assert total == feed_events(ds), (
        f"FAIL: {ds}: the sessions account for {total} events and the feed holds "
        f"{feed_events(ds)}"
    )


@pytest.fixture(scope="session", autouse=True)
def warehouse_ready():
    """The two relations the library writes. Both are derived, so the shipped
    warehouse has neither."""
    columns = ", ".join(f"{name} {sql_type}" for name, sql_type in GRAIN)
    con = duckdb.connect(DB)
    try:
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {MARTS}")
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {OPS}")
        con.execute(f"DROP TABLE IF EXISTS {SESSIONS}")
        con.execute(f"CREATE TABLE {SESSIONS} ({columns})")
        con.execute(
            f"CREATE TABLE IF NOT EXISTS {VOLUME} "
            "(ds DATE, sessions BIGINT, events BIGINT, converting_sessions BIGINT)"
        )
    finally:
        con.close()


def test_the_feed_still_holds_the_replay_the_incident_records():
    """The oracle checks the world before it grades anything.

    The overlap is what makes the graded days able to tell a right answer from a
    wrong one. If a rebuild ever moves it, or moves which days carry it, this
    says so rather than letting the tests below pass on a feed with no incident
    left in it."""
    found = query(
        f"SELECT count(*) FROM (SELECT event_id FROM {EVENTS} "
        "GROUP BY event_id HAVING count(*) > 1)"
    )[0][0]
    assert found == OVERLAP, (
        f"FAIL: the feed holds {found} repeated event(s), the incident records {OVERLAP}"
    )
    for ds in REPEATED:
        assert repeats(ds) > 0, f"FAIL: {ds} carries no repeat, so it grades nothing"
    for ds in (RECOVERED_WHOLE,) + CLEAN:
        assert repeats(ds) == 0, f"FAIL: {ds} carries a repeat and was meant to be clean"


def test_a_day_the_collector_resent_counts_each_event_once():
    for ds in REPEATED:
        check_day(ds)


def test_the_day_the_collector_dropped_whole_keeps_its_recovered_events():
    """Every event on this day reached the warehouse on the replay and none of
    them is a repeat. Throwing the replayed rows away — by offset band, by
    `load_time`, or by deleting them — takes the whole day with it."""
    check_day(RECOVERED_WHOLE)
    built = query(f"SELECT count(*) FROM {SESSIONS} WHERE ds = ?", [RECOVERED_WHOLE])[0][0]
    assert built > 0, f"FAIL: {RECOVERED_WHOLE}: the day came out empty"


def test_an_ordinary_day_does_not_move():
    """Two months either side of the incident, and the day inside the window the
    incident note names — which is a delivery window, not an event window, and
    holds no repeat at all."""
    for ds in CLEAN:
        check_day(ds)


def test_the_daily_volume_row_counts_the_same_events():
    """`config/alerts.yml` watches `ops.session_daily`, so the number the alert
    reads has to be the number the feed holds."""
    from projects.growth.lib import sessions

    ds = REPEATED[0]
    build(ds)
    sessions.record_volume(ds)
    row = query(
        f"SELECT sessions, events FROM {VOLUME} WHERE ds = ?", [ds]
    )
    assert row, f"FAIL: {ds}: nothing was written to the daily volume table"
    built_sessions, built_events = row[0]
    expected_sessions = query(
        f"SELECT count(DISTINCT session_id) FROM {EVENTS} WHERE event_time_utc::DATE = ?",
        [ds],
    )[0][0]
    assert (built_sessions, built_events) == (expected_sessions, feed_events(ds)), (
        f"FAIL: {ds}: the volume row says {built_sessions} session(s) and "
        f"{built_events} event(s); the feed says {expected_sessions} and "
        f"{feed_events(ds)}"
    )


def test_the_feed_keeps_every_row_it_held():
    """Run last, after every build above. `raw.web_events` is landed source and
    read-only, and the replay is additive by design: both copies of a resent
    event stay in the feed and the repair reads past them."""
    found = query(
        f"SELECT count(*) FROM (SELECT event_id FROM {EVENTS} "
        "GROUP BY event_id HAVING count(*) > 1)"
    )[0][0]
    assert found == OVERLAP, (
        f"FAIL: the feed now holds {found} repeated event(s) against {OVERLAP} "
        "before the rebuilds; rows were taken out of landed source"
    )


def test_rebuilding_a_day_twice_leaves_one_copy():
    """A repair that has to be run twice must not double the day."""
    ds = REPEATED[1]
    build(ds)
    once = query(BUILT, [ds])
    build(ds)
    assert query(BUILT, [ds]) == once, f"FAIL: {ds}: the second build changed the day"


def test_the_published_grain_did_not_move():
    """The ticket's last line. The fix is a change to what a count means, not to
    what the table holds."""
    from projects.growth.lib import sessions

    assert tuple(sessions.SESSION_COLUMNS) == tuple(name for name, _ in GRAIN), (
        f"FAIL: the published session grain is now {tuple(sessions.SESSION_COLUMNS)}"
    )
