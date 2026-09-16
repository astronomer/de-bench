"""GRO-447 — does a guest session come out naming the customer its order names?

Never ships to the agent. It runs beside the scored tree, with the tree root on
`PYTHONPATH`, so it imports the patched module the same way a DAG does.

**Why a verifier rather than a DAG run.** `gro_sessionize_daily` is
asset-scheduled and picks its own days out of the feed, so
`airflow dags test <dag_id> <ds>` cannot point it at a chosen day — and that is
all the run stage can do. The subject is one library call the DAG makes, and
calling it directly grades the same code in about a minute. It also removes the
shortcut: a day's `customer_ref` typed into the mart by hand is a day the next
build replaces before anything here reads it, and a trial's patch carries
nothing under `include/data` in the first place.

`marts.fct_web_sessions` is derived, so the shipped warehouse does not carry it
(`AGENTS.md`, "The warehouse"). The fixture below makes the schema and the
table, empty, at the grain the library publishes. That is scenery, not part of
the answer.

**No authored expected result.** Every id this compares against is read out of
`raw.web_events` and `raw.orders` in the same session. Both are landed
read-only tables and the scored container lays them again from the image, so
neither a tree edit nor a warehouse edit can move the answer.

**What the honest stitch is.** A session's customer as captured is
`max(customer_ref)` over its own events — that is what the build publishes. A
session that captured none, and that converted, takes the id its order carries:
`raw.orders.loyalty_id`. `raw.orders.customer_ref` is the trade account and is
null on every store, web and marketplace row, so the shipped join reaches
nothing. An order that carries neither id is a guest checkout and its session
stays unresolved.

**Three days, and the ticket names none of them.**

    2025-12-11   ordinary trading, five months before the range ends
    2026-04-17   the same, four months on
    2026-05-13   the same, near the end of the range

None is a closure day, a spike, a decay or replay day, or a migration day; none
falls in a reserved window; and none is named by any other copperline ticket.
The first test proves each of them still holds every population this grades,
so a world that stops taking guest checkouts fails here rather than passing a
task that measures nothing.

**Independent of how events are counted.** Nothing here reads `events`,
`product_views`, `add_to_carts`, `checkouts` or `orders`. Which key the build
counts an event by moves those columns and moves nothing this compares, so this
verifier grades the same answer whichever key `build_sessions` holds.
"""

from __future__ import annotations

import traceback

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
# search path and carries tables under the same names.
EVENTS = warehouse.qualify("raw.web_events")
ORDERS = warehouse.qualify("raw.orders")
SESSIONS = warehouse.qualify("marts.fct_web_sessions")
MARTS = warehouse.qualify("marts")
OPS = warehouse.qualify("ops")

#: The days the stitch is measured on. See the module docstring.
GRADED = ("2025-12-11", "2026-04-17", "2026-05-13")

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

#: One day's sessions as the event stream alone gives them: the customer the
#: session captured, and the order it converted against. This is what the build
#: publishes before the stitch runs, and it holds whichever key the build counts
#: events by.
CAPTURED = f"""
SELECT session_id,
       max(customer_ref) AS captured_ref,
       max(order_id)     AS order_id
FROM {EVENTS}
WHERE event_time_utc::DATE = ?
GROUP BY session_id
"""

#: The answer. A session keeps the customer it captured; one that captured none
#: takes the id its own order carries; one whose order names nobody stays null.
TRUTH = f"""
WITH captured AS ({CAPTURED})
SELECT c.session_id, coalesce(c.captured_ref, o.loyalty_id)
FROM captured c
LEFT JOIN {ORDERS} o ON o.order_id = c.order_id
ORDER BY c.session_id
"""

#: The same, over the population staging keeps. Its only use is the first
#: test's proof that reading the order book either way gives the same rows on
#: these days, so the ticket does not turn on a filter it never mentions.
TRUTH_TRADING = TRUTH.replace(
    "ON o.order_id = c.order_id",
    "ON o.order_id = c.order_id AND o.is_test = false AND o.deleted_at IS NULL",
)

#: What the day holds, by population.
POPULATIONS = f"""
WITH captured AS ({CAPTURED})
SELECT count(*) FILTER (c.captured_ref IS NOT NULL)                    AS signed_in,
       count(*) FILTER (c.order_id IS NULL)                            AS quiet,
       count(*) FILTER (c.captured_ref IS NULL AND c.order_id IS NOT NULL)
                                                                       AS guest,
       count(*) FILTER (c.captured_ref IS NULL AND c.order_id IS NOT NULL
                        AND o.loyalty_id IS NOT NULL)                  AS resolvable,
       count(*) FILTER (c.captured_ref IS NULL AND c.order_id IS NOT NULL
                        AND o.loyalty_id IS NULL)                      AS nameless,
       count(*) FILTER (c.captured_ref IS NULL AND c.order_id IS NOT NULL
                        AND o.customer_ref IS NOT NULL)                AS trade_keyed
FROM captured c
LEFT JOIN {ORDERS} o ON o.order_id = c.order_id
"""

BUILT = f"SELECT session_id, customer_ref FROM {SESSIONS} WHERE ds = ? ORDER BY session_id"

#: Run once, at the top. Every count the last test compares against.
FEED_SIZE = None
BOOK_SIZE = None

#: One build and one stitch per day, kept so that eight tests cost three builds.
_DAYS: dict[str, dict] = {}


def require(ok: bool, why: str) -> None:
    """Fail with a reason the report can read.

    The reason is PRINTED as well as raised. The scorer runs pytest with
    `--tb=line` and lifts the blame out of the run's own output by looking for
    lines that begin `FAIL: `; an assertion message alone reaches the log
    prefixed with pytest's `E`, and never reaches the report.
    """
    if not ok:
        print(why)
        raise AssertionError(why)


def query(sql: str, params: list | None = None) -> list[tuple]:
    """One read, on its own connection. DuckDB takes one writer and the calls
    under test open their own, so nothing here may hold the file open."""
    con = duckdb.connect(DB)
    try:
        return con.execute(sql, params or []).fetchall()
    finally:
        con.close()


@pytest.fixture(scope="session", autouse=True)
def warehouse_ready():
    """The relation the library writes, and the two feed sizes.

    `marts.fct_web_sessions` is derived, so the shipped warehouse has neither
    the table nor the schema that holds it. Making it here is scenery: a fix
    that creates it as well finds it there already.
    """
    global FEED_SIZE, BOOK_SIZE
    columns = ", ".join(f"{name} {sql_type}" for name, sql_type in GRAIN)
    con = duckdb.connect(DB)
    try:
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {MARTS}")
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {OPS}")
        con.execute(f"DROP TABLE IF EXISTS {SESSIONS}")
        con.execute(f"CREATE TABLE {SESSIONS} ({columns})")
        FEED_SIZE = con.execute(f"SELECT count(*) FROM {EVENTS}").fetchone()[0]
        BOOK_SIZE = con.execute(f"SELECT count(*) FROM {ORDERS}").fetchone()[0]
    finally:
        con.close()


def call(what: str, ds: str):
    """Run one library call for one day, and blame it by name if it raises."""
    from projects.growth.lib import sessions

    try:
        return getattr(sessions, what)(ds)
    except Exception as exc:  # noqa: BLE001 - the reason is the finding
        # The whole traceback goes to the run's log; the report only carries
        # the first 160 characters of a `FAIL: ` line, so that line leads with
        # the exception and nothing else.
        traceback.print_exc()
        why = str(exc).replace("\n", " ")
        require(False, f"FAIL: {ds}: {type(exc).__name__} from {what}: {why}")


def day(ds: str) -> dict:
    """Build the day, note what the build published, then stitch it once.

    The pre-stitch rows are kept because two of the tests are about them: what
    the build alone may know, and how many rows the stitch had left to change.
    """
    if ds not in _DAYS:
        call("build_sessions", ds)
        before = query(BUILT, [ds])
        _DAYS[ds] = {"before": before, "changed": call("stitch_orders", ds)}
    return _DAYS[ds]


def truth(ds: str) -> list[tuple]:
    return query(TRUTH, [ds])


def counts(ds: str) -> dict:
    row = query(POPULATIONS, [ds])[0]
    names = ("signed_in", "quiet", "guest", "resolvable", "nameless", "trade_keyed")
    return dict(zip(names, row))


def blame(ds: str, actual: list[tuple], want: list[tuple]) -> str:
    wrong = [pair for pair in zip(actual, want) if pair[0] != pair[1]][:3]
    return (
        f"FAIL: {ds}: the published sessions name {len(actual)} row(s) against "
        f"{len(want)} the feed and the order book give; "
        f"{len(set(actual) - set(want))} of them name somebody the two sides do "
        f"not support. first disagreements published/expected {wrong}"
    )


def test_each_graded_day_still_holds_every_population_this_grades():
    """The oracle checks the world before it grades anything.

    A day only grades the stitch if it carries all four populations: sessions
    that arrived signed in, sessions that never converted, guest sessions whose
    order names a member, and guest sessions whose order names nobody. It also
    has to carry the fault — no order a session names may carry a trade
    `customer_ref`, or the shipped join would already resolve something and the
    day could not tell the two links apart.

    The last assertion is why no filter is argued about below: on these days
    the orders staging drops resolve nothing, so reading the order book whole
    and reading it the way staging reads it give the identical rows.
    """
    for ds in GRADED:
        held = counts(ds)
        for population in ("signed_in", "quiet", "guest", "resolvable", "nameless"):
            require(
                held[population] > 0,
                f"FAIL: {ds}: no {population} session that day, so it grades nothing",
            )
        require(
            held["trade_keyed"] == 0,
            f"FAIL: {ds}: {held['trade_keyed']} guest session(s) name an order "
            "with a trade customer_ref, so the shipped join already resolves "
            "some of them and this day cannot grade the fix",
        )
        require(
            query(TRUTH_TRADING, [ds]) == truth(ds),
            f"FAIL: {ds}: dropping the test and soft-deleted orders changes who "
            "the sessions resolve to, so this day turns on a filter the ticket "
            "does not state",
        )


def test_a_guest_session_comes_out_naming_the_customer_its_order_names():
    """The whole of the ticket, on three days.

    One row per session, compared by name. A session that captured a customer
    keeps it, a guest session that converted takes the id its own order
    carries, and a session whose order names nobody stays null.
    """
    for ds in GRADED:
        want = truth(ds)
        require(bool(want), f"FAIL: {ds}: the feed holds no sessions, so nothing is graded")
        day(ds)
        actual = query(BUILT, [ds])
        require(actual == want, blame(ds, actual, want))


def test_a_session_that_arrived_signed_in_keeps_the_customer_it_arrived_with():
    """The stitch is for the sessions that arrived without a customer. A stitch
    that overwrites the ones that arrived with one has replaced the web
    platform's own id with the order's, on the sessions that never needed
    resolving."""
    for ds in GRADED:
        day(ds)
        moved = query(
            f"""
            WITH captured AS ({CAPTURED})
            SELECT count(*) FROM {SESSIONS} s
            JOIN captured c ON c.session_id = s.session_id
            WHERE s.ds = ? AND c.captured_ref IS NOT NULL
              AND s.customer_ref IS DISTINCT FROM c.captured_ref
            """,
            [ds, ds],
        )[0][0]
        require(
            moved == 0,
            f"FAIL: {ds}: {moved} session(s) that arrived signed in came out "
            "naming somebody else",
        )


def test_a_checkout_that_named_nobody_leaves_the_session_unresolved():
    """Guest checkout is real and the row has to say so. An id invented for a
    sale that named nobody — the order id, the anonymous id, a `GUEST-` label —
    is counted by the next reader as a customer."""
    for ds in GRADED:
        day(ds)
        named = query(
            f"""
            WITH captured AS ({CAPTURED})
            SELECT count(*) FROM {SESSIONS} s
            JOIN captured c ON c.session_id = s.session_id
            LEFT JOIN {ORDERS} o ON o.order_id = c.order_id
            WHERE s.ds = ? AND c.captured_ref IS NULL AND c.order_id IS NOT NULL
              AND o.loyalty_id IS NULL AND s.customer_ref IS NOT NULL
            """,
            [ds, ds],
        )[0][0]
        require(
            named == 0,
            f"FAIL: {ds}: {named} session(s) whose order names nobody came out "
            "naming somebody",
        )


def test_a_session_that_did_not_convert_does_not_move():
    """Most of the day never reaches a checkout. Those sessions have no order to
    be resolved against, and a stitch that reaches them has joined on something
    that is not the order."""
    for ds in GRADED:
        day(ds)
        moved = query(
            f"""
            WITH captured AS ({CAPTURED})
            SELECT count(*) FROM {SESSIONS} s
            JOIN captured c ON c.session_id = s.session_id
            WHERE s.ds = ? AND c.order_id IS NULL
              AND s.customer_ref IS DISTINCT FROM c.captured_ref
            """,
            [ds, ds],
        )[0][0]
        require(
            moved == 0,
            f"FAIL: {ds}: {moved} session(s) that never converted were resolved "
            "against an order they did not place",
        )


def test_the_day_is_built_from_the_event_stream_alone():
    """`build_day` reads the clickstream and the stitch reads the order feed
    after it. Four DAGs build through this library and only `gro_sessionize_daily`
    runs the stitch, so resolving inside the build gives three of them a
    different session table from the one they have now."""
    for ds in GRADED:
        published = day(ds)["before"]
        captured = query(
            f"WITH captured AS ({CAPTURED}) "
            "SELECT session_id, captured_ref FROM captured ORDER BY session_id",
            [ds],
        )
        require(
            published == captured,
            f"FAIL: {ds}: the build published a customer the event stream does "
            f"not carry on {len([p for p in zip(published, captured) if p[0] != p[1]])} "
            "session(s); the order feed is the stitch's to read, not the build's",
        )


def test_the_step_reports_the_rows_it_changed():
    """The number that should have raised this two years ago.

    The step's docstring has always said it returns the rows changed. What it
    returned was the day's converting sessions that carry any customer, which
    counts the sessions that arrived signed in as though the stitch had
    resolved them — so it moved with volume, it was never zero, and it said
    nothing about the stitch.
    """
    for ds in GRADED:
        reported = day(ds)["changed"]
        want = counts(ds)["resolvable"]
        require(
            reported == want,
            f"FAIL: {ds}: the step reported {reported} row(s) changed and the "
            f"order book resolves {want} guest session(s) that day",
        )


def test_running_the_day_twice_changes_nothing_and_says_so():
    """The step is rerun on every retry and on every backfill. The second run
    finds nothing left to resolve, so it changes no row and reports none."""
    ds = GRADED[0]
    day(ds)
    once = query(BUILT, [ds])
    again = call("stitch_orders", ds)
    require(
        query(BUILT, [ds]) == once,
        f"FAIL: {ds}: the second run over the same day changed the rows",
    )
    require(
        again == 0,
        f"FAIL: {ds}: the second run changed nothing and reported {again} row(s)",
    )


def test_the_feed_and_the_order_book_keep_every_row():
    """Run after every build and stitch above. `raw.web_events` and `raw.orders`
    are landed source: repairing the order book so that the shipped join
    resolves is not a fix, it is a rewrite of what the OMS sent us."""
    feed = query(f"SELECT count(*) FROM {EVENTS}")[0][0]
    book = query(f"SELECT count(*) FROM {ORDERS}")[0][0]
    require(
        (feed, book) == (FEED_SIZE, BOOK_SIZE),
        f"FAIL: the feed holds {feed} row(s) against {FEED_SIZE} and the order "
        f"book {book} against {BOOK_SIZE}; landed source was written to",
    )


def test_the_published_grain_did_not_move():
    """The ticket's last line. The fix is a change to which column the stitch
    reaches for, not to what the table holds."""
    from projects.growth.lib import sessions

    require(
        tuple(sessions.SESSION_COLUMNS) == tuple(name for name, _ in GRAIN),
        f"FAIL: the published session grain is now {tuple(sessions.SESSION_COLUMNS)}",
    )
