"""PAY-276 — does `int_orders_enriched` pick the same attempt on every build?

Never ships to the agent. It runs beside the scored tree, with the tree root on
`PYTHONPATH`, so it resolves the warehouse the way every DAG in the world does.

**No authored numbers.** Every expected result is computed from `raw.orders` and
`raw.payment_intents` in the same session. The expectation reproduces the
ranking the shipped model already had — day gap, a success ahead of a failure,
then the attempt number — and appends the two keys the ticket names, `created_at`
then `intent_id`. Nothing here holds a row count, a cent or a date typed by hand.

**The rule.** The shipped ranking is not a unique key: an order that raised the
same attempt number twice on the same day with the same outcome has two rows
level at the top, and `row_number()` takes whichever the engine hands it first.
The ticket keeps the three existing keys exactly as they are and adds a total
order underneath them, so the untied orders keep the attempt they have and the
tied ones settle on the attempt raised first.

**Why dbt runs here.** `int_orders_enriched` is a view and the world ships the
landed half of the warehouse only (`AGENTS.md`, "The warehouse"), so the relation
does not exist until dbt has made it. The `prepare` step in checks.yaml builds
it, everything under it and the reader above it. This file runs dbt once more,
for that reader, and nothing here may hold the warehouse open while it does:
DuckDB takes one writer.

**Why the picks are copied out.** Reading the model drags the order-line rollup
with it, so the whole book is read twice and no more: once at the engine's own
thread count and once at one thread. Both copies land in a scratch database
beside the warehouse, and every comparison below runs against those.
"""

from __future__ import annotations

import os
import subprocess
import tempfile

import duckdb
import pytest

from include.lib import warehouse, workspace_root

DB = str(warehouse.warehouse_path())

ORDERS = warehouse.qualify("raw.orders")
INTENTS = warehouse.qualify("raw.payment_intents")
MODEL = warehouse.qualify('"int".int_orders_enriched')
STG_INTENTS = warehouse.qualify("stg.stg_payments__payment_intents")

#: The published model that reads the attempt match. It selects the six payment
#: columns by name, so a pivot that drops or renames one takes it down first.
READER = "int_payment_matched"

#: The pinned dbt. `dbt/README.md` says a bare `dbt` is either not on the PATH or
#: is not the pinned one. The override exists so this file can be exercised
#: against a tree on a laptop; nothing in a trial or in scoring sets it.
DBT = os.environ.get("DE_BENCH_DBT", "/opt/dbt-venv/bin/dbt")
PROJECT = str(workspace_root() / "dbt" / "copperline_analytics")

#: Where the two copies of the pick go. Outside the warehouse, so nothing here
#: writes to the file dbt wants the single writer on.
SCRATCH = os.path.join(tempfile.gettempdir(), "pay276_picks.duckdb")


def connect(threads: int | None = None) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(DB)
    if threads is not None:
        con.execute(f"SET threads = {threads}")
    return con


def query(sql: str, params: list | None = None) -> list[tuple]:
    """One read, on its own connection. Nothing holds the file open: dbt wants
    the single writer and this runs between its invocations."""
    con = connect()
    try:
        return con.execute(sql, params or []).fetchall()
    finally:
        con.close()


def one(sql: str, params: list | None = None):
    rows = query(sql, params)
    return rows[0][0] if rows else None


def dbt(*args: str) -> subprocess.CompletedProcess:
    """Run the pinned dbt in the project directory, profile alongside."""
    return subprocess.run(
        [DBT, *args, "--profiles-dir", "."],
        cwd=PROJECT, capture_output=True, text=True, timeout=900, check=False,
    )


# ------------------------------------------------------------------ the oracle
#
# The candidate set and the ranking, read straight out of raw. The three keys are
# the shipped model's, spelled the way the shipped model spells them; the last
# two are the ones the ticket names. `stg_sales__orders` drops the test and the
# deleted rows and `stg_payments__payment_intents` casts the attempt number, so
# both are reproduced here rather than read through the views the agent may have
# touched.

CTES = f"""
with orders as (
    select
        order_id,
        order_ref,
        local_order_date                        as order_date
    from {ORDERS}
    where not coalesce(is_test, false) and deleted_at is null
),
intents as (
    select
        intent_id,
        order_ref,
        cast(attempt_no as integer)             as attempt_no,
        outcome,
        created_at,
        cast(created_at as date)                as created_date
    from {INTENTS}
),
candidates as (
    select
        o.order_id,
        i.intent_id,
        i.outcome,
        i.attempt_no,
        i.created_at,
        abs(date_diff('day', o.order_date, i.created_date))  as day_gap,
        count(*) over (partition by o.order_id)              as candidate_count
    from orders o
    join intents i
      on i.order_ref = o.order_ref
     and i.created_date between o.order_date - 7 and o.order_date + 7
),
ranked as (
    select
        *,
        row_number() over (
            partition by order_id
            order by
                day_gap,
                case outcome when 'succeeded' then 0 else 1 end,
                attempt_no,
                created_at,
                intent_id
        ) as pick,
        count(*) over (
            partition by order_id, day_gap,
                         case outcome when 'succeeded' then 0 else 1 end,
                         attempt_no
        ) as level_with
    from candidates
),
expected as (
    select order_id, intent_id, candidate_count, level_with
    from ranked
    where pick = 1
)
"""

#: One row per order that matches any attempt: the attempt the ticket's rule
#: names, how many attempts fell in the window, and how many of them are level
#: with the winner on the three keys the shipped model ranked by.
EXPECTED = CTES + "select * from expected"


def model_built() -> bool:
    try:
        query(f"SELECT 1 FROM {MODEL} LIMIT 1")
    except duckdb.Error:
        return False
    return True


@pytest.fixture(scope="session", autouse=True)
def picks():
    """Build the model if the prepare step did not, then copy the pick out twice
    and the expectation out once.

    Two reads of the whole book and no more. The second runs on one thread so a
    ranking that still depends on the order the engine happens to produce rows in
    has a chance to say so. Every test below joins these three tables instead of
    reading the model or re-ranking the feed.
    """
    if not model_built():
        result = dbt("run", "--select", "+int_orders_enriched")
        assert model_built(), (
            "FAIL: int_orders_enriched does not build, so nothing that reads the "
            f"attempt match can be graded:\n{(result.stdout or '')[-2000:]}"
        )

    if os.path.exists(SCRATCH):
        os.remove(SCRATCH)
    for label, threads in (("a", None), ("b", 1)):
        con = connect(threads)
        try:
            con.execute(f"ATTACH '{SCRATCH}' AS scratch")
            con.execute(
                f"CREATE TABLE scratch.picks_{label} AS "
                f"SELECT order_id, payment_intent_id, payment_outcome, payment_attempt_no, "
                f"payment_match_is_ambiguous, has_payment_attempt FROM {MODEL}"
            )
        finally:
            con.close()
    con = connect()
    try:
        con.execute(f"ATTACH '{SCRATCH}' AS scratch")
        con.execute(f"CREATE TABLE scratch.expected AS {EXPECTED}")
    finally:
        con.close()
    yield
    if os.path.exists(SCRATCH):
        os.remove(SCRATCH)


def picked(sql: str) -> list[tuple]:
    """A read that joins the copied picks. The scratch database is attached
    read-only so nothing can write to it by accident."""
    con = connect()
    try:
        con.execute(f"ATTACH '{SCRATCH}' AS scratch (READ_ONLY)")
        return con.execute(sql).fetchall()
    finally:
        con.close()


def picked_one(sql: str):
    rows = picked(sql)
    return rows[0][0] if rows else None


# ---------------------------------------------------------------- the world
#
# The oracle checks itself against the built data before it grades anything.

def test_the_shipped_ranking_really_does_leave_orders_level():
    """The ticket only exists because the three keys the model ranks on are not a
    unique key. If a rebuild of the world ever made them unique, every test below
    would pass on an untouched tree and grade nothing — this says so instead."""
    tied = one(CTES + "select count(*) from expected where level_with > 1")
    assert tied and tied > 0, (
        "FAIL: no order has two attempts level on day gap, outcome and attempt "
        "number, so the pick this ticket is about cannot move"
    )


def test_the_stated_tie_break_settles_every_tie():
    """`created_at` and then `intent_id` have to leave exactly one winner. If two
    attempts ever tied on all five keys the rule would not be a rule, and the
    expectation below would be picking arbitrarily itself."""
    unsettled = one(CTES + """
        , five as (
            select order_id, day_gap,
                   case outcome when 'succeeded' then 0 else 1 end as orank,
                   attempt_no, created_at, intent_id, count(*) over (
                       partition by order_id, day_gap,
                                    case outcome when 'succeeded' then 0 else 1 end,
                                    attempt_no, created_at, intent_id
                   ) as n
            from candidates
        )
        select count(*) from five where n > 1
    """)
    assert unsettled == 0, (
        f"FAIL: {unsettled} candidate rows tie on all five keys, so the rule the "
        "ticket states does not name one attempt"
    )


# ------------------------------------------------------------------- the model

def test_one_row_per_live_order():
    """The grain the ticket pins. Every mart in commerce joins this model to the
    order spine, so an order that arrives twice is counted twice everywhere."""
    rows, ids = picked("SELECT count(*), count(DISTINCT order_id) FROM scratch.picks_a")[0]
    orders = one(f"""
        SELECT count(*) FROM {ORDERS}
        WHERE NOT coalesce(is_test, false) AND deleted_at IS NULL
    """)
    assert (rows, ids) == (orders, orders), (
        f"FAIL: the model holds {rows} rows for {ids} orders, and {orders} live "
        "orders are on the spine"
    )


def test_the_orders_that_already_had_a_clear_winner_keep_it():
    """The ticket's first rule, and the only check that separates a tie-break
    added underneath the ranking from a ranking rewritten around one.

    Where the three shipped keys already name one attempt, the answer is fixed
    and nothing the ticket asks for may move it. Putting `created_at` at the
    front of the ordering, or the attempt number, changes the match on well over
    a hundred thousand orders and lands here.
    """
    moved = picked(f"""
        SELECT p.order_id, p.payment_intent_id, e.intent_id
        FROM scratch.picks_a p JOIN scratch.expected e USING (order_id)
        WHERE e.level_with = 1 AND p.payment_intent_id IS DISTINCT FROM e.intent_id
        ORDER BY 1 LIMIT 5
    """)
    total = picked_one(f"""
        SELECT count(*) FROM scratch.picks_a p JOIN scratch.expected e USING (order_id)
        WHERE e.level_with = 1 AND p.payment_intent_id IS DISTINCT FROM e.intent_id
    """)
    assert not moved, (
        f"FAIL: {total} orders whose ranking already produced one clear winner "
        f"came out with a different attempt. First five (order, model, the "
        f"attempt the shipped ranking names): {moved}"
    )


def test_the_level_orders_take_the_attempt_raised_first():
    """The ticket's second rule, and the whole of the fix.

    An order with two attempts level on the three shipped keys has to come back
    with the earlier `created_at`, and the lower `intent_id` where the instant is
    the same too. An untouched model lands here on roughly half of them, and it
    lands on a different half every time it is read, which is the complaint.
    A tie-break on `intent_id` alone lands here as well: the two orderings
    disagree on tens of thousands of these orders, and the ticket names
    `created_at` first.
    """
    wrong = picked(f"""
        SELECT p.order_id, p.payment_intent_id, e.intent_id
        FROM scratch.picks_a p JOIN scratch.expected e USING (order_id)
        WHERE e.level_with > 1 AND p.payment_intent_id IS DISTINCT FROM e.intent_id
        ORDER BY 1 LIMIT 5
    """)
    total = picked_one(f"""
        SELECT count(*) FROM scratch.picks_a p JOIN scratch.expected e USING (order_id)
        WHERE e.level_with > 1 AND p.payment_intent_id IS DISTINCT FROM e.intent_id
    """)
    assert not wrong, (
        f"FAIL: {total} orders with two attempts level on the shipped keys came "
        f"back with an attempt other than the one raised first. First five "
        f"(order, model, the attempt the rule names): {wrong}"
    )


def test_the_orders_with_no_attempt_in_the_window_still_have_none():
    """The match is a left join and it stays one. An order with nothing in its
    window publishes a null intent, and a fix that turned the join inner would
    drop three quarters of the book."""
    invented = picked_one(f"""
        SELECT count(*) FROM scratch.picks_a p
        LEFT JOIN scratch.expected e USING (order_id)
        WHERE e.order_id IS NULL AND p.payment_intent_id IS NOT NULL
    """)
    assert invented == 0, (
        f"FAIL: {invented} orders publish an attempt that falls in no window of theirs"
    )
    lost = picked_one(f"""
        SELECT count(*) FROM scratch.expected e
        LEFT JOIN scratch.picks_a p USING (order_id)
        WHERE p.order_id IS NULL
    """)
    assert lost == 0, f"FAIL: {lost} orders that match an attempt are not in the model at all"


def test_the_same_read_twice_returns_the_same_attempt():
    """The ticket in one sentence: make the pick repeatable.

    The model is read twice, the second time on one thread, and the two copies
    have to agree order for order. A ranking whose last key is not unique can
    disagree with itself here; one that ends in a unique key cannot.
    """
    disagree = picked_one("""
        SELECT count(*) FROM scratch.picks_a a JOIN scratch.picks_b b USING (order_id)
        WHERE a.payment_intent_id IS DISTINCT FROM b.payment_intent_id
    """)
    assert disagree == 0, (
        f"FAIL: two reads of the model name a different attempt on {disagree} "
        "orders, so the pick still moves between builds"
    )


def test_the_ambiguity_flag_was_not_re_cut():
    """The ticket's fifth rule. The flag is misleading and the ticket says to
    explain it rather than move it: models this task does not own read it, and
    `int_payment_matched` publishes it. It is true when more than one attempt
    falls in the window, which also pins the window itself."""
    wrong = picked_one(f"""
        SELECT count(*) FROM scratch.picks_a p
        LEFT JOIN scratch.expected e USING (order_id)
        WHERE p.payment_match_is_ambiguous
              IS DISTINCT FROM (coalesce(e.candidate_count, 0) > 1)
    """)
    assert wrong == 0, (
        f"FAIL: payment_match_is_ambiguous disagrees with 'more than one attempt "
        f"in the window' on {wrong} orders, so the flag has been re-cut"
    )


def test_the_staging_view_still_carries_every_attempt():
    """The ticket's fourth rule. Staging is 1:1 with the landed feed and other
    teams read it. Deduplicating the attempts down here would make the tie go
    away and take the duplicates out of everyone else's sight, which is a larger
    change than the one that was asked for."""
    stg = one(f"SELECT count(*) FROM {STG_INTENTS}")
    raw = one(f"SELECT count(*) FROM {INTENTS}")
    assert stg == raw, (
        f"FAIL: the payment-intent staging view returns {stg} rows and the feed "
        f"landed {raw}"
    )


def test_the_model_that_reads_the_match_still_builds():
    """`int_payment_matched` selects the six payment columns by name and every
    commerce mart is downstream of it. A pivot that drops or renames one takes
    the reader down with it, which is a larger break than the one the ticket is
    about."""
    result = dbt("run", "--select", READER)
    blob = (result.stdout or "") + (result.stderr or "")
    assert result.returncode == 0, (
        f"FAIL: the model that reads the attempt match no longer builds:\n{blob[-2000:]}"
    )
