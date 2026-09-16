"""CUS-544 — does `int_customer_lifecycle` answer as of the day it was given?

Never ships to the agent. It runs beside the scored tree, with the tree root on
`PYTHONPATH`, so it resolves the warehouse the way every DAG in the world does.

**No authored numbers.** The day this grades against is the feed's own last
order date less 150 days, read from `raw.orders` in the same session, and every
expected result is counted from `raw.orders` under the two filters
`stg_sales__orders` applies. Nothing here holds a row count, a party key or a
date typed by hand, so a rebuild of the world cannot make this verifier stale.

**Why dbt runs here.** The world ships the landed half of the warehouse only
(`AGENTS.md`, "The warehouse"), so `int_customer_lifecycle` does not exist until
dbt has made it. The `prepare` step in checks.yaml builds it and its ancestors
for the world's own `ds`. This file then builds the model a second time for the
backdated day, reads it, and builds it back. Nothing may hold the warehouse open
while dbt runs: DuckDB takes one writer.

**What is graded.** Three things, and the third is why the ticket names the
spine.

1. The nightly's answer does not move. A build for the world's own `ds` still
   counts every order in the book, on the same parties.
2. A build for a backdated day counts that day's book and no more — the party
   set, every party's orders, both rolling windows and all four date columns.
3. The order spine still publishes the whole book on a backdated build. A bound
   pushed into `int_orders_enriched` fixes the lifecycle model and takes ten
   other readers down with it.

**The keys this can derive without the tree.** A `loyalty` party's key is the
`loyalty_id` on the order and a `guest` party's key is `'GUEST-' || market_code`,
so both sets come straight out of `raw.orders`. A `trade` party's key runs
through the id crosswalk and the Northwave merge decisions, which is the tree
this file is grading, so trade parties are pinned a different way: against the
build for the world's own `ds`, which test 1 has already tied to the feed.
"""

from __future__ import annotations

import datetime as dt
import os
import subprocess

import duckdb
import pytest

from include.lib import warehouse, workspace_root

DB = str(warehouse.warehouse_path())

ORDERS = warehouse.qualify("raw.orders")
MODEL = warehouse.qualify('"int".int_customer_lifecycle')
SPINE_MODEL = warehouse.qualify('"int".int_orders_enriched')

#: The two filters `stg_sales__orders` applies and every reader of the spine
#: inherits. Written here rather than read from the staging view so the oracle
#: does not depend on the tree it is grading.
SPINE = "not coalesce(is_test, false) and deleted_at is null"

#: How far back the backdated build reaches. Far enough that a large part of the
#: book falls after it and that parties exist whose first order is still to come,
#: and taken off the feed rather than typed, so the world can be rebuilt under it.
LOOKBACK_DAYS = 150

#: The rolling window the model publishes as `orders_7d`, and the one it
#: publishes as `orders_12m`. Both are the model's own, restated here so a
#: bounded build can be checked against the feed on both sides.
WINDOW_7D = 7
WINDOW_12M = 365

#: The pinned dbt. `dbt/README.md` says a bare `dbt` is either not on the PATH
#: or is not the pinned one. The override exists so this file can be exercised
#: against a tree on a laptop; nothing in a trial or in scoring sets it.
DBT = os.environ.get("DE_BENCH_DBT", "/opt/dbt-venv/bin/dbt")
PROJECT = str(workspace_root() / "dbt" / "copperline_analytics")

#: The two models a backdated build has to produce. The spine is in the list
#: because it is a view: rebuilding it for the backdated day is the only way to
#: see whether a bound was pushed into it.
BUILD = ("int_orders_enriched", "int_customer_lifecycle")


def query(sql: str, params: list | None = None) -> list[tuple]:
    """One read, on its own connection. Nothing holds the file open: dbt wants
    the single writer and this runs between its invocations."""
    con = duckdb.connect(DB)
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
        cwd=PROJECT, capture_output=True, text=True, timeout=1200, check=False,
    )


def build(ds: str | None = None) -> subprocess.CompletedProcess:
    """Build the spine and the model, for one day or for the world's own."""
    args = ["run", "--select", *BUILD]
    if ds is not None:
        args += ["--vars", "{ds: %s}" % ds]
    return dbt(*args)


def model_built() -> bool:
    try:
        query(f"SELECT 1 FROM {MODEL} LIMIT 1")
    except duckdb.Error:
        return False
    return True


def read_model() -> dict[tuple[str, str], tuple]:
    """Every published party, keyed on the grain the model states."""
    rows = query(f"""
        SELECT party_key, party_kind, orders_lifetime, first_order_date,
               last_order_date, tenure_days, days_since_last_order,
               orders_7d, orders_12m
        FROM {MODEL}
    """)
    return {(key, kind): rest for key, kind, *rest in rows}


def spine_orders(upper: dt.date | None = None) -> int:
    where = SPINE + (" AND local_order_date <= ?" if upper else "")
    return int(one(f"SELECT count(*) FROM {ORDERS} WHERE {where}", [upper] if upper else []))


def loyalty_parties(upper: dt.date | None = None) -> dict[str, int]:
    """Every loyalty party and its orders, from the feed. A loyalty order is one
    that names no trade account, and the party key is the loyalty id itself."""
    bound = " AND local_order_date <= ?" if upper else ""
    rows = query(f"""
        SELECT loyalty_id, count(*) FROM {ORDERS}
        WHERE {SPINE} AND customer_ref IS NULL AND loyalty_id IS NOT NULL{bound}
        GROUP BY 1
    """, [upper] if upper else [])
    return {key: int(n) for key, n in rows}


def guest_parties(upper: dt.date | None = None) -> dict[str, int]:
    """Every guest bucket and its orders, from the feed. One per market that has
    taken an order naming nobody."""
    bound = " AND local_order_date <= ?" if upper else ""
    rows = query(f"""
        SELECT 'GUEST-' || market_code, count(*) FROM {ORDERS}
        WHERE {SPINE} AND customer_ref IS NULL AND loyalty_id IS NULL{bound}
        GROUP BY 1
    """, [upper] if upper else [])
    return {key: int(n) for key, n in rows}


def compare(published: dict[str, int], expected: dict[str, int]) -> tuple[list, list, list]:
    """Two party-to-orders maps, as three short lists: keys the model owes, keys
    it invented, and keys whose orders do not agree.

    The lists are built here rather than left to an `assert a == b`, because
    these maps run to a quarter of a million keys and pytest renders the whole
    difference of a failed equality. Three counts and three samples say the same
    thing in a line.
    """
    missing = sorted(set(expected) - set(published))
    extra = sorted(set(published) - set(expected))
    wrong = sorted(
        (key, published[key], expected[key])
        for key in set(expected) & set(published)
        if published[key] != expected[key]
    )
    return missing, extra, wrong


def orders_between(low: dt.date, high: dt.date) -> int:
    return int(one(
        f"SELECT count(*) FROM {ORDERS} WHERE {SPINE} "
        "AND local_order_date > ? AND local_order_date <= ?", [low, high]
    ))


@pytest.fixture(scope="session")
def facts() -> dict:
    """Both builds, measured, and the tree left as the nightly leaves it.

    dbt is run twice and the warehouse is read between the runs, never during
    one. Everything the tests below assert on is collected here, so the order
    the tests run in cannot matter.
    """
    if not model_built():
        result = build()
        assert model_built(), (
            "FAIL: int_customer_lifecycle does not build for the world's own ds, "
            f"so nothing about it can be read:\n{(result.stdout or '')[-2000:]}"
        )

    last_order = one(f"SELECT max(local_order_date) FROM {ORDERS} WHERE {SPINE}")
    probe = last_order - dt.timedelta(days=LOOKBACK_DAYS)
    probe_s = probe.isoformat()

    nightly = read_model()

    result = build(probe_s)
    assert result.returncode == 0, (
        f"FAIL: the model does not build for {probe_s}:\n{(result.stdout or '')[-2000:]}"
    )
    backdated = read_model()
    spine_on_backdated_build = int(one(f"SELECT count(*) FROM {SPINE_MODEL}"))

    result = build()
    assert result.returncode == 0, (
        "FAIL: the model does not build back for the world's own ds:\n"
        f"{(result.stdout or '')[-2000:]}"
    )

    return {
        "probe": probe_s,
        "last_order": last_order.isoformat(),
        "nightly": nightly,
        "backdated": backdated,
        "spine_on_backdated_build": spine_on_backdated_build,
        "spine_all": spine_orders(),
        "spine_to_probe": spine_orders(probe),
        "loyalty_all": loyalty_parties(),
        "loyalty_to_probe": loyalty_parties(probe),
        "guest_all": guest_parties(),
        "guest_to_probe": guest_parties(probe),
        "orders_7d_to_probe": orders_between(
            probe - dt.timedelta(days=WINDOW_7D), probe),
        "orders_12m_to_probe": orders_between(
            probe - dt.timedelta(days=WINDOW_12M), probe),
    }


# ---------------------------------------------------------------- the oracle

def test_the_feed_still_reaches_past_the_backdated_day(facts):
    """The oracle checks itself against the world before it grades anything.

    Everything below rests on a large part of the order book falling after the
    backdated day, and on there being parties whose first order is still to come
    on it. If a rebuild of the world moves either, this says so rather than
    letting the tests below pass on a feed that has nothing to hide.
    """
    assert facts["spine_all"] > 0, "FAIL: the order spine is empty"
    later = facts["spine_all"] - facts["spine_to_probe"]
    share = later / facts["spine_all"]
    assert 0.02 < share < 0.90, (
        f"FAIL: {later} of {facts['spine_all']} spine orders fall after "
        f"{facts['probe']} ({share:.1%}); the world has moved off the shape this "
        "ticket was written against"
    )
    unborn = len(facts["loyalty_all"]) - len(facts["loyalty_to_probe"])
    assert unborn > 0, (
        f"FAIL: every loyalty party had already ordered by {facts['probe']}, so "
        "a backdated build cannot publish a party that did not exist"
    )


# -------------------------------------------------------- the nightly does not move

def test_the_nightly_build_still_counts_the_whole_book(facts):
    """A build for the world's own ds publishes what it published before.

    The ticket says so in as many words, and it is the check that convicts a fix
    that bounds the wrong thing — a rolling window, a bound a day short, or the
    orders dropped rather than bounded. Every order in the spine still lands on
    exactly one party, and the loyalty and guest party sets are the feed's own.
    """
    counted = sum(row[0] for row in facts["nightly"].values())
    assert counted == facts["spine_all"], (
        f"FAIL: the nightly build counts {counted} orders against "
        f"{facts['spine_all']} in the spine; a build for the current day has to "
        "hold the whole book"
    )

    for kind, expected in (("loyalty", facts["loyalty_all"]),
                           ("guest", facts["guest_all"])):
        published = {key: row[0] for (key, k), row in facts["nightly"].items()
                     if k == kind}
        missing, extra, wrong = compare(published, expected)
        assert (len(missing), len(extra), len(wrong)) == (0, 0, 0), (
            f"FAIL: the nightly build's {kind} parties are not the feed's — "
            f"{len(missing)} missing e.g. {missing[:3]}, "
            f"{len(extra)} unexpected e.g. {extra[:3]}, "
            f"{len(wrong)} with the wrong orders e.g. {wrong[:3]}"
        )

    trade = [key for key, kind in facts["nightly"] if kind == "trade"]
    assert len(trade) > 0, "FAIL: the nightly build publishes no trade party"


# ------------------------------------------------ a backdated build is as of its day

def test_a_backdated_build_dates_nothing_after_the_day_it_was_given(facts):
    """The four date columns, and the two the ticket names by name.

    This is the fault itself: an order that had not happened on `ds` gives a
    `last_order_date` after `ds` and a `days_since_last_order` below nought, and
    a party whose first order is still to come gives a negative `tenure_days`.
    """
    probe = facts["probe"]
    late_last = [k for k, row in facts["backdated"].items()
                 if row[2] is not None and str(row[2]) > probe]
    late_first = [k for k, row in facts["backdated"].items()
                  if row[1] is not None and str(row[1]) > probe]
    negative_recency = [k for k, row in facts["backdated"].items()
                        if row[4] is not None and row[4] < 0]
    negative_tenure = [k for k, row in facts["backdated"].items()
                       if row[3] is not None and row[3] < 0]

    assert len(late_last) == 0, (
        f"FAIL: {len(late_last)} parties on the {probe} build carry a last order "
        f"date after {probe}, e.g. {late_last[:3]}"
    )
    assert len(late_first) == 0, (
        f"FAIL: {len(late_first)} parties on the {probe} build carry a first order "
        f"date after {probe}, e.g. {late_first[:3]}"
    )
    assert len(negative_recency) == 0, (
        f"FAIL: {len(negative_recency)} parties on the {probe} build carry a "
        f"negative days_since_last_order, e.g. {negative_recency[:3]}"
    )
    assert len(negative_tenure) == 0, (
        f"FAIL: {len(negative_tenure)} parties on the {probe} build carry a "
        f"negative tenure_days, e.g. {negative_tenure[:3]}"
    )


def test_a_backdated_build_publishes_the_parties_that_had_ordered_and_no_others(facts):
    """The party set, exactly, for the two kinds the feed settles on its own.

    A loyalty key and a guest key are both readable straight off `raw.orders`, so
    these two sets are the feed's answer rather than the tree's. Set equality
    convicts the fault in both directions: a party whose orders are all still to
    come must not be published, and a party that had ordered must not be dropped
    by a bound a day short or a window that cuts the early years off.
    """
    probe = facts["probe"]
    for kind, expected in (("loyalty", facts["loyalty_to_probe"]),
                           ("guest", facts["guest_to_probe"])):
        published = {key: row[0] for (key, k), row in facts["backdated"].items()
                     if k == kind}
        missing, extra, wrong = compare(published, expected)
        assert (len(missing), len(extra), len(wrong)) == (0, 0, 0), (
            f"FAIL: the {probe} build's {kind} parties are not the ones that had "
            f"ordered by {probe} — {len(missing)} missing e.g. {missing[:3]}, "
            f"{len(extra)} published that had not ordered e.g. {extra[:3]}, "
            f"{len(wrong)} carrying orders that are not that day's e.g. {wrong[:3]}"
        )


def test_a_backdated_build_publishes_the_trade_parties_that_had_ordered(facts):
    """The trade side, pinned against the nightly build rather than the feed.

    A trade party's key runs through the id crosswalk and the Northwave merge
    decisions, which is the tree this file is grading, so it is not re-derived
    here.
    What is asserted instead: the trade parties on a backdated build are exactly
    the trade parties the nightly publishes whose first order falls on or before
    the backdated day — and the nightly build is already tied to the feed above.
    """
    probe = facts["probe"]
    expected = {key for (key, kind), row in facts["nightly"].items()
                if kind == "trade" and row[1] is not None and str(row[1]) <= probe}
    published = {key for key, kind in facts["backdated"] if kind == "trade"}
    missing = sorted(expected - published)
    extra = sorted(published - expected)
    assert (len(missing), len(extra)) == (0, 0), (
        f"FAIL: the {probe} build's trade parties are not the ones that had "
        f"ordered by {probe} — {len(missing)} missing e.g. {missing[:3]}, "
        f"{len(extra)} published that had not ordered e.g. {extra[:3]}"
    )


def test_a_backdated_build_counts_that_days_book_and_its_windows(facts):
    """The totals, on the feed's own arithmetic.

    `orders_lifetime` over every party is the whole spine as of the day, because
    every order lands on exactly one party. The two rolling counts are the same
    statement inside a window, and they are here because the fault reached them
    too: `orders_7d` was bounded below and not above, so on a replayed day it
    counted every order from that day to the end of the feed.
    """
    probe = facts["probe"]
    for column, index, expected in (
        ("orders_lifetime", 0, facts["spine_to_probe"]),
        ("orders_7d", 5, facts["orders_7d_to_probe"]),
        ("orders_12m", 6, facts["orders_12m_to_probe"]),
    ):
        counted = sum(row[index] or 0 for row in facts["backdated"].values())
        assert counted == expected, (
            f"FAIL: the {probe} build's {column} adds to {counted} against "
            f"{expected} orders in the feed as of {probe}"
        )


# ------------------------------------------------------ the spine keeps the book

def test_the_order_spine_still_publishes_the_whole_book_on_a_backdated_build(facts):
    """The bound belongs to the model, not to the spine.

    `int_orders_enriched` is read by ten other models and a data test, none of
    which is asking an as-of question, and the ticket says it keeps the whole
    book. This rebuilds it for the backdated day and counts it: a `where
    order_date <= ds` pushed down there answers CUS-544 and quietly truncates
    every other reader on the same run.
    """
    assert facts["spine_on_backdated_build"] == facts["spine_all"], (
        f"FAIL: built for {facts['probe']}, int_orders_enriched publishes "
        f"{facts['spine_on_backdated_build']} orders against {facts['spine_all']} "
        "in the feed — the bound has been pushed into the order spine, which "
        "every other reader of it shares"
    )


# ------------------------------------------------------------ the model's own tests

def test_the_model_still_passes_its_own_tests(facts):
    """`dbt test --select int_customer_lifecycle`, clean, on the rebuilt nightly.

    `first_order_date` and `orders_lifetime` both carry a `not_null` and the
    grain carries a uniqueness test. A fix that empties a column or splits a
    party is caught here.
    """
    result = dbt("test", "--select", "int_customer_lifecycle")
    blob = (result.stdout or "") + (result.stderr or "")
    assert result.returncode == 0, (
        "FAIL: the model's own tests do not pass after the change:\n" + blob[-2000:]
    )
