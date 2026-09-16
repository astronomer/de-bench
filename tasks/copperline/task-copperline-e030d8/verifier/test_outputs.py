"""CUS-441 — does the repeat-purchase line put the day's orders under the
right population?

Never ships to the agent. It runs beside the scored tree, with the tree root on
`PYTHONPATH`, so it imports the delivered module the same way a DAG would.

**Why a verifier rather than a DAG run.** The world ships the landed half of
the warehouse only (`AGENTS.md`, "The warehouse"): every `marts.*` model is
derived and none of them is on disk. The ticket asks for a builder and says
there is no DAG yet, so there is nothing to run and nothing to wait for — the
verifier calls `build_daily` directly, which grades the same code in seconds.
It also removes the shortcut: a day typed into the table by hand is a day the
next build replaces.

**No authored figures.** Every expected result below is computed from
`raw.orders` in the same session. The only work done in SQL is one group-by
for the earliest order date each member holds; the classification the ticket is
actually about — guest against member, new against repeat — is walked in
Python here, over the day's own rows, so the answer key and the delivered build
do not share a code path.

**Three days, on purpose.**

    2026-03-11  inside the week the ticket names, and a day it does not name
    2025-09-17  five months before that week
    2026-05-06  eight weeks after it, and named nowhere

A build that special-cases the ticket's week passes the first and fails the
other two. All three sit outside the world's era boundaries and outside the
three days every market is shut, so all three carry orders on all three
channels — `test_the_graded_days_can_tell_the_readings_apart` re-measures that
rather than trusting this paragraph.

**The tolerance.** Every count is graded exactly. `repeat_order_share_bps`
carries `BPS_TOLERANCE`, two basis points, because the ticket fixes the
fraction and not the rounding mode, and the two defensible modes cannot differ
by more than one. The reading the column exists to separate — dividing by the
day's orders rather than by the member orders — is about fifteen hundred basis
points away, and the guard test measures that gap every run.
"""

from __future__ import annotations

import datetime as dt
from decimal import Decimal, ROUND_HALF_UP
from functools import lru_cache

import duckdb

# The scored tree is on PYTHONPATH (scoring.build_score_env), so the house
# library resolves the warehouse the same way every DAG in the world does —
# `DUCKDB_PATH` against the root that holds `include/`, whatever the working
# directory happens to be.
from include.lib import warehouse

DB = str(warehouse.warehouse_path())

# Every name is qualified onto the live warehouse, for the reason
# `include/lib/warehouse.py` gives: the frozen `nwv` copy sits first on the
# search path and carries `raw.orders` under the same name, so an unqualified
# read would answer with September 2025's Northwave numbers.
ORDERS = warehouse.qualify("raw.orders")
PAGE = warehouse.qualify("marts.repeat_purchase_daily")

#: The days the build is measured on. See the module docstring.
IN_WEEK = dt.date(2026, 3, 11)
BEFORE = dt.date(2025, 9, 17)
LATER = dt.date(2026, 5, 6)
GRADED = (IN_WEEK, BEFORE, LATER)

#: The population the ticket names.
CHANNELS = ("marketplace", "store", "web")

#: The columns the ticket asks for, in the order this file compares them.
FIGURES = ("order_count", "guest_orders", "member_orders", "new_member_orders",
           "repeat_member_orders", "distinct_members")

#: See "The tolerance" in the module docstring.
BPS_TOLERANCE = 2

#: What staging drops and nothing else does: the synthetic orders and the rows
#: the OMS soft-deleted. The ticket says one population, read twice, so this
#: clause is on the day and on the first-order lookup both.
_KEPT = ("o.channel IN ('marketplace', 'store', 'web') "
         "AND NOT coalesce(o.is_test, false) AND o.deleted_at IS NULL")


def query(sql: str, params: list | None = None) -> list[tuple]:
    """One read, on its own connection. DuckDB takes one writer and the build
    under test opens its own, so nothing here may hold the file open. The
    connection is not read-only: DuckDB refuses a second connection to a file
    this process already opened under a different configuration."""
    con = duckdb.connect(DB)
    try:
        return con.execute(sql, params or []).fetchall()
    finally:
        con.close()


# --------------------------------------------------------------------------
# The expected page, from the order spine
# --------------------------------------------------------------------------

@lru_cache(maxsize=1)
def first_order_dates() -> dict[str, dt.date]:
    """The earliest order each member holds, over the whole feed.

    One group-by, and the only part of the answer key written in SQL. The
    ticket's rules about what is new and what is repeat are applied in Python
    below, against these dates.
    """
    rows = query(
        f"SELECT o.loyalty_id, min(o.local_order_date) FROM {ORDERS} o "
        f"WHERE {_KEPT} AND o.loyalty_id IS NOT NULL GROUP BY 1"
    )
    return {member: day for member, day in rows}


@lru_cache(maxsize=None)
def day_orders(day: dt.date) -> list[tuple[str, str | None]]:
    """One entry per order the day holds: its channel and its loyalty id."""
    return query(
        f"SELECT o.channel, o.loyalty_id FROM {ORDERS} o "
        f"WHERE {_KEPT} AND o.local_order_date = ?",
        [day],
    )


@lru_cache(maxsize=None)
def expected(day: dt.date) -> dict[str, dict[str, int]]:
    """The whole page for one day, walked order by order.

    A guest order is an order with no loyalty id. It counts once, in
    `order_count` and in `guest_orders`, and it reaches no other column. A
    member order is new when the member's earliest order is this day and repeat
    when it is earlier — the same-day tie goes to new, which is what the ticket
    says.
    """
    firsts = first_order_dates()
    out: dict[str, dict[str, int]] = {}
    members: dict[str, set[str]] = {}
    for channel, member in day_orders(day):
        row = out.setdefault(channel, dict.fromkeys(FIGURES, 0))
        row["order_count"] += 1
        if member is None:
            row["guest_orders"] += 1
            continue
        row["member_orders"] += 1
        if firsts[member] == day:
            row["new_member_orders"] += 1
        else:
            row["repeat_member_orders"] += 1
        members.setdefault(channel, set()).add(member)
    for channel, row in out.items():
        row["distinct_members"] = len(members.get(channel, ()))
    return out


def basis_points(part: int, whole: int) -> int | None:
    """The share, half up, to the basis point."""
    if whole == 0:
        return None
    return int((Decimal(part) * 10000 / Decimal(whole)).quantize(
        Decimal("1"), rounding=ROUND_HALF_UP))


# --------------------------------------------------------------------------
# The page under test
# --------------------------------------------------------------------------

def build(day: dt.date) -> None:
    """Run the delivered build for one day, the way a DAG task would."""
    from projects.customer.lib import repeat

    repeat.build_daily(day.isoformat())


def built(day: dt.date) -> dict[str, dict[str, int]]:
    rows = query(
        f"SELECT channel, {', '.join(FIGURES)}, repeat_order_share_bps "
        f"FROM {PAGE} WHERE ds = ? ORDER BY channel",
        [day],
    )
    assert rows, f"FAIL: {day}: the page holds no row for that day"
    channels = [row[0] for row in rows]
    assert len(channels) == len(set(channels)), (
        f"FAIL: {day}: a channel appears more than once: {sorted(channels)}"
    )
    return {
        row[0]: {
            **{name: int(value) for name, value in zip(FIGURES, row[1:-1])},
            "repeat_order_share_bps": None if row[-1] is None else int(row[-1]),
        }
        for row in rows
    }


def check_day(day: dt.date) -> None:
    want = expected(day)
    build(day)
    got = built(day)

    assert set(got) == set(want), (
        f"FAIL: {day}: the page holds {sorted(got)} and the order spine says "
        f"{sorted(want)}"
    )

    wrong: list[str] = []
    for channel in sorted(want):
        for name in FIGURES:
            mine, theirs = got[channel][name], want[channel][name]
            if mine != theirs:
                wrong.append(f"{channel}.{name} is {mine} and the spine says "
                             f"{theirs} (out by {mine - theirs})")
        share = basis_points(want[channel]["repeat_member_orders"],
                             want[channel]["member_orders"])
        mine = got[channel]["repeat_order_share_bps"]
        if share is None:
            if mine is not None:
                wrong.append(f"{channel}.repeat_order_share_bps is {mine} on a "
                             "channel with no member orders")
        elif mine is None or abs(mine - share) > BPS_TOLERANCE:
            wrong.append(f"{channel}.repeat_order_share_bps is {mine} and the "
                         f"spine says {share}")
    assert not wrong, f"FAIL: {day}: " + "; ".join(wrong)


# --------------------------------------------------------------------------
# The tests
# --------------------------------------------------------------------------

def test_the_graded_days_can_tell_the_readings_apart():
    """The fixture's own guard. Each graded day has to separate the right
    reading from the wrong ones on every channel, or the day proves nothing.
    Re-measured every run, so a change to the world that flattened one of these
    gaps fails here rather than passing everybody quietly."""
    for day in GRADED:
        want = expected(day)
        missing = sorted(set(CHANNELS) - set(want))
        assert not missing, (
            f"FAIL: {day}: no orders on {', '.join(missing)}, so the day "
            "cannot grade a channel the page is supposed to hold"
        )
        for channel in CHANNELS:
            row = want[channel]
            # Guests dropped by an inner join, or folded onto one identity, or
            # given one identity each: all three move a column by this much.
            assert row["guest_orders"] > 0, (
                f"FAIL: {day}: {channel} holds no guest order, so the day "
                "cannot grade how guests are treated"
            )
            # A first-order lookup scoped to the day calls every member new; a
            # whole-history one does not. Both columns have to be occupied for
            # the day to tell them apart.
            assert row["new_member_orders"] > 0, (
                f"FAIL: {day}: {channel} holds no first-time member, so a "
                "day-scoped lookup and a whole-history one agree here"
            )
            assert row["repeat_member_orders"] > 0, (
                f"FAIL: {day}: {channel} holds no returning member"
            )
            # The two denominators the ticket rules between.
            by_members = basis_points(row["repeat_member_orders"],
                                      row["member_orders"])
            by_day = basis_points(row["repeat_member_orders"],
                                  row["order_count"])
            assert abs(by_members - by_day) > BPS_TOLERANCE, (
                f"FAIL: {day}: {channel}: the two denominators land {by_members} "
                f"and {by_day}, inside the tolerance, so the share cannot be graded"
            )


def test_a_day_inside_the_week_the_ticket_names():
    check_day(IN_WEEK)


def test_a_day_five_months_before_that_week():
    check_day(BEFORE)


def test_a_day_the_ticket_never_mentions():
    check_day(LATER)


def test_the_two_identities_hold_on_the_page():
    """The ticket's two arithmetic rules, on the page's own numbers. A row
    whose parts do not add to its whole has lost a population somewhere between
    them, whatever the totals say."""
    for day in GRADED:
        build(day)
        for channel, row in sorted(built(day).items()):
            assert row["order_count"] == row["guest_orders"] + row["member_orders"], (
                f"FAIL: {day}: {channel}: {row['guest_orders']} guest orders and "
                f"{row['member_orders']} member orders are not {row['order_count']}"
            )
            assert (row["member_orders"]
                    == row["new_member_orders"] + row["repeat_member_orders"]), (
                f"FAIL: {day}: {channel}: {row['new_member_orders']} new and "
                f"{row['repeat_member_orders']} repeat are not "
                f"{row['member_orders']} member orders"
            )


def test_only_the_consumer_channels_land():
    """The ticket's population. The trade channel is another team's question,
    and a trade order always names an account, so leaving it in both widens the
    page and flatters every share on it."""
    for day in GRADED:
        build(day)
        extra = sorted(set(built(day)) - set(CHANNELS))
        assert not extra, f"FAIL: {day}: the page holds {', '.join(extra)}"


def test_rebuilding_a_day_leaves_one_copy():
    """A page that has to be rebuilt must not end up with the day twice."""
    build(BEFORE)
    once = built(BEFORE)
    build(BEFORE)
    assert built(BEFORE) == once, f"FAIL: {BEFORE}: the second build changed the day"
    rows = query(f"SELECT count(*) FROM {PAGE} WHERE ds = ?", [BEFORE])
    assert rows[0][0] == len(once), (
        f"FAIL: {BEFORE}: the page holds {rows[0][0]} rows for {len(once)} channels"
    )


def test_a_rebuild_leaves_the_other_days_alone():
    """One day is one partition. Emptying the table before each build gives a
    page that only ever holds the day somebody ran last."""
    for day in GRADED:
        build(day)
    held = {row[0] for row in query(f"SELECT DISTINCT ds FROM {PAGE}")}
    missing = sorted(str(day) for day in GRADED if day not in held)
    assert not missing, f"FAIL: the page lost {', '.join(missing)} to a later build"
