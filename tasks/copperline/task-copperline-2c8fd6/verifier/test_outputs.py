"""PLAT-509: the amendment, judged by the loader that has to read it.

Nothing here reads the contract file as text. Every test loads it through
`include.lib.contracts` — the module `plat_contracts_enforce` calls — and then
runs `contracts.check` against a mart this file stands up itself. That is the
whole point of the ticket: a clause the loader does not look at enforces
nothing, and only the loader can say which clauses those are.

The mart is stood up rather than read, because `marts.*` is dbt output and a
trial tree carries the landed half of the warehouse only. It is stood up in a
scratch DuckDB file named `copperline.duckdb`, because `warehouse.qualify`
builds `<db-stem>.<schema>.<table>` from `DUCKDB_PATH` and the check's SQL is
written against that name.

ONE AUTHORED SET, CROSS-CHECKED. `CHANNELS` is the four selling channels. It is
authored so that a world that opens a fifth fails this file loudly instead of
grading a stale list, and the first test compares it against `raw.orders` in the
warehouse the scorer rebuilt from the image. Its other source in the tree is
`dbt/copperline_analytics/models/shared/dims/dim_channel.sql`, the register,
which holds the same four rows.

The rest of the authored values — the columns, the grain, the three assertions,
and which other contracts declare a list of allowed values — are the shipped
`contracts/*.yml` read off the page, and the ticket says none of them moves.
"""

from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

import duckdb
import pytest

# The scorer lays the tree at /work and runs pytest there. The override exists
# so this file can be exercised against a tree on a laptop; nothing in a trial
# or in scoring sets it.
WORKDIR = Path(os.environ.get("DE_BENCH_WORKDIR") or "/work")
DB = WORKDIR / os.environ.get("DUCKDB_PATH", "include/data/copperline.duckdb")

if str(WORKDIR) not in sys.path:
    sys.path.insert(0, str(WORKDIR))

#: The four selling channels. `dim_channel` is the register; `raw.orders` is the
#: order book, and the first test holds the two together.
CHANNELS = ("marketplace", "store", "trade", "web")

#: A typo for `web`. No feed sends it, and catching one is what the rule under
#: test is for.
UNKNOWN = "wbe"

#: The contract's published surface, in order, as C-4 built on it.
COLUMNS = (
    "order_id",
    "order_date",
    "channel",
    "booked_cents",
    "net_sales_cents",
    "merch_margin_cents",
)
GRAIN = ["order_id"]
UNIQUE = ["order_id"]
NOT_NULL = ["order_id", "order_date", "channel", "booked_cents", "net_sales_cents"]
NO_FLOATS_IN = ["booked_cents", "net_sales_cents", "merch_margin_cents"]

#: Every other contract that pins a list of allowed values, and the columns it
#: pins one on. The quiet rule silenced all of these too, so the fix has to
#: leave them where they are rather than reach for the rule.
OTHERS = {
    "account_rollup": ["entity"],
    "audience_segments": ["consent_state"],
    "customer_360": ["source_book", "status"],
    "revenue_recognized_monthly": ["entity"],
}


def library():
    """`include.lib.contracts`, from the tree under test."""
    from include.lib import contracts

    return contracts


def contract():
    """`contracts/order_economics.yml`, as the morning run loads it."""
    return library().load("order_economics")


def order_book_channels() -> list[str]:
    """Every channel the order book holds, from the warehouse."""
    con = duckdb.connect(str(DB), read_only=True)
    try:
        rows = con.execute(
            "SELECT DISTINCT channel FROM raw.orders WHERE channel IS NOT NULL"
        ).fetchall()
    finally:
        con.close()
    return sorted(str(row[0]) for row in rows)


def violations(channels) -> list:
    """Stand up `marts.order_economics` holding one order per channel given,
    and run the contract against it.

    Every other column is filled to satisfy every other clause — one row per
    order id, nothing null, money in integer cents — so that what comes back is
    about the channel list and nothing else.
    """
    contracts = library()
    with tempfile.TemporaryDirectory() as scratch:
        con = duckdb.connect(str(Path(scratch) / "copperline.duckdb"))
        try:
            con.execute("CREATE SCHEMA marts")
            con.execute(
                "CREATE TABLE marts.order_economics ("
                "  order_id VARCHAR, order_date DATE, channel VARCHAR,"
                "  booked_cents BIGINT, net_sales_cents BIGINT,"
                "  merch_margin_cents BIGINT)"
            )
            for i, channel in enumerate(channels):
                con.execute(
                    "INSERT INTO marts.order_economics VALUES (?, DATE '2026-06-01', ?, ?, ?, ?)",
                    [f"ORD-{i:06d}", channel, 1000 + i, 900 + i, 100 + i],
                )
            return list(contracts.check(contract(), con))
        finally:
            con.close()


def channel_violations(channels) -> list:
    return [v for v in violations(channels) if v.rule == "accepted_values"]


# --- the world says what the answer is -------------------------------------

def test_the_channel_set_this_file_grades_against_is_still_the_whole_order_book():
    """The oracle checks itself before it grades anything. If a later change
    opens a fifth channel, the set above stops being what the order book holds
    and this test says so, rather than the rest of the file quietly grading a
    list the business has moved past."""
    assert order_book_channels() == sorted(CHANNELS)


def test_the_contract_is_clean_on_the_three_channels_it_already_allowed():
    """Two things at once. It says the scaffolding below satisfies every clause
    except the one under test, so a violation reported later is about the
    channel list. And it says the amendment took nothing away: a contract that
    stopped admitting `store` would fail here."""
    reported = violations(["store", "web", "marketplace"])
    assert not reported, "; ".join(str(v) for v in reported)


# --- the amendment ----------------------------------------------------------

def test_the_contract_admits_every_channel_the_order_book_holds():
    """The whole ask. The mart publishes four channels, the contract allowed
    three, and the morning run has to stop reporting a mart that is right.

    An unamended contract fails here on `trade`."""
    reported = channel_violations(CHANNELS)
    assert not reported, "; ".join(str(v) for v in reported)


def test_the_channel_rule_still_bites():
    """And it has to stop reporting it for the right reason.

    A contract with the list taken off, or with it written somewhere the loader
    does not read, is green on every mart there could ever be — including one
    that publishes a typo. The rule reports one violation per column and names
    the values it found in it, so the detail is what this reads: the typo has to
    be in it and no channel the business sells may be."""
    reported = channel_violations(list(CHANNELS) + [UNKNOWN])
    assert reported, (
        f"a mart publishing {UNKNOWN!r} came back clean; the channel column pins "
        "no list the loader reads"
    )
    detail = "; ".join(v.detail for v in reported)
    assert UNKNOWN in detail, detail
    for channel in CHANNELS:
        assert f"'{channel}'" not in detail, (
            f"{channel} is in the order book and the contract still calls it a "
            f"violation: {detail}"
        )


def test_the_allowed_list_is_the_order_book_and_not_a_wider_one():
    """A list that admits a channel nobody sells has stopped saying anything.
    The allowed set is exactly what the order book holds, read from the same
    warehouse the mart is built from — and it is read off the column, because
    that is the only place `include/lib/contracts.py` looks for it."""
    channel = contract().column("channel")
    assert channel is not None, "the contract no longer publishes a channel column"
    allowed = channel.get("accepted_values")
    assert allowed, (
        "the channel column declares no accepted_values; the loader reads the "
        "list from the column and nowhere else"
    )
    assert sorted(str(value) for value in allowed) == order_book_channels()


# --- and nothing else in the contract moved --------------------------------

def test_the_contract_still_promises_what_c4_built_on():
    """The grain and the published surface. The cheapest way to make a contract
    stop complaining is to make it promise less, and MD-1 and MD-2 in
    contracts/merch-dashboards.md say what breaks when it does."""
    subject = contract()
    assert subject.model == "marts.order_economics"
    assert subject.consumer == "C-4"
    assert subject.owner == "commerce"
    assert subject.grain == GRAIN
    assert tuple(subject.column_names) == COLUMNS
    required = {c["name"] for c in subject.columns if c.get("required")}
    assert required == set(COLUMNS) - {"merch_margin_cents"}


@pytest.mark.parametrize(
    "rule,argument",
    [("unique", UNIQUE), ("not_null", NOT_NULL), ("no_floats_in", NO_FLOATS_IN)],
)
def test_the_three_assertions_are_still_declared(rule, argument):
    """Each of the three, at the columns it named. `plat_contracts_enforce` runs
    whatever is here; a rule dropped from this list is a rule nobody runs and
    nobody notices is gone."""
    declared = {name: value for test in contract().tests for name, value in test.items()}
    assert rule in declared, f"{rule} is no longer declared"
    assert list(declared[rule]) == argument


# --- the same rule, in the other four contracts ----------------------------

def test_the_other_allowed_value_lists_are_where_they_were():
    """The quiet rule was not the channel list's alone: four other contracts
    pin a list of allowed values and none of them has been enforced either. The
    fix belongs to this contract, so those four come out of it unchanged. A fix
    aimed at the rule rather than at the contract shows up here."""
    contracts = library()
    declaring = {}
    for name in contracts.names():
        columns = sorted(
            column["name"]
            for column in contracts.load(name).columns
            if column.get("accepted_values")
        )
        if columns:
            declaring[name] = columns
    assert declaring == {**OTHERS, "order_economics": ["channel"]}
