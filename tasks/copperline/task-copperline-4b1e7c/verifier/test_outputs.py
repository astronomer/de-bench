"""CUS-233 — does the backfill put each order under the account that owns it?

Never ships to the agent. It runs beside the scored tree, after
`cus_crosswalk_backfill` has been run once, and reads what that run landed in
`ops.customer_ref_resolved`.

**No authored numbers.** Every expected result is computed from `raw.orders`
and `raw.customer_id_map` in the same session. The answer is graded against the
world's own landed tables, never against a figure typed here.

**The two readings the window separates.** The re-key moved trade accounts from
`CUST####` to `C-######` on one day, and two facts about the landed data decide
whether a backfill is right:

    the late old-format refs   orders dated after the re-key that still carry
                               an old-format reference. A crosswalk applied by
                               the order's date leaves these on an id their
                               account no longer trades under.
    the untranslatable refs    legacy ids whose map row carries no current
                               account. Taking the mapped id without a fallback
                               turns every one of them null, and filtering them
                               out loses the orders altogether.

Both populations sit inside 2024-09-01..2024-12-31, which is why the ticket
asks for that window. `test_the_window_separates_the_readings` is the oracle's
own guard: it fails loudly if a change to the world empties either population,
rather than letting the rest of this file grade a distinction that no longer
exists.

The table is read whole rather than through the window, so a backfill that did
more than it was asked is not convicted for it — only the orders the ticket
named are compared.
"""

from __future__ import annotations

import duckdb
import pytest

# The scored tree is on PYTHONPATH (scoring.build_score_env), so the house
# library resolves the warehouse the same way every DAG in the world does.
from include.lib import warehouse

DB = str(warehouse.warehouse_path())

# Every name is qualified onto the live warehouse, for the reason
# `include/lib/warehouse.py` gives: the frozen `nwv` copy sits first on the
# search path and carries `raw.orders` under the same name, so an unqualified
# read would answer with September 2025's Northwave numbers.
ORDERS = warehouse.qualify("raw.orders")
CROSSWALK = warehouse.qualify("raw.customer_id_map")
RESOLVED = warehouse.qualify("ops.customer_ref_resolved")

#: The window the ticket names.
FROM_DS = "2024-09-01"
TO_DS = "2024-12-31"

WINDOW = f"o.local_order_date BETWEEN DATE '{FROM_DS}' AND DATE '{TO_DS}'"

#: What the backfill is graded against: the reference resolved by the format it
#: is written in, with a legacy id the map cannot translate kept as it stands.
TRUTH = f"""
SELECT o.order_id,
       o.customer_ref,
       coalesce(m.customer_id, o.customer_ref) AS customer_id,
       o.local_order_date                      AS ds
FROM {ORDERS} o
LEFT JOIN {CROSSWALK} m
       ON o.customer_ref = m.legacy_id
      AND o.customer_ref LIKE 'CUST%'
WHERE {WINDOW}
  AND o.customer_ref IS NOT NULL
"""

#: Orders dated on or after the re-key that still carry an old-format
#: reference. The day itself comes from the map, not from this file.
LATE_OLD_FORMAT = f"""
SELECT o.order_id
FROM {ORDERS} o
WHERE {WINDOW}
  AND o.customer_ref LIKE 'CUST%'
  AND o.local_order_date >= (SELECT cast(min(migrated_at) AS DATE) FROM {CROSSWALK})
"""

#: Orders whose reference has a map row with no current account behind it.
UNTRANSLATABLE = f"""
SELECT o.order_id
FROM {ORDERS} o
JOIN {CROSSWALK} m ON m.legacy_id = o.customer_ref
WHERE {WINDOW}
  AND m.customer_id IS NULL
"""


def query(sql: str, params: list | None = None) -> list[tuple]:
    """One read, on its own connection.

    Opened writable rather than read-only: DuckDB refuses a read-only open on a
    file that still has a write-ahead log to replay, and the backfill run that
    goes before this leaves one.
    """
    con = duckdb.connect(DB)
    try:
        return con.execute(sql, params or []).fetchall()
    finally:
        con.close()


@pytest.fixture(scope="session")
def expected() -> dict[str, tuple[str, str]]:
    """order_id -> (the reference written, the account it belongs to today)."""
    return {row[0]: (row[1], row[2]) for row in query(TRUTH)}


@pytest.fixture(scope="session")
def landed() -> dict[str, tuple[str, str, object]]:
    """What the backfill wrote, whole. order_id -> (ref, account, ds).

    Read outside the window on purpose: a run that filled more history than the
    ticket asked for is not wrong, and only the named orders are compared.
    """
    tables = query(
        "SELECT count(*) FROM duckdb_tables() "
        "WHERE schema_name = 'ops' AND table_name = 'customer_ref_resolved'"
    )
    assert tables[0][0], (
        "FAIL: ops.customer_ref_resolved is not in the warehouse; the backfill "
        "landed nothing"
    )
    rows = query(
        f"SELECT order_id, customer_ref, customer_id, cast(ds AS DATE) FROM {RESOLVED}"
    )
    assert rows, "FAIL: ops.customer_ref_resolved is empty"
    return {row[0]: (row[1], row[2], row[3]) for row in rows}


def rows_written() -> int:
    return query(f"SELECT count(*) FROM {RESOLVED}")[0][0]


def test_the_window_separates_the_readings():
    """The oracle checks the world before it grades anything.

    The window has to hold late old-format references and references the map
    cannot translate. Without both, this file grades a distinction the data no
    longer makes, and would pass a backfill that gets either rule wrong.
    """
    late = query(f"SELECT count(*) FROM ({LATE_OLD_FORMAT})")[0][0]
    orphaned = query(f"SELECT count(*) FROM ({UNTRANSLATABLE})")[0][0]
    assert late, (
        "FAIL: no order in the window carries an old-format reference after the "
        "re-key, so a crosswalk applied by date would grade the same as one "
        "applied by format"
    )
    assert orphaned, (
        "FAIL: no order in the window carries a reference the map cannot "
        "translate, so dropping the fallback would grade the same as keeping it"
    )


def test_every_order_in_the_window_that_carries_a_reference_has_a_row(expected, landed):
    """The ticket's population: every order carrying a reference, the test rows
    and the ones the OMS deleted included. A backfill that took staging's
    filters instead loses orders support will ask about."""
    missing = sorted(set(expected) - set(landed))
    assert not missing, (
        f"FAIL: {len(missing)} order(s) in the window carry a reference and have "
        f"no row, first few: {missing[:5]}"
    )


def test_one_row_per_order(landed):
    """The grain. Two rows for one order is a join that fanned out, and support
    reads the count."""
    assert rows_written() == len(landed), (
        f"FAIL: {rows_written()} rows over {len(landed)} orders"
    )


def test_no_row_carries_a_null_account(landed):
    """The ticket says `customer_id` is never null. A null is how an account
    that never reached the new platform disappears out of its own history.

    Only the rows that landed are looked at: an order with no row at all is the
    coverage test's finding, and blaming it here would name the wrong cause.
    """
    empty = sorted(order for order, row in landed.items() if row[1] is None)
    assert not empty, (
        f"FAIL: {len(empty)} order(s) landed with no account, first few: {empty[:5]}"
    )


def test_the_reference_column_holds_the_reference_the_oms_wrote(expected, landed):
    """`customer_ref` is the reference as written, not the resolved id. Support
    reads both columns to explain why an old invoice says something else."""
    wrong = [
        f"{order}: wrote {landed[order][0]!r}, the OMS wrote {ref!r}"
        for order, (ref, _) in expected.items()
        if order in landed and landed[order][0] != ref
    ]
    assert not wrong, f"FAIL: {len(wrong)} reference(s) rewritten, first few: {wrong[:3]}"


def test_the_partition_key_is_the_order_date(expected, landed):
    """`ds` is the order's date. A run that stamped its own date instead leaves
    a table nobody can replay one day of."""
    dates = {
        row[0]: row[1]
        for row in query(
            f"SELECT o.order_id, o.local_order_date FROM {ORDERS} o "
            f"WHERE {WINDOW} AND o.customer_ref IS NOT NULL"
        )
    }
    wrong = [
        f"{order}: ds {landed[order][2]}, ordered {dates[order]}"
        for order in expected
        if order in landed and landed[order][2] != dates[order]
    ]
    assert not wrong, f"FAIL: {len(wrong)} row(s) on the wrong day, first few: {wrong[:3]}"


def test_the_old_format_references_that_landed_after_the_re_key_reach_their_current_account(expected, landed):
    """The store integration kept writing old-format references for two weeks
    after the re-key. Those orders are real and their accounts are current, so
    the reference resolves by the format it is written in. A backfill that
    switched on the order's date instead leaves every one of them filed under
    an id its account stopped trading under."""
    late = [row[0] for row in query(LATE_OLD_FORMAT)]
    wrong = [
        f"{order}: filed under {landed[order][1]!r}, the account is {expected[order][1]!r}"
        for order in late
        if order in landed and order in expected and landed[order][1] != expected[order][1]
    ]
    assert not wrong, (
        f"FAIL: {len(wrong)} of {len(late)} order(s) written in the old scheme after "
        f"the re-key are on the wrong account, first few: {wrong[:3]}"
    )


def test_the_accounts_the_crosswalk_cannot_translate_keep_the_id_they_traded_under(expected, landed):
    """Some legacy ids have a map row and no current account behind it. Their
    orders still happened and still belong to somebody, so they stay on the id
    they were placed under. Taking the mapped id without a fallback empties
    them, and filtering them out loses them."""
    orphaned = [row[0] for row in query(UNTRANSLATABLE)]
    lost = [order for order in orphaned if order not in landed]
    wrong = [
        f"{order}: filed under {landed[order][1]!r}, placed under {expected[order][1]!r}"
        for order in orphaned
        if order in landed and order in expected and landed[order][1] != expected[order][1]
    ]
    assert not lost, (
        f"FAIL: {len(lost)} of {len(orphaned)} order(s) on an account the map cannot "
        f"translate have no row at all, first few: {lost[:5]}"
    )
    assert not wrong, (
        f"FAIL: {len(wrong)} of {len(orphaned)} order(s) on an account the map cannot "
        f"translate are on the wrong id, first few: {wrong[:3]}"
    )


def test_every_reference_in_the_window_resolves_the_way_the_crosswalk_does(expected, landed):
    """The whole window, order by order, against the crosswalk applied by
    format with the untranslatable ids left as they stand."""
    wrong = [
        f"{order}: {landed[order][1]!r} for reference {ref!r}, wanted {account!r}"
        for order, (ref, account) in expected.items()
        if order in landed and landed[order][1] != account
    ]
    assert not wrong, (
        f"FAIL: {len(wrong)} of {len(expected)} order(s) resolve to the wrong "
        f"account, first few: {wrong[:5]}"
    )
