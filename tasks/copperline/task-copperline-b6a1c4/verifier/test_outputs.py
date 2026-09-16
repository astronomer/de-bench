"""CUS-402 — did the nightly land the keyed addresses, and is the dimension a
dimension of places?

Never ships to the agent. It runs beside the scored tree, with the tree root on
`PYTHONPATH`, so it opens the warehouse the same way every DAG in the world
does.

**It reads, it does not build.** The DAG has already been run for the graded day
by the time this file runs, and it has been run TWICE at that day, because the
ticket says a day is replaced rather than added to. Everything below reads what
those runs left behind.

**No authored numbers.** Every expected result comes out of `raw.customers`,
`raw.nwv_accounts` and `ops.address_keyed` in the same session. Nothing here is
compared against a figure typed into this file.

**The two grains.** A place is an address key and an account is a row. The
acquired book carries a city and a state and nothing finer, so many of its
accounts key to one place, and a dimension built at account grain has more rows
than a dimension built at place grain. `test_the_day_can_tell_a_place_from_an_
account` is the guard that says the graded day can tell the two apart at all.
"""

from __future__ import annotations

import duckdb
import pytest

from include.lib import warehouse

DB = str(warehouse.warehouse_path())

#: The day the DAG was run for. It sits outside the world's reserved windows
#: and outside the acquisition and cutover seams.
DS = "2026-05-12"

# Every name is qualified onto the live warehouse, for the reason
# `include/lib/warehouse.py` gives: the frozen `nwv` copy sits first on the
# search path and carries some of these names too.
CUSTOMERS = warehouse.qualify("raw.customers")
NWV = warehouse.qualify("raw.nwv_accounts")
KEYED = warehouse.qualify("ops.address_keyed")
DIM = warehouse.qualify("marts.dim_location")

#: The key's columns, in the order `projects/customer/lib/addresses.py` and
#: `projects/customer/CONVENTIONS.md` both fix. Written out here rather than
#: imported, because the order is the contract and a patch that reorders it has
#: to be measured against the contract and not against itself.
KEY_COLUMNS = ("country_code", "region_code", "postal_code", "locality",
               "street_line", "unit")

#: The columns the ticket asks the dimension to carry.
DIM_COLUMNS = ("ds", "address_key", *KEY_COLUMNS, "parts_present", "accounts",
               "accounts_copperline", "accounts_northwave")


def query(sql: str, params: list | None = None) -> list[tuple]:
    """One read, on its own connection."""
    con = duckdb.connect(DB, read_only=True)
    try:
        return con.execute(sql, params or []).fetchall()
    finally:
        con.close()


def columns_of(table: str) -> list[str]:
    schema, _, bare = table.rpartition(".")
    schema = schema.split(".")[-1]
    return [r[0] for r in query(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position",
        [schema, bare],
    )]


@pytest.fixture(scope="session", autouse=True)
def both_tables_exist():
    """Nothing below means anything if the nightly wrote neither table."""
    for table in (KEYED, DIM):
        assert columns_of(table), f"FAIL: {table}: the nightly wrote no such table"


def test_the_day_can_tell_a_place_from_an_account():
    """The fixture's own guard: on the graded day, per-place and per-account are
    different answers. Without that, a dimension at the wrong grain would pass
    every count below."""
    rows, places = query(
        f"SELECT count(*), count(DISTINCT address_key) FROM {KEYED} WHERE ds = ?",
        [DS],
    )[0]
    assert places, f"FAIL: {DS}: no keyed addresses, so nothing can be graded"
    assert places < rows, (
        f"FAIL: {DS}: {rows} keyed addresses fall on {places} places, so the two "
        "grains give the same answer and this day cannot grade the dimension"
    )


def test_both_books_are_keyed_once_each():
    """Every account on both books, once. A run that finished by dropping the
    acquired leg, or by dropping the rows whose parts are absent, is not a run
    that finished."""
    expected = query(f"SELECT (SELECT count(*) FROM {CUSTOMERS}) "
                     f"+ (SELECT count(*) FROM {NWV})")[0][0]
    rows, parties = query(
        f"SELECT count(*), count(DISTINCT party_id || '/' || source_book) "
        f"FROM {KEYED} WHERE ds = ?", [DS],
    )[0]
    assert rows == expected, (
        f"FAIL: {DS}: {rows} keyed addresses against {expected} accounts on the "
        "two books"
    )
    assert parties == expected, (
        f"FAIL: {DS}: {parties} distinct accounts behind {rows} rows"
    )


def test_no_part_of_the_key_is_absent():
    """`addresses.py`: empty is not absent, and the key cannot tell — so a part
    the source did not carry is written as an empty string, never as a null. A
    null part makes the key itself null and the place unjoinable."""
    for table in (KEYED, DIM):
        blank = " OR ".join(f"{c} IS NULL" for c in KEY_COLUMNS)
        bad = query(f"SELECT count(*) FROM {table} WHERE ds = ? AND ({blank} "
                    "OR address_key IS NULL)", [DS])[0][0]
        assert bad == 0, f"FAIL: {DS}: {table}: {bad} row(s) carry a null part"


def test_the_key_is_the_six_parts_in_the_fixed_order():
    """The key is `KEY_COLUMNS` hashed in that order, joined by a separator that
    cannot occur in a part. Two models that list the same columns differently
    make two keys for one place, and nothing downstream can tell them apart."""
    spelled = " || '|' || ".join(KEY_COLUMNS)
    for table in (KEYED, DIM):
        bad = query(f"SELECT count(*) FROM {table} WHERE ds = ? "
                    f"AND md5({spelled}) <> address_key", [DS])[0][0]
        assert bad == 0, (
            f"FAIL: {DS}: {table}: {bad} row(s) carry a key that is not the six "
            "parts hashed in the dimension's own order"
        )


def test_the_dimension_carries_the_columns_the_review_asked_for():
    found = columns_of(DIM)
    missing = [c for c in DIM_COLUMNS if c not in found]
    assert not missing, f"FAIL: marts.dim_location has no {', '.join(missing)}"


def test_the_dimension_is_one_row_per_place():
    """The grain. One row per address key for the day — not one per account, and
    not two per place because the day was added to rather than replaced. The DAG
    was run twice at this day."""
    places = query(f"SELECT count(DISTINCT address_key) FROM {KEYED} WHERE ds = ?",
                   [DS])[0][0]
    rows, keys = query(
        f"SELECT count(*), count(DISTINCT address_key) FROM {DIM} WHERE ds = ?",
        [DS],
    )[0]
    assert keys == places, (
        f"FAIL: {DS}: the dimension holds {keys} places and the keyed addresses "
        f"fall on {places}"
    )
    assert rows == places, (
        f"FAIL: {DS}: {rows} rows for {places} places — the grain is the place, "
        "and a re-run replaces the day"
    )
    orphans = query(
        f"SELECT count(*) FROM (SELECT address_key FROM {DIM} WHERE ds = ? "
        f"EXCEPT SELECT address_key FROM {KEYED} WHERE ds = ?)", [DS, DS],
    )[0][0]
    assert orphans == 0, (
        f"FAIL: {DS}: {orphans} place(s) in the dimension are keyed to nothing in "
        "ops.address_keyed"
    )


def test_the_dimension_carries_the_parts_the_run_wrote():
    """The six parts and `parts_present` come off the keyed rows. A dimension
    that re-derives them cannot be joined back to the accounts at the place."""
    parts = ", ".join(KEY_COLUMNS)
    bad = query(
        f"""
        SELECT count(*) FROM (
            SELECT DISTINCT address_key, {parts}, parts_present
            FROM {KEYED} WHERE ds = ?
            EXCEPT
            SELECT address_key, {parts}, parts_present FROM {DIM} WHERE ds = ?
        )
        """, [DS, DS],
    )[0][0]
    assert bad == 0, (
        f"FAIL: {DS}: {bad} place(s) carry parts the run did not write"
    )


def test_the_account_counts_tie_to_the_accounts_at_the_place():
    """`accounts` is how many accounts sit at the place, and the two book
    columns split it. A dimension that counts rows of its own, or that folds
    the acquired book into the total without naming it, fails here."""
    bad = query(
        f"""
        SELECT count(*) FROM (
            SELECT address_key,
                   count(*)                                     AS accounts,
                   count(*) FILTER (source_book = 'copperline') AS accounts_copperline,
                   count(*) FILTER (source_book = 'northwave')  AS accounts_northwave
            FROM {KEYED} WHERE ds = ? GROUP BY address_key
            EXCEPT
            SELECT address_key, accounts, accounts_copperline, accounts_northwave
            FROM {DIM} WHERE ds = ?
        )
        """, [DS, DS],
    )[0][0]
    assert bad == 0, (
        f"FAIL: {DS}: {bad} place(s) carry an account count the keyed addresses "
        "do not support"
    )
