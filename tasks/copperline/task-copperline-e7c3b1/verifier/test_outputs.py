"""STO-419 — does the pack put each night under the day the store traded it?

Never ships to the agent. It runs beside the scored tree, after
`store_trading_day_backfill` has been run once, and reads what that run landed
in `ops.store_trading_day`.

**No authored numbers.** Every expected result is computed from
`raw.pos_sales_header` and `raw.stores` in the same session. The answer is
graded against the world's own landed tables, never against a figure typed
here.

**The three readings the window separates.** The UTC standardization falls
inside it, and three facts about the landed data decide whether a pack is
right:

    no UTC stamp at all     every transaction before 2025-11-03 carries a NULL
                            `event_time_utc`. A pack keyed on it holds nothing
                            for the first of the two periods.
    the 23:05 close batch   before the standardization the registers stamped
                            the whole day with the close time in the store's
                            own clock. 23:05 in an Americas zone is the next
                            day in UTC, so rebuilding a UTC day out of the
                            local stamp moves the whole night of two thirds of
                            the estate onto the night after it.
    the evening sale        after the standardization the UTC date and the
                            trading day disagree for an evening sale, in the
                            same direction and on the same estate.

`test_the_window_separates_the_readings` is the oracle's own guard: it fails
loudly if a change to the world empties any of those populations, rather than
letting the rest of this file grade a distinction the data no longer makes.

The table is read whole rather than through the window, so a backfill that did
more than it was asked is not convicted for it — only the days the ticket named
are compared.
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
# search path and carries `raw.stores` under the same name, so an unqualified
# read would answer with September 2025's Northwave numbers.
HEADER = warehouse.qualify("raw.pos_sales_header")
STORES = warehouse.qualify("raw.stores")
MANIFEST = warehouse.qualify("raw.pos_batch_manifest")
PACK = warehouse.qualify("ops.store_trading_day")

#: The window the ticket names: FY2025 periods 9 and 10.
FROM_DS = "2025-10-05"
TO_DS = "2025-11-29"

WINDOW = f"h.business_date BETWEEN DATE '{FROM_DS}' AND DATE '{TO_DS}'"

#: What the pack is graded against: the till batches rolled up to the store and
#: the day the store traded them, with the store's market taken from the one
#: current row of an SCD2 dimension.
TRUTH = f"""
SELECT h.store_id,
       s.market_code,
       h.business_date                                    AS ds,
       count(*)                                           AS txn_count,
       count(*) FILTER (WHERE h.void_flag)                AS void_count,
       count(*) FILTER (WHERE h.return_flag)              AS return_count,
       coalesce(sum(h.net_cents) FILTER (WHERE NOT h.void_flag), 0) AS net_cents
FROM {HEADER} h
JOIN {STORES} s ON s.store_id = h.store_id AND s.is_current
WHERE {WINDOW}
GROUP BY 1, 2, 3
"""

#: The day the registers began sending UTC. Read off the data, not typed: it is
#: the first business date on which any transaction carries a UTC stamp.
CUTOVER = f"SELECT min(h.business_date) FROM {HEADER} h WHERE h.event_time_utc IS NOT NULL"

#: Store-days inside the window whose transactions all predate the
#: standardization and whose 23:05 close batch reads as the next day in UTC.
SHIFTED = f"""
SELECT DISTINCT h.store_id, h.business_date
FROM {HEADER} h
JOIN {STORES} s ON s.store_id = h.store_id AND s.is_current
WHERE {WINDOW}
  AND h.event_time_utc IS NULL
  AND ((h.event_time_local AT TIME ZONE s.tz_name) AT TIME ZONE 'UTC')::date
      <> h.business_date
"""

#: Transactions after the standardization whose UTC date is not the trading day.
EVENING = f"""
SELECT count(*) FROM {HEADER} h
WHERE {WINDOW} AND h.event_time_utc IS NOT NULL
  AND h.event_time_utc::date <> h.business_date
"""

#: Stores that carry more than one row in the dimension and traded in the
#: window. A join that takes both rows counts every one of their transactions
#: twice, and the market is the same on both rows, so nothing else moves.
SCD2_STORES = f"""
SELECT DISTINCT h.store_id FROM {HEADER} h
WHERE {WINDOW}
  AND h.store_id IN (SELECT store_id FROM {STORES} GROUP BY 1 HAVING count(*) > 1)
"""

#: Store-days the manifest holds a row for and no transaction landed under.
#: POS-1: a store that sent nothing is late, not a store that took nothing.
UNBACKED = f"""
SELECT m.store_id, m.business_date
FROM {MANIFEST} m
WHERE m.business_date BETWEEN DATE '{FROM_DS}' AND DATE '{TO_DS}'
  AND NOT EXISTS (SELECT 1 FROM {HEADER} h
                  WHERE h.store_id = m.store_id
                    AND h.business_date = m.business_date)
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


def _row(record: tuple) -> tuple:
    """(market, txns, voids, returns, cents) from a pack or truth row.

    `net_cents` is read through `or 0`: a store-day whose every transaction was
    voided took nothing, and a job that writes that as NULL rather than as a
    zero has said the same thing.
    """
    return (record[1], int(record[3]), int(record[4]), int(record[5]),
            int(record[6] or 0))


@pytest.fixture(scope="session")
def expected() -> dict[tuple, tuple]:
    """(store, trading day) -> (market, txns, voids, returns, cents)."""
    return {(r[0], r[2]): _row(r) for r in query(TRUTH)}


@pytest.fixture(scope="session")
def landed() -> dict[tuple, tuple]:
    """What the backfill wrote for the window. (store, ds) -> the same tuple.

    Rows outside the window are read and dropped on purpose: a run that filled
    more history than the ticket asked for is not wrong, and only the days the
    ticket named are compared.
    """
    tables = query(
        "SELECT count(*) FROM duckdb_tables() "
        "WHERE schema_name = 'ops' AND table_name = 'store_trading_day'"
    )
    assert tables[0][0], (
        "FAIL: ops.store_trading_day is not in the warehouse; the backfill "
        "landed nothing"
    )
    columns = {row[0] for row in query(
        "SELECT column_name FROM duckdb_columns() "
        "WHERE schema_name = 'ops' AND table_name = 'store_trading_day'"
    )}
    wanted = {"store_id", "market_code", "ds", "txn_count", "void_count",
              "return_count", "net_cents"}
    assert wanted <= columns, (
        f"FAIL: ops.store_trading_day is missing {sorted(wanted - columns)}; "
        "the ticket names every column the pack holds"
    )
    rows = query(
        "SELECT store_id, market_code, cast(ds AS DATE), txn_count, void_count, "
        f"return_count, net_cents FROM {PACK} "
        f"WHERE cast(ds AS DATE) BETWEEN DATE '{FROM_DS}' AND DATE '{TO_DS}'"
    )
    assert rows, "FAIL: ops.store_trading_day holds no row inside the window"
    return {(r[0], r[2]): _row(r) for r in rows}


def rows_in_window() -> int:
    return query(
        f"SELECT count(*) FROM {PACK} "
        f"WHERE cast(ds AS DATE) BETWEEN DATE '{FROM_DS}' AND DATE '{TO_DS}'"
    )[0][0]


def test_the_window_separates_the_readings():
    """The oracle checks the world before it grades anything.

    The window has to straddle the standardization, hold close batches that
    read as the next UTC day, hold evening sales whose UTC date is not the
    trading day, hold a store with two dimension rows, and hold a manifest row
    with no transactions behind it. Without all five this file grades
    distinctions the data no longer makes.
    """
    cutover = query(CUTOVER)[0][0]
    assert cutover is not None, "FAIL: no transaction anywhere carries a UTC stamp"
    blind = query(f"SELECT count(*) FROM {HEADER} h WHERE {WINDOW} "
                  "AND h.event_time_utc IS NULL")[0][0]
    assert blind, (
        "FAIL: every transaction in the window carries a UTC stamp, so a pack "
        "keyed on it would grade the same as one keyed on the trading day"
    )
    shifted = query(f"SELECT count(*) FROM ({SHIFTED})")[0][0]
    assert shifted, (
        "FAIL: no close batch in the window reads as the next day in UTC, so "
        "converting the local stamp would grade the same as reading the "
        "trading day"
    )
    evening = query(EVENING)[0][0]
    assert evening, (
        "FAIL: after the standardization no transaction's UTC date differs "
        "from its trading day, so the second period separates nothing"
    )
    doubled = query(f"SELECT count(*) FROM ({SCD2_STORES})")[0][0]
    assert doubled, (
        "FAIL: no store trading in the window carries a second dimension row, "
        "so a join that took every version would grade the same as one that "
        "took the current one"
    )
    unbacked = query(f"SELECT count(*) FROM ({UNBACKED})")[0][0]
    assert unbacked, (
        "FAIL: every manifest row in the window has transactions behind it, so "
        "a pack built off the manifest would grade the same as one built off "
        "the transactions"
    )


def test_every_store_day_that_traded_has_a_row(expected, landed):
    """The population: every store-day the till data covers.

    A pack keyed on the UTC stamp loses the whole of the first period, and one
    that drops the store-days whose manifest count disagrees with what loaded
    loses the partial files — which POS-2 says are reloaded, not repaired.
    """
    missing = sorted(set(expected) - set(landed))
    assert not missing, (
        f"FAIL: {len(missing)} store-day(s) traded and have no row, first few: "
        f"{missing[:5]}"
    )


def test_no_store_day_lands_without_transactions_behind_it(expected, landed):
    """POS-1: a store that sent nothing is late, not a store that took nothing.

    The rows come from the transactions. A pack built off a store-and-date
    spine, or off the batch manifest, invents nights that no register reported.
    """
    invented = sorted(set(landed) - set(expected))
    assert not invented, (
        f"FAIL: {len(invented)} row(s) name a store-day with no transactions "
        f"behind it, first few: {invented[:5]}"
    )


def test_one_row_per_store_and_trading_day(landed):
    """The grain. Two rows for one store-night is a join that fanned out, and
    the district manager reads the line."""
    assert rows_in_window() == len(landed), (
        f"FAIL: {rows_in_window()} rows over {len(landed)} store-days"
    )


def test_the_nights_before_the_standardization_land_on_the_day_they_traded(expected, landed):
    """Before the standardization the registers held the day and posted one
    close batch, stamped 23:05 in the store's own clock. That stamp is the
    close, not the sale, and 23:05 in an Americas zone is the next day in UTC —
    so a pack that rebuilds a UTC day out of it moves the whole night of two
    thirds of the estate onto the night after the one it traded."""
    shifted = [(row[0], row[1]) for row in query(SHIFTED)]
    lost = [key for key in shifted if key not in landed]
    wrong = [
        f"{key[0]} {key[1]}: {landed[key][1]} transactions, the till took "
        f"{expected[key][1]}"
        for key in shifted
        if key in landed and key in expected and landed[key][1] != expected[key][1]
    ]
    assert not lost, (
        f"FAIL: {len(lost)} of {len(shifted)} store-night(s) whose close batch "
        f"reads as the next UTC day have no row, first few: {lost[:5]}"
    )
    assert not wrong, (
        f"FAIL: {len(wrong)} of {len(shifted)} store-night(s) whose close batch "
        f"reads as the next UTC day hold the wrong night's transactions, first "
        f"few: {wrong[:3]}"
    )


def test_a_store_with_two_dimension_rows_is_not_counted_twice(expected, landed):
    """`raw.stores` is SCD2 and holds more rows than stores, because a store
    that moves region keeps its old row. The market is the same on both rows,
    so a join that takes every version changes no market and doubles every
    count and every cent those stores took."""
    stores = {row[0] for row in query(SCD2_STORES)}
    wrong = [
        f"{key[0]} {key[1]}: {landed[key][1]} transactions and "
        f"{landed[key][4]} cents, the till took {expected[key][1]} and "
        f"{expected[key][4]}"
        for key in expected
        if key[0] in stores and key in landed and landed[key] != expected[key]
    ]
    assert not wrong, (
        f"FAIL: {len(wrong)} store-day(s) of a store carrying two dimension "
        f"rows are wrong, first few: {wrong[:3]}"
    )


def test_a_voided_transaction_is_counted_and_takes_no_money(expected, landed):
    """A void is a transaction the till recorded and then reversed. The pack
    counts it and leaves its money out, which is what the staging model does
    with the same rows. Dropping voids outright loses the count, and it loses
    altogether the store-days on which every transaction was voided."""
    voided = [key for key, row in expected.items() if row[2]]
    wrong = [
        f"{key[0]} {key[1]}: {landed[key][1]} transactions of which "
        f"{landed[key][2]} voided for {landed[key][4]} cents, the till took "
        f"{expected[key][1]} / {expected[key][2]} / {expected[key][4]}"
        for key in voided
        if key in landed and landed[key] != expected[key]
    ]
    assert not wrong, (
        f"FAIL: {len(wrong)} of {len(voided)} store-day(s) holding a voided "
        f"transaction are wrong, first few: {wrong[:3]}"
    )


def test_every_store_day_matches_the_till(expected, landed):
    """The whole window, store-night by store-night: the market, the three
    counts and the money, against the till batches rolled up to the day the
    store traded them."""
    wrong = [
        f"{key[0]} {key[1]}: {landed[key]} against {expected[key]}"
        for key, row in expected.items()
        if key in landed and landed[key] != row
    ]
    assert not wrong, (
        f"FAIL: {len(wrong)} of {len(expected)} store-day(s) disagree with the "
        f"till, as (market, txns, voids, returns, cents), first few: {wrong[:5]}"
    )
