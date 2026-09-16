"""The store trading-day pack: one row per store per day the store traded.

Store ops read this beside the store's own end-of-day report, so the day on the
row has to be the day the store counted, and nothing else.

**The day is `business_date` and it is the only day a store row has.** The
register software closes the trading day at 23:05 store-local and stamps the
whole batch with that one time, so `event_time_local` on a pre-cutover row is
the close, not the sale. `docs/runbooks/pos-ingestion.md` POS-2.

**Before 2025-11-03 there is no UTC stamp at all.** `event_time_utc` is NULL on
every transaction the registers sent before the standardization, so a pack
keyed on it holds nothing for the first of the two periods. Building one out of
the local stamp and the store's zone is worse than holding nothing: 23:05 in an
Americas zone is the next day in UTC, so the whole close batch of two thirds of
the estate lands on the night after the one it was taken on.

**From 2025-11-03 the registers send per-transaction times**, and the UTC date
and the trading day then genuinely disagree for an evening sale. Reading the
UTC date once the column is populated therefore moves rows across nights on the
second period as well, in the other direction from the estate that is right.

**A voided transaction still happened.** It is counted and it took no money.
`stg_store__pos_sales_header` keeps voids and flags them for exactly this
reason. A return is a transaction the till recorded too, and its amount sits on
the row the way the till wrote it.

**A store-day with no transactions is not a zero-sales day.** POS-1: a store
that has sent nothing is late until three business days have passed, and a
manifest row with no file behind it is not a day the store took nothing. Rows
come from the transactions, never from a calendar.

**`raw.stores` is SCD2.** 290 rows over 268 stores, because a store that moves
region gets a second row. The market never moves with it, but a join that takes
both rows counts every transaction of those stores twice.
"""

from __future__ import annotations

import datetime as dt

from include.lib import warehouse

__all__ = ["BACKFILL_FROM", "BACKFILL_TO", "COLUMNS", "ensure_table",
           "backfill", "coverage"]

#: The window STO-419 asks for: FY2025 periods 9 and 10, which the UTC
#: standardization falls inside. A one-off, so the dates live here rather than
#: in a Param — store ops trigger the job and type nothing.
BACKFILL_FROM = "2025-10-05"
BACKFILL_TO = "2025-11-29"

#: `ops.store_trading_day`, in order. One row per store per trading day,
#: partitioned on the trading day.
COLUMNS = ("store_id", "market_code", "ds", "txn_count", "void_count",
           "return_count", "net_cents")


def ensure_table(con) -> None:
    """Create the pack's table if the warehouse has not got one yet.

    `delete_insert` replaces a partition of a table that exists; it does not
    make one. This backfill is the first thing to write the table, so the DDL
    sits here rather than in a migration nobody runs.
    """
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {warehouse.qualify("ops.store_trading_day")} (
            store_id VARCHAR NOT NULL,
            market_code VARCHAR NOT NULL,
            ds DATE NOT NULL,
            txn_count BIGINT NOT NULL,
            void_count BIGINT NOT NULL,
            return_count BIGINT NOT NULL,
            net_cents BIGINT NOT NULL
        )
    """)


def backfill(from_ds: str | dt.date = BACKFILL_FROM,
             to_ds: str | dt.date = BACKFILL_TO) -> dict[str, int]:
    """Fill the pack for a window of trading days. Returns the counts.

    One delete-insert per trading day, so the job may be run twice and a day
    outside the window is never touched.
    """
    first, last = _as_date(from_ds), _as_date(to_ds)
    if last < first:
        raise ValueError(f"to_ds {last} is before from_ds {first}")
    header = warehouse.qualify("raw.pos_sales_header")
    stores = warehouse.qualify("raw.stores")
    with warehouse.connect() as con:
        ensure_table(con)
        rows = con.execute(f"""
            SELECT h.store_id,
                   s.market_code,
                   h.business_date                                   AS ds,
                   count(*)                                          AS txn_count,
                   count(*) FILTER (WHERE h.void_flag)               AS void_count,
                   count(*) FILTER (WHERE h.return_flag)             AS return_count,
                   coalesce(sum(h.net_cents)
                            FILTER (WHERE NOT h.void_flag), 0)       AS net_cents
            FROM {header} h
            JOIN {stores} s ON s.store_id = h.store_id AND s.is_current
            WHERE h.business_date BETWEEN DATE '{first}' AND DATE '{last}'
            GROUP BY 1, 2, 3
            ORDER BY 3, 1
        """).fetchall()
        by_day: dict[dt.date, list[tuple]] = {}
        for row in rows:
            by_day.setdefault(row[2], []).append(row)
        written = 0
        for day in _days(first, last):
            written += warehouse.delete_insert(
                "ops.store_trading_day", "ds", day, by_day.get(day, []),
                columns=list(COLUMNS), con=con,
            )
    return {"days": (last - first).days + 1, "store_days": written}


def coverage(from_ds: str | dt.date = BACKFILL_FROM,
             to_ds: str | dt.date = BACKFILL_TO) -> dict[str, int]:
    """Every store-day that traded landed, once, and nothing else did.

    Fails the run rather than reporting it: store ops read a store's night
    straight back to the district manager, so a hole in the pack is worse than
    a job that did not finish.
    """
    first, last = _as_date(from_ds), _as_date(to_ds)
    header = warehouse.qualify("raw.pos_sales_header")
    pack = warehouse.qualify("ops.store_trading_day")
    window = f"BETWEEN DATE '{first}' AND DATE '{last}'"
    with warehouse.connect(read_only=True) as con:
        traded, landed, grain, unbacked = con.execute(f"""
            SELECT (SELECT count(*) FROM (SELECT DISTINCT store_id, business_date
                        FROM {header} WHERE business_date {window})),
                   (SELECT count(DISTINCT (store_id, ds)) FROM {pack}
                        WHERE ds {window}),
                   (SELECT count(*) FROM {pack} WHERE ds {window}),
                   (SELECT count(*) FROM {pack} p WHERE p.ds {window}
                        AND NOT EXISTS (SELECT 1 FROM {header} h
                                        WHERE h.store_id = p.store_id
                                          AND h.business_date = p.ds))
        """).fetchone()
    if landed != traded:
        raise ValueError(f"{traded} store-days traded, {landed} landed")
    if grain != landed:
        raise ValueError(f"{grain} rows over {landed} store-days")
    if unbacked:
        raise ValueError(f"{unbacked} store-day(s) with no transactions behind them")
    return {"store_days": landed}


def _days(first: dt.date, last: dt.date) -> list[dt.date]:
    return [first + dt.timedelta(days=step)
            for step in range((last - first).days + 1)]


def _as_date(value: str | dt.date) -> dt.date:
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value
    return dt.date.fromisoformat(str(value)[:10])
