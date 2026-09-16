"""Replaying a customer mart into the era before the acquired book landed.

The acquired book's history predates our ownership of it. An account opened
in 2011 has orders from before the deal, and a mart rebuilt only from the
acquisition forward has a hole where they should be. Filling that hole means
reading Northwave's own reporting tables, which is the frozen `nwv` copy.

**The frozen copy is not a live source.** It stopped refreshing when the
namespace was frozen and holds those tables as they stood then. A figure
taken from it is a figure from that date whatever date you ask for. Every
read here is qualified onto it deliberately and every row it produces carries
`source_era = 'pre_acquisition'`, so a number built from it can be told from
one built from the live feed.

**The window is refused rather than trimmed.** A range that crosses the day
the book landed would take half its rows from the frozen copy and half from
the live feed, and nobody could explain the result afterwards.
"""

from __future__ import annotations

import datetime as dt

from include.lib import warehouse

__all__ = ["REPLAYABLE", "BOOK_LANDED_ON", "SOURCE_ERA", "era_window",
           "frozen_counts", "replay", "verify"]

#: The marts this DAG knows how to replay, and the frozen table each reads.
REPLAYABLE = {
    "marts.customer_360": "nwv.reporting.account_summary",
    "marts.churn_scores_weekly": "nwv.reporting.account_activity",
}

#: The day the acquired book landed. Everything before it is the old era.
BOOK_LANDED_ON = dt.date(2025, 3, 17)

#: What a replayed row says about where it came from.
SOURCE_ERA = "pre_acquisition"


def era_window(first: dt.date, last: dt.date) -> list[str]:
    """The days to replay, or a refusal.

    Raises when the range is backwards, or when it reaches the day the book
    landed or past it. Both are refusals rather than trims: a trimmed range
    is a replay somebody thinks covered a month and did not.
    """
    if last < first:
        raise ValueError(f"to_ds {last} is before from_ds {first}")
    if last >= BOOK_LANDED_ON:
        raise ValueError(
            f"{last} is on or after {BOOK_LANDED_ON}, when the acquired book "
            "landed. Days from then on come from the live feed, and "
            "plat_backfill_broker is the way to replay those."
        )
    return [(first + dt.timedelta(days=step)).isoformat()
            for step in range((last - first).days + 1)]


def frozen_counts(days: list[str]) -> dict[str, int]:
    """What the frozen copy holds for the window, before anything moves.

    The baseline a replay is checked against. The frozen copy does not
    change, so the same window gives the same number every time.
    """
    if not days:
        return {"days": 0, "rows": 0}
    with warehouse.connect(read_only=True) as con:
        row = con.execute(
            "SELECT count(*) FROM nwv.reporting.account_summary "
            f"WHERE as_of_date IN ({_dates(days)})"
        ).fetchone()
    return {"days": len(days), "rows": int(row[0] or 0)}


def replay(mart: str, ds: str | dt.date) -> int:
    """Rebuild one day of a mart from the frozen copy. Returns the rows.

    The write is a delete-insert on that day's partition, so a replay run
    twice leaves one copy and a day outside the window is never touched.
    """
    if mart not in REPLAYABLE:
        raise ValueError(f"{mart} is not one of {', '.join(sorted(REPLAYABLE))}")
    day = _as_date(ds)
    if day >= BOOK_LANDED_ON:
        raise ValueError(f"{day} is not in the pre-acquisition era")
    source = REPLAYABLE[mart]
    with warehouse.connect() as con:
        rows = con.execute(
            f"""
            SELECT a.account_id                       AS customer_id,
                   a.account_name,
                   'northwave'                        AS source_book,
                   '{SOURCE_ERA}'                     AS source_era,
                   a.orders_12m,
                   (a.net_sales * 100)::BIGINT        AS net_sales_cents,
                   DATE '{day}'                       AS ds
            FROM {source} a
            WHERE a.as_of_date = DATE '{day}'
            ORDER BY a.account_id
            """
        ).fetchall()
        return warehouse.delete_insert(
            mart, "ds", day, rows,
            columns=["customer_id", "account_name", "source_book",
                     "source_era", "orders_12m", "net_sales_cents", "ds"],
            con=con,
        )


def verify(mart: str, days: list[str]) -> dict[str, int]:
    """What the replay wrote, and whether it stayed inside its window.

    Raises when a partition outside the window carries the replay's era: a
    backfill that reached past its range would silently restate a month
    finance has closed.
    """
    if not days:
        return {"days": 0, "rows": 0}
    table = warehouse.qualify(mart)
    with warehouse.connect(read_only=True) as con:
        inside = con.execute(
            f"SELECT count(*) FROM {table} WHERE ds IN ({_dates(days)}) "
            f"AND source_era = '{SOURCE_ERA}'"
        ).fetchone()
        outside = con.execute(
            f"SELECT count(*) FROM {table} WHERE ds NOT IN ({_dates(days)}) "
            f"AND source_era = '{SOURCE_ERA}' AND ds >= DATE '{BOOK_LANDED_ON}'"
        ).fetchone()
    if outside[0]:
        raise ValueError(
            f"{outside[0]} replayed row(s) landed outside the window and "
            "after the book landed"
        )
    return {"days": len(days), "rows": int(inside[0] or 0)}


def _dates(days: list[str]) -> str:
    return ", ".join(f"DATE '{_as_date(day)}'" for day in days)


def _as_date(value: str | dt.date) -> dt.date:
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value
    return dt.date.fromisoformat(str(value)[:10])
