"""The two calendars, and the one sanctioned clock.

Copperline reports on a 4-5-4 retail calendar, not on calendar months, and
`docs/retail-calendar.md` is the authority. This module does not implement
that calendar. It reads `raw.fiscal_calendar`, which holds one authored row
per date, and `raw.market_calendar`, which holds one row per market per date.
Nothing here derives a fiscal attribute with arithmetic, because the answers
that matter — the comp date after the 53-week year, the periods that are five
weeks long — are not the answers arithmetic gives.

**`today()` is the only clock in the tree.** It reads `WORLD_TODAY`. Nothing
in `include/lib/`, in a DAG or in a model calls `datetime.now()`, `date.today()`
or a database `current_date`: a run is told which dates it owns, by its
interval or by this function, so a rerun in August still reports the June it
was asked for.

The tables stop at 2027-01-30. A date outside them raises here rather than
returning nothing, because `docs/retail-calendar.md` says what silence would
cost: every year-end date lands NULL and nothing fails loudly.

Both tables are reference data and neither changes inside a run, so each one
is read once per process and held.
"""

from __future__ import annotations

import datetime as dt
import os
from typing import Any

from . import warehouse

__all__ = [
    "TODAY_ENV",
    "today",
    "fiscal",
    "fiscal_year",
    "fiscal_period",
    "fiscal_week",
    "week_start",
    "comp_date_ly",
    "is_market_holiday",
    "feed_expected",
    "is_trading_day",
    "business_days_between",
]

TODAY_ENV = "WORLD_TODAY"

_FISCAL: dict[dt.date, dict[str, Any]] | None = None
_MARKET: dict[tuple[str, dt.date], dict[str, Any]] | None = None

_FISCAL_COLUMNS = (
    "cal_date", "fiscal_year", "fiscal_quarter", "fiscal_period", "fiscal_week",
    "week_start", "week_end", "day_of_fiscal_week", "comp_date_ly",
    "comp_week_ly", "is_53rd_week",
)
_MARKET_COLUMNS = (
    "market_code", "calendar_date", "is_trading_day", "holiday_name",
    "feed_expected",
)


def today() -> dt.date:
    """The current business date, from `WORLD_TODAY`.

    This is the platform's clock and the only one. Use it where a job
    genuinely needs to know what day it is now — a retention cutoff, a
    freshness check, a report header. Do NOT use it to choose which rows a run
    reads or writes: that date comes from the run's own interval, and a task
    that asks the clock instead reads August's data when August reruns June.
    """
    value = os.environ.get(TODAY_ENV)
    if not value:
        raise RuntimeError(
            f"{TODAY_ENV} is not set, so there is no business date. The "
            "platform exports it; a local shell has to set it by hand."
        )
    return dt.date.fromisoformat(value.strip()[:10])


def fiscal(d: str | dt.date) -> dict[str, Any]:
    """The whole `raw.fiscal_calendar` row for a date, by column name.

    Keys: `cal_date`, `fiscal_year`, `fiscal_quarter`, `fiscal_period`,
    `fiscal_week`, `week_start`, `week_end`, `day_of_fiscal_week`,
    `comp_date_ly`, `comp_week_ly`, `is_53rd_week`. The named helpers below
    are shorthands for one key each; take the row when you want several.
    """
    day = _as_date(d)
    rows = _fiscal_rows()
    if day not in rows:
        raise KeyError(
            f"{day} is outside raw.fiscal_calendar, which runs "
            f"{min(rows)} to {max(rows)}"
        )
    return rows[day]


def fiscal_year(d: str | dt.date) -> str:
    """The fiscal year a date falls in, written as the table writes it:
    `FY2026`. FY2026 begins Sunday 2026-02-01 and the year ends on the
    Saturday nearest 31 January."""
    return fiscal(d)["fiscal_year"]


def fiscal_period(d: str | dt.date) -> int:
    """The fiscal period, 1 to 12, written `P1` to `P12` in reports.

    A period is a fiscal month of four or five weeks in the 4-5-4 pattern. It
    is not a calendar month and does not line up with one, whatever a
    particular year makes it look like.
    """
    return int(fiscal(d)["fiscal_period"])


def fiscal_week(d: str | dt.date) -> int:
    """The fiscal week number within the fiscal year. A week runs Sunday
    through Saturday. FY2023 has 53 of them; every other year has 52."""
    return int(fiscal(d)["fiscal_week"])


def week_start(d: str | dt.date) -> dt.date:
    """The Sunday that opens the fiscal week a date falls in."""
    return _as_date(fiscal(d)["week_start"])


def comp_date_ly(d: str | dt.date) -> dt.date | None:
    """The prior-year comparable date, as the calendar authored it.

    This is the answer to "what does this date compare to", and it is not a
    date offset and not the same-numbered week of the year before. FY2023 held
    53 weeks, so every FY2024 week W compares to FY2023 week W+1, and a
    same-week-number join is wrong by one week for the whole of FY2024.

    Returns None where no honest comparable exists — the 53rd week itself, and
    every date whose prior year is outside the table.
    """
    value = fiscal(d)["comp_date_ly"]
    return _as_date(value) if value is not None else None


def is_trading_day(d: str | dt.date, market: str) -> bool:
    """Whether the market trades that day.

    False on Saturdays, Sundays and every holiday the market names.
    `raw.finance_close_calendar` counts its fifth business day against this
    flag, and so does `business_days_between`.
    """
    return bool(_market(d, market)["is_trading_day"])


def is_market_holiday(d: str | dt.date, market: str) -> bool:
    """Whether the market names a public holiday that day.

    A holiday is not the same as a closure and not the same as a missing feed.
    A US home retailer sells hard on Memorial Day and a UK bank holiday is a
    sale day: both are holidays, both are non-trading days, and both still
    deliver a file. Ask `feed_expected` when the question is about a file.
    """
    return _market(d, market)["holiday_name"] is not None


def feed_expected(d: str | dt.date, market: str) -> bool:
    """Whether the market is due to send anything that day.

    False only where the market shuts hard enough to send nothing. Three dates
    in the range are false in every market at once — 2024-12-25, 2025-12-25
    and 2026-05-25 — and on those days a feed that does not arrive has
    arrived correctly. Six of the nine markets name no holiday on 2026-05-25,
    so it reads as an ordinary Monday to anything that does not open this
    table.
    """
    return bool(_market(d, market)["feed_expected"])


def business_days_between(a: str | dt.date, b: str | dt.date, market: str = "US") -> int:
    """How many trading days the market has from `a` to `b`.

    HALF OPEN, and it matters when you are counting to a deadline: `a`
    counts if it trades, `b` never counts. So the fifth business day of a
    month is the day `d` where `business_days_between(first_of_month, d) == 4`,
    and `business_days_between(d, d)` is 0 for every date.

    Counts backwards when `b` is before `a`, and returns a negative number.
    """
    start, end = _as_date(a), _as_date(b)
    if end < start:
        return -business_days_between(end, start, market)
    rows = _market_rows()
    days = (end - start).days
    return sum(
        1
        for n in range(days)
        if _row(rows, market, start + dt.timedelta(days=n))["is_trading_day"]
    )


# --- reading the two tables ------------------------------------------------

def _fiscal_rows() -> dict[dt.date, dict[str, Any]]:
    global _FISCAL
    if _FISCAL is None:
        _FISCAL = {
            _as_date(row[0]): dict(zip(_FISCAL_COLUMNS, row))
            for row in _read("raw.fiscal_calendar", _FISCAL_COLUMNS)
        }
    return _FISCAL


def _market_rows() -> dict[tuple[str, dt.date], dict[str, Any]]:
    global _MARKET
    if _MARKET is None:
        _MARKET = {
            (row[0], _as_date(row[1])): dict(zip(_MARKET_COLUMNS, row))
            for row in _read("raw.market_calendar", _MARKET_COLUMNS)
        }
    return _MARKET


def _market(d: str | dt.date, market: str) -> dict[str, Any]:
    return _row(_market_rows(), market, _as_date(d))


def _row(rows: dict, market: str, day: dt.date) -> dict[str, Any]:
    try:
        return rows[(market, day)]
    except KeyError:
        raise KeyError(
            f"raw.market_calendar has no row for {market} on {day}"
        ) from None


def _read(table: str, columns: tuple[str, ...]) -> list[tuple]:
    """Read a calendar table. Read-only, so it never queues behind a run, and
    qualified, so it never reads the frozen `nwv` copy."""
    with warehouse.connect(read_only=True) as con:
        return con.execute(
            f"SELECT {', '.join(columns)} FROM {warehouse.qualify(table)}"
        ).fetchall()


def _as_date(value: Any) -> dt.date:
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value
    return dt.date.fromisoformat(str(value)[:10])
