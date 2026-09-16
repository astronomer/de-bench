"""The old scheduler's run calendar, answered from the warehouse.

`legacy/autosys/copperline.jil` is the estate's own definition and
`legacy/autosys/calendars/retail_2026.cal` is the calendar the boxes that name
it obey. Wave 2 moves those boxes here, so this module answers the two
questions a converted DAG has to ask: which jobs the calendar governs, and
whether tonight is a night they may start.

**A calendar on a box reaches everything the box holds.** AutoSys starts a job
inside a box only when its box starts. Eight jobs carry
`run_calendar: retail_2026` and eighteen obey it — the tie steps under
`cpl.nightly.recon`, the pack and send steps under `cpl.nightly.dist` and the
close step under `cpl.month_end` inherit it and never say so. `box_name:` is the
edge and `calendared_jobs` walks it.

**The calendar is read from the warehouse, not from the export.** The export was
cut for 2026 alone. This deployment has run since February 2024 and wave 2 will
still be running after 2026, so a gate built on the file answers no to every
night outside one year — which stops the tie-outs rather than gating them.
`raw.market_calendar` at the home market carries the same fact, covers the years
either side, and reproduces the export's 251 days exactly over the year the two
share.

**`ds` is the day the run fires.** `CONVENTIONS.md` says so, and the export's
own header says `BUSDATE` is the day before it. So a converted box gated on
`is_run_day(ds)` owns `ds - 1`, which is what the old one did — and it inherits
the hole that comes with it: the runs that would carry a Friday and a Saturday
fall on a Saturday and a Sunday, and neither is a calendar day.

**A date the calendar does not cover raises.** Absence of a row is not a no. A
gate that answers "not a trading day" for a night it has never heard of stops
the estate and does it quietly.

Owned by supply-chain.
"""

from __future__ import annotations

import datetime as dt
import re

from include.lib import warehouse, workspace_root

__all__ = ["CalendarGap", "calendared_jobs", "is_run_day"]

JIL = workspace_root() / "legacy" / "autosys" / "copperline.jil"

#: `retail_2026.cal` says "Trading days, US market" in its own header, and the
#: warehouse calendar is per market, so the gate has to pick one.
HOME_MARKET = "US"

MARKET_CALENDAR = "raw.market_calendar"

_INSERT = re.compile(r"^insert_job:\s*(\S+)")
_ATTRIBUTE = re.compile(r"^(box_name|run_calendar):\s*(\S+)")

#: Read once and held. The export and the calendar are both settled records and
#: a backfill asks about hundreds of dates. Nothing is read at import:
#: `CONVENTIONS.md` rule 10 says module level does no work.
_JOBS: dict[str, dict[str, str | None]] | None = None
_CALENDAR: dict[str, bool] | None = None


class CalendarGap(LookupError):
    """A date the market calendar holds no row for."""


def _jobs() -> dict[str, dict[str, str | None]]:
    """Every job the export still defines, with its box and its own calendar.

    A `delete_job:` line is a job wave 1 took out. It defines nothing and opens
    no attribute block, so it closes whichever block it follows.
    """
    global _JOBS
    if _JOBS is None:
        jobs: dict[str, dict[str, str | None]] = {}
        current: str | None = None
        for raw in JIL.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            opened = _INSERT.match(line)
            if opened:
                current = opened.group(1)
                jobs[current] = {"box": None, "calendar": None}
                continue
            if line.startswith("delete_job:"):
                current = None
                continue
            attribute = _ATTRIBUTE.match(line)
            if attribute and current is not None:
                key = "box" if attribute.group(1) == "box_name" else "calendar"
                jobs[current][key] = attribute.group(2)
        _JOBS = jobs
    return _JOBS


def _governing(name: str) -> str | None:
    """The calendar a job obeys: its own, else the nearest box above it."""
    jobs = _jobs()
    seen: set[str] = set()
    current: str | None = name
    while current and current in jobs and current not in seen:
        seen.add(current)
        if jobs[current]["calendar"]:
            return jobs[current]["calendar"]
        current = jobs[current]["box"]
    return None


def calendared_jobs(calendar: str) -> set[str]:
    """Every job in the export the named run calendar governs.

    `calendared_jobs("retail_2026")` is the eighteen wave 2 has to gate.
    `calendared_jobs("retail_2025")` is `cpl.util.archive.logs` alone — a
    calendar this estate does not hold, on a job carrying `status: OI`.
    """
    return {name for name in _jobs() if _governing(name) == calendar}


def _home_calendar() -> dict[str, bool]:
    """Every date the home market's calendar covers, and whether it trades."""
    global _CALENDAR
    if _CALENDAR is None:
        with warehouse.connect(read_only=True) as con:
            rows = con.execute(
                f"SELECT calendar_date, is_trading_day "
                f"FROM {warehouse.qualify(MARKET_CALENDAR)} WHERE market_code = ?",
                [HOME_MARKET],
            ).fetchall()
        _CALENDAR = {day.isoformat(): bool(trading) for day, trading in rows}
    return _CALENDAR


def is_run_day(ds: str | dt.date) -> bool:
    """Whether the calendared boxes may start on `ds`.

    `ds` is the day the run fires. A box that starts on a Monday carries
    Sunday's `BUSDATE`, so a caller that wants to know about the day it owns
    asks about the day after it.
    """
    day = ds.isoformat() if isinstance(ds, dt.date) else str(ds)
    calendar = _home_calendar()
    try:
        return calendar[day]
    except KeyError:
        raise CalendarGap(
            f"{day} is outside the {HOME_MARKET} calendar "
            f"({min(calendar)} to {max(calendar)}), so there is nothing to gate "
            "on. Load the year before running anything for that date."
        ) from None
