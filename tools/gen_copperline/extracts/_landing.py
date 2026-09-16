"""The dated landing tree, and the rules every extract writes it by.

Spec chapter 03 section 2 gives each source a landing path and a retention
class: 90 days for a vendor file, 400 days for one Copperline wrote. Writing
the whole retention window is not affordable for every source. The POS feed
is one file per store per night, so 400 days is 107,200 files — minutes of
setup on every trial, for a tree no task reads more than a few days of.

So the tree materializes a **horizon**: the last N days of the range, plus
every date in `KEY_DATES` whatever the horizon says. Each module picks its
own N and gives the reason beside it. The warehouse tables are complete over
the whole range either way; the horizon only decides which days keep a file.

The horizon is shorter than every retention class, so the retention policy is
a document fact and not a tree fact. Nothing in the register grades how deep
the tree goes; what a task reads is the last few days, the key dates, and the
warehouse tables behind them. The `LANDING_DAYS` constant in each module is
the knob if that ever stops being true.

`KEY_DATES` are the boundary days a task can land on: the era cutovers, the
DST days, the peak day and the all-market holiday (spec 01 sections 4.5 and
5). They always keep files, so a file-level question about any of them has an
answer at both profiles.
"""

from __future__ import annotations

import csv
import datetime as dt
from pathlib import Path

from ..config import Context

KEY_DATES: tuple[dt.date, ...] = (
    dt.date(2024, 11, 3),    # US fall back, armed
    dt.date(2024, 11, 29),   # Black Friday
    dt.date(2024, 12, 25),   # all-market skip
    dt.date(2025, 11, 2),    # US fall back, armed, and the last day of E1
    dt.date(2025, 11, 3),    # E6, the UTC cutover
    dt.date(2025, 11, 28),   # Black Friday, first peak of the new era
    dt.date(2026, 1, 5),     # E7, the orchestrator migration
    dt.date(2026, 2, 1),     # FY2026 and the E8 valuation change
    dt.date(2026, 3, 8),     # US spring forward, graded
    dt.date(2026, 3, 29),    # EU spring forward, graded
    dt.date(2026, 4, 6),     # the legacy control break
    dt.date(2026, 5, 25),    # the all-market holiday
)


def window(ctx: Context, days: int) -> list[dt.date]:
    """The dates a source's landing tree covers: the last `days` of the range,
    plus every key date inside it."""
    horizon = {ctx.end - dt.timedelta(days=n) for n in range(days)}
    horizon |= {d for d in KEY_DATES if ctx.start <= d <= ctx.end}
    return sorted(d for d in horizon if ctx.start <= d <= ctx.end)


def sql_dates(dates: list[dt.date]) -> str:
    """The dates as a SQL list, for `WHERE ds IN (...)`."""
    if not dates:
        return "(NULL)"
    return "(" + ", ".join(f"DATE '{d}'" for d in dates) + ")"


def copy_csv(ctx: Context, path: Path, select: str) -> None:
    """One landing file, written by DuckDB. The select must carry its own
    ORDER BY: a landing file is a byte-for-byte fixture, so row order is part
    of it."""
    path.parent.mkdir(parents=True, exist_ok=True)
    ctx.sql(f"COPY ({select}) TO '{path}' (FORMAT CSV, HEADER, DATEFORMAT '%Y-%m-%d')")


def write_csv(path: Path, header: list[str], rows) -> None:
    """One landing file, written in Python. Use this where a source writes
    thousands of small files and a COPY per file would cost more than the
    rows do — the POS feed writes one file per store per night."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(header)
        writer.writerows(rows)


def cell(value) -> str:
    """A value as a landing file spells it. Nothing here is locale-formatted
    and nothing carries a float: the two feeds that do are hand-authored."""
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, dt.datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, dt.date):
        return value.isoformat()
    return str(value)
