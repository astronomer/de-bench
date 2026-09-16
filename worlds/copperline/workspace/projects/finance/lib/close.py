"""The month-end close: which month, what it holds, and what it hands over.

`docs/finance-policy.md` §REV-8 is the rule this module implements. A fiscal
month closes on the 5th business day of the following month; before that it is
open and nothing is final, and after it nothing restates.

The close file itself is C-2 in `docs/report-registry.md`, it is due to the
controller's office on the 6th business day, and `contracts/finance-close.md`
governs what it contains.
"""

from __future__ import annotations

import csv
import datetime as dt
from pathlib import Path

from include.lib import calendar as cal
from include.lib import warehouse, workspace_root

__all__ = ["EXPORT_ROOT", "closing_month", "close_date", "recognised_daily",
           "recognised_monthly", "write_close_file", "verify_close_file"]

EXPORT_ROOT = workspace_root() / "exports" / "close"

#: The five legal entities, in the order the close file lists them.
ENTITIES = ("CL-US", "CL-GB", "CL-IE", "CL-DE", "CL-MX")

#: The columns the controller's office reads, in order.
HEADER = ["fiscal_month", "entity", "recognized_cents", "ledger_cents",
          "difference_cents", "is_closed"]


def closing_month(run_ds: str) -> str:
    """The fiscal month this run closes, from the day the run fires.

    The DAG fires on the 1st, so the month being closed is the one the previous
    day fell in. Read from `raw.fiscal_calendar` rather than computed: the
    calendar is 4-5-4 and a fiscal month is not a calendar month, whatever a
    particular year makes it look like.
    """
    previous = dt.date.fromisoformat(run_ds) - dt.timedelta(days=1)
    row = cal.fiscal(previous)
    return f"{row['fiscal_year']}-P{int(row['fiscal_period']):02d}"


def close_date(run_ds: str, market: str = "US") -> str:
    """The 5th business day of the month the run fires in. §REV-8.

    `business_days_between` is half open — the first day counts if it trades
    and the last never does — so the 5th business day is the day `d` where the
    count from the 1st is 4.
    """
    first = dt.date.fromisoformat(run_ds).replace(day=1)
    day = first
    while cal.business_days_between(first, day, market) < 4:
        day += dt.timedelta(days=1)
    return day.isoformat()


def recognised_daily(fiscal_month: str, table: str = "marts.revenue_recognized_daily") -> int:
    """How many daily recognition rows the month holds. Raises when it holds none.

    The daily rows are what the monthly figure is summed from, so a month with
    a monthly figure and no daily rows behind it is a figure nobody can take
    apart when the tie fails.
    """
    with warehouse.connect(read_only=True) as con:
        rows = con.execute(
            f"SELECT count(*) FROM {warehouse.qualify(table)} WHERE fiscal_month = ?",
            [fiscal_month],
        ).fetchone()[0]
    if not rows:
        raise ValueError(f"{table} holds no rows for {fiscal_month}")
    return int(rows)


def recognised_monthly(fiscal_month: str,
                       table: str = "marts.revenue_recognized_monthly") -> list[dict]:
    """The month's recognised figure per entity, in cents.

    An entity with no row is returned at zero rather than left out, so the
    close file has the same five lines every month and a missing entity is
    visible as a zero rather than as an absence.
    """
    with warehouse.connect(read_only=True) as con:
        found = dict(con.execute(
            f"SELECT entity, coalesce(sum(recognized_cents), 0)::BIGINT "
            f"FROM {warehouse.qualify(table)} WHERE fiscal_month = ? GROUP BY entity",
            [fiscal_month],
        ).fetchall())
    return [{"fiscal_month": fiscal_month, "entity": entity,
             "recognized_cents": found.get(entity, 0)} for entity in ENTITIES]


def write_close_file(rows: list[dict], ties: list[dict], fiscal_month: str) -> str:
    """Write `exports/close/<fiscal_month>.csv`, and return the path.

    The tie is written into the file beside the figure. The controller's office
    reads the difference column first, and a close file that carried the figure
    without the ledger beside it sent somebody to two systems for one number.
    """
    tie_by_entity = {tie["entity"]: tie for tie in ties}
    EXPORT_ROOT.mkdir(parents=True, exist_ok=True)
    path = EXPORT_ROOT / f"{fiscal_month}.csv"
    partial = path.with_suffix(".csv.partial")
    with partial.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(HEADER)
        for row in rows:
            tie = tie_by_entity.get(row["entity"], {})
            writer.writerow([
                row["fiscal_month"], row["entity"], row["recognized_cents"],
                tie.get("ledger_cents"), tie.get("difference_cents"),
                tie.get("ledger_cents") is not None,
            ])
    partial.replace(path)
    return str(path)


def verify_close_file(path: str, fiscal_month: str) -> int:
    """Five entity lines, a header, and the month on every one of them.

    Returns the line count. The check is small and it has caught the two ways
    this file has gone wrong: a month written into the previous month's file,
    and an entity dropped by a join.
    """
    lines = [line for line in
             Path(path).read_text(encoding="utf-8").splitlines() if line]
    if len(lines) != len(ENTITIES) + 1:
        raise ValueError(
            f"{path}: {len(lines) - 1} entity line(s) and there are {len(ENTITIES)}"
        )
    wrong = [line for line in lines[1:] if not line.startswith(f"{fiscal_month},")]
    if wrong:
        raise ValueError(f"{path}: {len(wrong)} line(s) are not for {fiscal_month}")
    return len(lines)
