"""Reading `config/alerts.yml`, and deciding whether a subject is quiet.

`contracts/alerting.md` AL-3 makes the config the definition: an alert exists
because it is in the file, with the table as a string, the check, the
threshold and the owning team. Nothing is defined in the DAG. This module is
the reader, and it holds no thresholds of its own.

AL-2 is the other half. A day Copperline agreed would be quiet is not an
anomaly, and neither is a known peak. Two files say which:
`ops/calendar/quiet-days.yml` holds the agreed windows, and
`raw.market_calendar` holds the trading days per market. `is_quiet` reads the
first; the second belongs to whoever writes a check that needs it, through
`include.lib.calendar`.
"""

from __future__ import annotations

import datetime as dt
from typing import Any

from include.lib import warehouse, workspace_root

__all__ = ["CONFIG_PATH", "QUIET_DAYS_PATH", "CHECKS", "subjects", "quiet_days",
           "is_quiet", "measure", "unowned", "record"]

#: The alert definitions. Read when the DAG runs, never at parse.
CONFIG_PATH = "config/alerts.yml"

#: The agreed quiet windows, owned by growth.
QUIET_DAYS_PATH = "ops/calendar/quiet-days.yml"

#: The checks a subject may ask for. `freshness` is hours since the newest
#: row; `row_count` is the rows landed for the day; `null_rate` is the share
#: of the day's rows with the named column empty, in per cent.
CHECKS = ("freshness", "row_count", "null_rate")


def subjects() -> list[dict[str, Any]]:
    """Every alert subject in the config, in file order.

    Each one carries `table`, `check`, `threshold`, `owner`, and whatever the
    check needs — `column` for `null_rate`, `timestamp_column` for
    `freshness`. A subject with no `owner` is returned as it is; AL-1 makes
    that a defect in the config, and the DAG is the place that says so.
    """
    import yaml

    document = yaml.safe_load(
        (workspace_root() / CONFIG_PATH).read_text(encoding="utf-8")
    ) or {}
    return list(document.get("subjects") or [])


def quiet_days() -> dict[dt.date, str]:
    """The agreed quiet days, as `{date: reason}`.

    The file carries single days as `date:` and spans as `dates:`; both come
    back flattened to one entry per day.
    """
    import yaml

    document = yaml.safe_load(
        (workspace_root() / QUIET_DAYS_PATH).read_text(encoding="utf-8")
    ) or []
    windows: dict[dt.date, str] = {}
    for entry in document:
        reason = entry.get("reason", "agreed quiet day")
        days = entry.get("dates") or ([entry["date"]] if "date" in entry else [])
        for day in days:
            windows[_as_date(day)] = reason
    return windows


def is_quiet(day: str | dt.date) -> str | None:
    """The reason `day` was agreed to be quiet, or None.

    A subject that is low or absent on a quiet day is behaving as agreed. AL-2
    makes paging on one a defect in the alert rather than a fact about the
    feed.
    """
    return quiet_days().get(_as_date(day))


def measure(subject: dict[str, Any], ds: str | dt.date) -> float:
    """Run one subject's check against its table and return the number.

    The comparison against the threshold is the caller's, because the
    direction differs per check: freshness and null rate fail high, row count
    fails low.
    """
    day = _as_date(ds)
    table = warehouse.qualify(subject["table"])
    check = subject["check"]
    if check not in CHECKS:
        raise ValueError(f"{subject['table']}: unknown check {check!r}")
    with warehouse.connect(read_only=True) as con:
        if check == "freshness":
            column = subject.get("timestamp_column", "loaded_at")
            hours = con.execute(
                f"SELECT date_diff('hour', max({column}), "
                f"DATE '{day}' + INTERVAL 1 DAY) FROM {table}"
            ).fetchone()
            return float(hours[0] or 0)
        if check == "row_count":
            column = subject.get("partition_column", "ds")
            rows = con.execute(
                f"SELECT count(*) FROM {table} WHERE {column}::DATE = DATE '{day}'"
            ).fetchone()
            return float(rows[0] or 0)
        column = subject["column"]
        partition = subject.get("partition_column", "ds")
        rate = con.execute(
            f"SELECT 100.0 * count(*) FILTER ({column} IS NULL) "
            f"/ nullif(count(*), 0) FROM {table} "
            f"WHERE {partition}::DATE = DATE '{day}'"
        ).fetchone()
        return float(rate[0] or 0)


def unowned(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Subjects with no owning team, or an owner that is not a team.

    §AL-1: the owner is a rotation, and a name that is not one of the six
    teams has no rotation behind it. Both cases mean the same thing — nobody
    is woken — so they come back in one list.
    """
    from include.lib.notify import TEAMS

    return [dict(entry, reason="no owner" if not entry.get("owner")
                 else f"{entry['owner']} is not a team")
            for entry in entries
            if entry.get("owner") not in TEAMS]


def record(results: list[dict[str, Any]], ds: str | dt.date) -> int:
    """Write every evaluation to `ops.alert_history`, breach or not.

    The runs that did not fire are the record that says whether a threshold
    is doing anything. `contracts/alerting.md` names "nobody reviews the
    threshold set" as an open item; this table is what a review would read.
    """
    day = _as_date(ds)
    rows = [
        {"ds": day, "table_name": result["table"], "check_name": result["check"],
         "owner": result.get("owner"), "measured": float(result["measured"]),
         "threshold": float(result["threshold"]),
         "quiet_reason": result.get("quiet_day"),
         "breached": bool(result["breached"])}
        for result in results
    ]
    return warehouse.delete_insert("ops.alert_history", "ds", day, rows)


def _as_date(value: Any) -> dt.date:
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value
    return dt.date.fromisoformat(str(value)[:10])
