"""MIG-63 — does the bridge answer the calendar the AutoSys boxes obey?

Never ships to the agent. It runs beside the scored tree, with the tree root on
`PYTHONPATH`, so it loads the patched DAG module the same way a DagBag would.

**No authored dates.** Every expected day is parsed out of
`legacy/autosys/calendars/retail_2026.cal` in this same session. That file is
read-only to the task (the world's `do_not_modify` floor covers `legacy/**`),
so the two sides of every assertion below cannot drift apart: if the export
changes, the expectation changes with it.

**Why a verifier rather than a run.** `sc_autosys_bridge` shells `ssh` to a box
that is not in the container. `trigger_nightly` is red whatever the calendar
says, so a `dag_runs` check here would grade the network. The calendar reader is
plain Python with no warehouse and no socket in it, and calling it is the whole
question the ticket asks.

**What the four tests separate.** The shipped reader is wrong three ways at
once — it treats `/* ... */` as a comment only when the line starts with `*`, it
takes the first token of a line when a date line carries six, and it never turns
`MM/DD/YYYY` round into the `YYYY-MM-DD` a run's `ds` is written in. A repair
that lands one or two of the three still fails: the count test catches the
first two, and the membership tests catch the third.
"""

from __future__ import annotations

import datetime as dt
import importlib.util
import sys
import re

import pytest

# The scored tree is on PYTHONPATH (scoring.build_score_env), so the house
# library resolves the tree root the same way every DAG in the world does —
# from its own file, whatever the working directory happens to be.
from include.lib import workspace_root

BRIDGE = workspace_root() / "projects" / "supply" / "dags" / "sc_autosys_bridge.py"
CALENDAR = (
    workspace_root() / "legacy" / "autosys" / "calendars" / "retail_2026.cal"
)

#: An `autocal_asc` export: `/* ... */` comments, a `calendar:` line, then the
#: dates, six to a line, US-style. The comments carry dates too — the header
#: lists the holidays that were left out — so they come out before anything is
#: read as a day.
_COMMENT = re.compile(r"/\*.*?\*/", re.S)
_DATE = re.compile(r"\b(\d{2})/(\d{2})/(\d{4})\b")


def export_days() -> set[dt.date]:
    """The days `retail_2026.cal` names."""
    body = _COMMENT.sub(" ", CALENDAR.read_text(encoding="utf-8"))
    return {
        dt.date(int(y), int(m), int(d)) for m, d, y in _DATE.findall(body)
    }


def days_in(year: int) -> set[dt.date]:
    """Every date in a calendar year."""
    start, end = dt.date(year, 1, 1), dt.date(year, 12, 31)
    return {start + dt.timedelta(n) for n in range((end - start).days + 1)}


@pytest.fixture(scope="module")
def bridge():
    """The patched DAG module, loaded from its file.

    Loading it builds the DAG object, which is what a parse does and is the
    only thing this file asks of it.
    """
    spec = importlib.util.spec_from_file_location("sc_autosys_bridge_under_test", BRIDGE)
    assert spec and spec.loader, f"{BRIDGE} is not importable"
    module = importlib.util.module_from_spec(spec)
    # Register the module before executing it: dataclasses resolves string
    # annotations through sys.modules, and a hand-loaded module that is not
    # there fails on any @dataclass it defines. A fair repair lost a trial
    # to this before the line below.
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    for name in ("calendar_days", "is_due"):
        assert hasattr(module, name), (
            f"sc_autosys_bridge no longer has {name}(); the ticket says both keep "
            "their names, because the DAG is where the next reader looks for this"
        )
    return module


@pytest.fixture(scope="module")
def expected() -> set[dt.date]:
    days = export_days()
    assert days, "the export named no days — the fixture itself is wrong"
    return days


def test_the_bridge_names_as_many_days_as_the_export_does(bridge, expected):
    """One count against the other.

    The shipped reader answers with fifty entries: the first date of each date
    line, plus `/*` and `calendar:`, which are not dates at all. Nothing about
    the return type is pinned here — a set of dates and a set of strings both
    have a length — so a reader that hands back `datetime.date` objects is
    graded on the same footing as one that hands back ISO strings.
    """
    got = bridge.calendar_days()
    assert len(got) == len(expected), (
        f"calendar_days() named {len(got)} day(s); the export names {len(expected)}"
    )


def test_every_day_the_export_names_is_a_calendar_day(bridge, expected):
    """`is_due` says yes to all of them, from the ISO date a run is given."""
    wrong = sorted(d for d in expected if not bridge.is_due(d.isoformat()))
    assert not wrong, (
        f"{len(wrong)} day(s) the export names came back as not a calendar day, "
        f"starting {wrong[:3]}"
    )


def test_no_day_the_export_leaves_out_is_a_calendar_day(bridge, expected):
    """And no to the weekends and the holidays inside the same year.

    Scoped to the years the export covers, so this test asks only about days
    the file had an opinion about.
    """
    covered = {d.year for d in expected}
    left_out = {d for year in covered for d in days_in(year)} - expected
    wrong = sorted(d for d in left_out if bridge.is_due(d.isoformat()))
    assert not wrong, (
        f"{len(wrong)} day(s) the export leaves out came back as calendar days, "
        f"starting {wrong[:3]}"
    )


def test_the_bridge_answers_for_the_export_and_for_nothing_else(bridge, expected):
    """A date outside the export's year is not a calendar day.

    The ticket says this in as many words, and it is the difference between
    reading the file and re-deriving a rule from it. A reader that answers
    "weekday, and not one of these eleven holidays" is right about the year the
    file covers and is inventing an answer for every other year — including the
    two years of estate history the export was never exported for.
    """
    covered = {d.year for d in expected}
    outside = {
        d
        for year in range(min(covered) - 2, max(covered) + 2)
        if year not in covered
        for d in days_in(year)
    }
    wrong = sorted(d for d in outside if bridge.is_due(d.isoformat()))
    assert not wrong, (
        f"{len(wrong)} date(s) outside the export's year came back as calendar "
        f"days, starting {wrong[:3]}"
    )
