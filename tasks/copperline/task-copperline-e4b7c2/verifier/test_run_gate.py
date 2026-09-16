"""MIG-84 — the gate wave 2 hands the boxes it moves.

Never ships to the agent. It runs beside the scored tree, with the tree root on
`PYTHONPATH`, so `include.lib` resolves the workspace the same way every DAG in
the world does.

**No authored expectation.** Every figure below is computed in this session out
of two records the task may not touch — `legacy/autosys/copperline.jil` and
`legacy/autosys/calendars/retail_2026.cal`, both under the world's
`do_not_modify` floor — and out of `raw.market_calendar` in the warehouse the
scorer rebuilds from the image. So an agent that edits the tree cannot move an
answer, and if the world changes the expectation changes with it.

**Why a verifier rather than a run.** The gate is a library module, not a DAG.
`sc_autosys_bridge`, the DAG that will call it, shells `ssh` to a box that is
not in the container, so a `dag_runs` check would grade the network. Calling the
two functions is the whole question the ticket asks.

**What the tests separate.**

  the eight declaring boxes taken as the answer   test 2: `retail_2026` governs
                                                  eighteen jobs, and ten of them
                                                  inherit it from a box
  a list of job names typed into the module       test 3: the retired calendar
                                                  has to come back with its one
                                                  job, from the same walk
  the export read for the gate                    test 5: the export names one
                                                  year and the estate has run
                                                  outside it since 2022
  a weekday-and-holiday rule written out in       test 6: a computed rule has an
    Python instead of the calendar read           answer for 2031 and the
                                                  calendar does not
  the gate asked about the day the run OWNS       test 4: `ds` is the day the
    rather than the day it fires                  run fires, and the two sets
                                                  differ on 110 days of 2026
  a date off the end answered `False`             test 6

Test 1 grades nothing the agent wrote. It asserts the two calendars agree over
the year they share, which is the premise the ticket rests on when it sends the
gate to the warehouse instead of the export. If it ever fails, the world moved
and this task needs rewriting rather than the answer being wrong.
"""

from __future__ import annotations

import datetime as dt
import importlib.util
import sys
import os
import re
from pathlib import Path

import duckdb
import pytest

# The scored tree is on PYTHONPATH (scoring.build_score_env), so the house
# library resolves the tree root from its own file, whatever the working
# directory happens to be.
from include.lib import workspace_root

GATE = workspace_root() / "projects" / "supply" / "lib" / "run_calendar.py"
JIL = workspace_root() / "legacy" / "autosys" / "copperline.jil"
CALENDAR = workspace_root() / "legacy" / "autosys" / "calendars" / "retail_2026.cal"

#: `retail_2026.cal` says "Trading days, US market" in its own header, and
#: `dim_date` calls US the home market in as many words.
HOME_MARKET = "US"

#: The calendar the boxes wave 2 moves obey, and the one `cpl.util.archive.logs`
#: names and the estate does not hold.
LIVE_CALENDAR = "retail_2026"
RETIRED_CALENDAR = "retail_2025"

#: The year the export covers. Read off the export rather than typed: see
#: `export_days`.
_COMMENT = re.compile(r"/\*.*?\*/", re.S)
_DATE = re.compile(r"\b(\d{2})/(\d{2})/(\d{4})\b")

_INSERT = re.compile(r"^insert_job:\s*(\S+)")
_ATTRIBUTE = re.compile(r"^(box_name|run_calendar):\s*(\S+)")


def warehouse_file() -> Path:
    """The warehouse the world's own `DUCKDB_PATH` points at."""
    raw = os.environ.get("DUCKDB_PATH") or "include/data/copperline.duckdb"
    path = Path(raw)
    return path if path.is_absolute() else workspace_root() / path


def export_days() -> set[dt.date]:
    """The days `retail_2026.cal` names.

    An `autocal_asc` export: `/* ... */` comments, a `calendar:` line, then six
    dates to a line in `MM/DD/YYYY`. The header comment lists the holidays that
    were left out, written the same way as the days, so the comments come out
    before anything is read as a date.
    """
    body = _COMMENT.sub(" ", CALENDAR.read_text(encoding="utf-8"))
    return {dt.date(int(y), int(m), int(d)) for m, d, y in _DATE.findall(body)}


def export_jobs() -> dict[str, dict[str, str | None]]:
    """Every job the export still defines, with its box and its own calendar.

    A `delete_job:` line is a job wave 1 took out. It defines nothing and it
    opens no attribute block, so it closes whichever block it follows.
    """
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
    return jobs


def governed_by(calendar: str) -> set[str]:
    """The jobs a run calendar governs, its own line or the box holding it."""
    jobs = export_jobs()

    def governing(name: str | None) -> str | None:
        seen: set[str] = set()
        while name and name in jobs and name not in seen:
            seen.add(name)
            if jobs[name]["calendar"]:
                return jobs[name]["calendar"]
            name = jobs[name]["box"]
        return None

    return {name for name in jobs if governing(name) == calendar}


def declaring(calendar: str) -> set[str]:
    """The jobs that carry the calendar on a line of their own."""
    return {n for n, j in export_jobs().items() if j["calendar"] == calendar}


def days_in(year: int) -> list[dt.date]:
    start, end = dt.date(year, 1, 1), dt.date(year, 12, 31)
    return [start + dt.timedelta(n) for n in range((end - start).days + 1)]


@pytest.fixture(scope="module")
def gate():
    """The module the ticket asks for, loaded from the path it pins."""
    assert GATE.exists(), (
        f"{GATE} is not there; the ticket names that path and the converted "
        "DAGs are going to import it"
    )
    spec = importlib.util.spec_from_file_location("supply_run_calendar_under_test", GATE)
    assert spec and spec.loader, f"{GATE} is not importable"
    module = importlib.util.module_from_spec(spec)
    # Register the module before executing it: dataclasses resolves string
    # annotations through sys.modules, and a hand-loaded module that is not
    # there fails on any @dataclass it defines. A fair repair lost a trial
    # to this before the line below.
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    for name in ("calendared_jobs", "is_run_day"):
        assert hasattr(module, name), (
            f"run_calendar.py has no {name}(); the ticket pins both names, "
            "because the converted DAGs import them"
        )
    return module


@pytest.fixture(scope="module")
def home_calendar() -> dict[dt.date, bool]:
    """The home market's calendar: every date it covers, and whether it trades."""
    path = warehouse_file()
    assert path.exists(), f"no warehouse at {path}"
    con = duckdb.connect(str(path), read_only=True)
    try:
        rows = con.execute(
            "SELECT calendar_date, is_trading_day FROM raw.market_calendar "
            "WHERE market_code = ?",
            [HOME_MARKET],
        ).fetchall()
    finally:
        con.close()
    assert rows, f"raw.market_calendar holds no {HOME_MARKET} rows"
    return {day: bool(trading) for day, trading in rows}


def test_the_export_and_the_warehouse_agree_over_the_year_they_share(home_calendar):
    """The premise, not the answer.

    The ticket sends the gate to the warehouse because the export covers one
    year. That swap is only honest if the two records say the same thing over
    the year they both cover. Nothing the agent wrote is read here.
    """
    export = export_days()
    assert export, "the export named no days — the fixture itself is wrong"
    years = {d.year for d in export}
    assert len(years) == 1, f"the export now spans {sorted(years)}; this task assumed one year"
    year = years.pop()
    warehouse = {d for d, trading in home_calendar.items() if trading and d.year == year}
    assert warehouse == export, (
        f"the {HOME_MARKET} calendar and the export disagree over {year}: "
        f"{len(warehouse - export)} in the warehouse only, "
        f"{len(export - warehouse)} in the export only"
    )


def test_the_calendar_reaches_every_job_its_boxes_hold(gate):
    """A box's calendar governs everything inside the box.

    Eight jobs carry `run_calendar: retail_2026` and eighteen obey it. The ten
    in between are the three ties, two of the tie boxes, the four pack and send
    steps and the month-end close step — every one of them a job a grep for
    `run_calendar:` never sees.
    """
    expected = governed_by(LIVE_CALENDAR)
    declared = declaring(LIVE_CALENDAR)
    assert declared < expected, (
        "the export no longer holds a calendared box with jobs inside it, so "
        "this test grades nothing — the world moved"
    )
    got = set(gate.calendared_jobs(LIVE_CALENDAR))
    assert got == expected, (
        f"calendared_jobs({LIVE_CALENDAR!r}) named {len(got)} job(s); the export "
        f"puts {len(expected)} under that calendar. Missing "
        f"{sorted(expected - got)[:4]}; extra {sorted(got - expected)[:4]}"
    )


def test_the_retired_calendar_answers_from_the_same_walk(gate):
    """`retail_2025` governs one job, and it is not one of the eighteen.

    A module with the live calendar's answer typed into it passes the test
    above and cannot answer this one. `cpl.util.archive.logs` names a calendar
    the estate does not hold and carries `status: OI`, which is why the ticket
    says to answer for any calendar the export mentions.
    """
    expected = governed_by(RETIRED_CALENDAR)
    assert expected, "the export no longer names a second calendar; the world moved"
    got = set(gate.calendared_jobs(RETIRED_CALENDAR))
    assert got == expected, (
        f"calendared_jobs({RETIRED_CALENDAR!r}) named {sorted(got)[:4]}; the export "
        f"puts {sorted(expected)} under it"
    )
    assert not got & governed_by(LIVE_CALENDAR), (
        "a job came back under both calendars, so the walk is not stopping at "
        "the first calendar it meets"
    )


def test_the_gate_names_the_days_the_export_names(gate):
    """Every day of the export's year, both ways.

    `ds` is the day the run fires — `CONVENTIONS.md` says so and the export's
    own header says `BUSDATE` is the day before it — so the gate answers about
    the day the box STARTS. A gate asked about the day the run owns is right
    about the count and wrong about 110 of the 365 days here.
    """
    expected = export_days()
    covered = [d for year in sorted({d.year for d in expected}) for d in days_in(year)]
    wrong_no = sorted(d for d in expected if not gate.is_run_day(d.isoformat()))
    assert not wrong_no, (
        f"{len(wrong_no)} day(s) the export names came back as not a run day, "
        f"starting {wrong_no[:3]}"
    )
    wrong_yes = sorted(d for d in covered if d not in expected and gate.is_run_day(d.isoformat()))
    assert not wrong_yes, (
        f"{len(wrong_yes)} day(s) the export leaves out came back as run days, "
        f"starting {wrong_yes[:3]}"
    )


def test_the_gate_answers_for_the_years_the_export_does_not(gate, home_calendar):
    """A whole year either side of the export's.

    The estate has run since February 2024 and wave 2 will be running after
    2026. A gate that reads `retail_2026.cal` answers no to every night in
    2025, which stops the tie-outs rather than gating them.
    """
    covered = {d.year for d in export_days()}
    outside = sorted(
        d for d in home_calendar
        if d.year not in covered and d.year in (min(covered) - 1, max(covered) + 1)
    )
    assert len(outside) > 300, (
        f"only {len(outside)} date(s) outside the export's year to grade; the "
        "warehouse calendar no longer reaches past it"
    )
    wrong = sorted(d for d in outside if gate.is_run_day(d.isoformat()) != home_calendar[d])
    assert not wrong, (
        f"{len(wrong)} of {len(outside)} date(s) outside the export's year "
        f"disagree with the {HOME_MARKET} calendar, starting "
        f"{[d.isoformat() for d in wrong[:3]]}"
    )


def test_a_night_the_calendar_has_never_heard_of_is_not_a_no(gate, home_calendar):
    """Off the end of the calendar the gate raises rather than answering.

    The two dates are one day either side of what the calendar covers, so they
    move with the world. The ticket puts this in as many words: a gate that
    says "not a trading day" about a night it has no row for stops the estate
    quietly, and that is the failure nobody gets paged for.
    """
    for edge in (min(home_calendar) - dt.timedelta(days=1),
                 max(home_calendar) + dt.timedelta(days=1)):
        try:
            answered = gate.is_run_day(edge.isoformat())
        except Exception:
            continue
        pytest.fail(
            f"is_run_day({edge.isoformat()!r}) answered {answered!r}; the "
            "calendar holds no row for that date, so the gate has nothing to "
            "answer from"
        )
