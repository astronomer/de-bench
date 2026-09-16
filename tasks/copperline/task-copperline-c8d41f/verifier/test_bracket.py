"""MIG-70 — does the bracket close the nights the estate finished, and refuse
the rest?

Never ships to the agent. It runs beside the scored tree, with the tree root on
`PYTHONPATH`, so it loads the patched DAG module the same way a DagBag would.

**No authored dates and no authored roster.** Every night graded below is picked
out of `ops.load_control` in this same session, and every job name expected is
parsed out of `legacy/autosys/copperline.jil` in the same session. Both are
read-only to the task — the world's `do_not_modify` floor covers `legacy/**`,
and the control table is the estate's own record — so the two sides of an
assertion cannot drift apart.

**Why a verifier rather than a run.** The second task of this DAG shells `ssh`
to a box that is not in the container, so a `dag_runs` check would be red
whatever the guard says and would grade the network. The guard is the first
task, it is plain Python over the warehouse, and calling it is the whole
question the ticket asks.

**What the tests separate.**

    the roster, night by night   `due_jobs` against what the control table
                                 holds for all 161 nights since the cutover.
                                 A fixed roster of thirteen is right on the
                                 Sundays and wrong on the other six nights; a
                                 fixed roster of twelve is wrong on the Sundays.
    the roster off the table     `due_jobs` for two nights the estate has no
                                 rows for at all. A roster taken from what the
                                 night wrote answers "nothing owed" and closes
                                 a night that never ran.
    the guard                    four nights the estate finished must close,
                                 and four that did not must not — the two the
                                 control table calls failed, and the two with
                                 no rows at all.
"""

from __future__ import annotations

import datetime as dt
import importlib.util
import sys
import re

import duckdb
import pytest

# The scored tree is on PYTHONPATH (scoring.build_score_env), so the house
# library resolves the tree root and the warehouse the same way every DAG in
# the world does — from its own file and from DUCKDB_PATH, whatever the working
# directory happens to be.
from include.lib import warehouse, workspace_root

DAG_FILE = (
    workspace_root() / "projects" / "supply" / "dags" / "sc_pdi_history_complete.py"
)
JIL = workspace_root() / "legacy" / "autosys" / "copperline.jil"

#: Read straight from the live file rather than through
#: `warehouse.connect(read_only=True)`, which copies the whole warehouse to a
#: snapshot. The patched guard is what exercises that path; this only needs to
#: read a control table.
DB = str(warehouse.warehouse_path())
CONTROL = warehouse.qualify("ops.load_control")

#: The box every nightly load job sits in. `cpl.nightly.stage`, `.recon` and
#: `.dist` hold the other three quarters of the night and write no control row.
LOAD_BOX = "cpl.nightly.load"

#: How AutoSys writes the days a box may start, Sunday first.
DAY_CODES = ("su", "mo", "tu", "we", "th", "fr", "sa")


# --- what the export says --------------------------------------------------

def export_boxes() -> tuple[dict[str, str], list[str]]:
    """The nightly load jobs the export names, and the ones wave 1 took out.

    Keyed by the name the job writes to `ops.load_control` under, which is the
    box name without its `cpl.` prefix, against the `days_of_week` the box
    carries.
    """
    text = JIL.read_text(encoding="utf-8")
    migrated = [
        name.removeprefix("cpl.")
        for name in re.findall(r"(?m)^delete_job:\s*(\S+)\s*$", text)
    ]
    surviving: dict[str, str] = {}
    for block in re.split(r"(?m)^insert_job:", text)[1:]:
        if not re.search(rf"(?m)^box_name:\s*{re.escape(LOAD_BOX)}\s*$", block):
            continue
        days = re.search(r"(?m)^days_of_week:\s*(.*?)\s*$", block)
        surviving[block.split()[0].removeprefix("cpl.")] = days.group(1) if days else ""
    return surviving, migrated


def owed_by_the_export(night: dt.date) -> set[str]:
    """The jobs the export says that night owed a row for."""
    surviving, _ = export_boxes()
    code = DAY_CODES[(night.weekday() + 1) % 7]
    return {
        job
        for job, days in surviving.items()
        if not days or days == "all" or code in days.replace(",", " ").split()
    }


# --- what the control table shows ------------------------------------------

def control_rows() -> dict[dt.date, dict[str, str]]:
    """Every night since the cutover, against the jobs it wrote and how each
    one ended.

    The cutover is derived, not typed: wave 1's nine jobs write their last row
    the night before this deployment took the estate over, so the night after
    the last of those rows is the first night this deployment owns.
    """
    _, migrated = export_boxes()
    assert migrated, "the export names no `delete_job` line — the fixture is wrong"
    marks = ", ".join("?" for _ in migrated)
    con = duckdb.connect(DB, read_only=True)
    try:
        last_wave_one = con.execute(
            f"SELECT max(business_date) FROM {CONTROL} WHERE job_name IN ({marks})",
            migrated,
        ).fetchone()[0]
        assert last_wave_one is not None, (
            "no wave 1 job ever wrote a control row — the fixture is wrong"
        )
        rows = con.execute(
            f"""SELECT business_date, job_name, load_status FROM {CONTROL}
                WHERE business_date > ?::DATE ORDER BY business_date, job_name""",
            [last_wave_one],
        ).fetchall()
    finally:
        con.close()
    nights: dict[dt.date, dict[str, str]] = {}
    for day, job, status in rows:
        nights.setdefault(day, {})[job] = status
    return nights


# --- fixtures ---------------------------------------------------------------

@pytest.fixture(scope="module")
def bracket():
    """The patched DAG module, loaded from its file.

    Loading it builds the DAG object, which is what a parse does and is the
    only thing this file asks of it.
    """
    spec = importlib.util.spec_from_file_location(
        "sc_pdi_history_complete_under_test", DAG_FILE
    )
    assert spec and spec.loader, f"{DAG_FILE} is not importable"
    module = importlib.util.module_from_spec(spec)
    # Register the module before executing it: dataclasses resolves string
    # annotations through sys.modules, and a hand-loaded module that is not
    # there fails on any @dataclass it defines. A fair repair lost a trial
    # to this before the line below.
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    for name in ("due_jobs", "check_loads_finished"):
        assert hasattr(module, name), (
            f"sc_pdi_history_complete no longer has {name}(); the ticket says "
            "both keep their names, because the DAG is where the next person "
            "closing a repair night looks for them"
        )
    return module


@pytest.fixture(scope="module")
def nights() -> dict[dt.date, dict[str, str]]:
    rows = control_rows()
    assert len(rows) > 100, f"only {len(rows)} night(s) since the cutover"
    return rows


@pytest.fixture(scope="module")
def finished(nights) -> list[dt.date]:
    """Four nights the estate finished: the first and the last of each kind of
    weekday it has. A repair that special-cases one date passes one of them."""
    done = [
        day
        for day, jobs in sorted(nights.items())
        if all(status == "complete" for status in jobs.values())
    ]
    sundays = [d for d in done if d.weekday() == 6]
    weekdays = [d for d in done if d.weekday() != 6]
    assert sundays and weekdays, "the estate finished no Sunday, or no other night"
    return sorted({sundays[0], sundays[-1], weekdays[0], weekdays[-1]})


@pytest.fixture(scope="module")
def unfinished(nights) -> list[dt.date]:
    """The first and the last night the estate did not finish."""
    short = [
        day
        for day, jobs in sorted(nights.items())
        if any(status != "complete" for status in jobs.values())
    ]
    assert short, "the estate finished every night, so nothing here can fail"
    return sorted({short[0], short[-1]})


@pytest.fixture(scope="module")
def no_record(nights) -> list[dt.date]:
    """A Sunday and a Monday the control table holds no row for at all.

    The first of each after the last night the estate has a record of, so both
    are outside the table by construction and one of each weekday kind.
    """
    last = max(nights)
    sunday = last + dt.timedelta(days=(6 - last.weekday()) % 7 or 7)
    monday = sunday + dt.timedelta(days=1)
    for day in (sunday, monday):
        assert day not in nights, f"{day} has control rows; the fixture is wrong"
    return [sunday, monday]


# --- the roster -------------------------------------------------------------

def test_the_night_owed_what_the_control_table_shows_it_ran(bracket, nights):
    """`due_jobs` against the estate's own record, night by night.

    Nothing about the return type is pinned — a list and a set both go through
    `set()` — and nothing about the order is either.
    """
    wrong = []
    for day, jobs in sorted(nights.items()):
        owed = set(bracket.due_jobs(day.isoformat()))
        ran = set(jobs)
        if owed != ran:
            wrong.append((day, sorted(owed - ran), sorted(ran - owed)))
    assert not wrong, (
        f"{len(wrong)} of {len(nights)} night(s) came back with a roster the "
        f"control table disagrees with, starting {wrong[:3]}"
    )


def test_a_night_with_no_record_still_owes_what_the_export_says(bracket, no_record):
    """The roster comes from the export, not from what the night wrote.

    A roster taken from `ops.load_control` answers "nothing owed" for a night
    that wrote nothing, and a guard built on it closes the bracket over a night
    that never ran — which is the one night the bracket exists to refuse.
    """
    for day in no_record:
        owed = set(bracket.due_jobs(day.isoformat()))
        expected = owed_by_the_export(day)
        assert owed == expected, (
            f"{day}: due_jobs named {len(owed)} job(s), the export names "
            f"{len(expected)} — missing {sorted(expected - owed)}, extra "
            f"{sorted(owed - expected)}"
        )


# --- the guard --------------------------------------------------------------

def test_the_bracket_closes_every_night_the_estate_finished(bracket, finished):
    """Four nights, every row of each of them `complete`."""
    refused = []
    for day in finished:
        try:
            bracket.check_loads_finished(day.isoformat())
        except Exception as why:  # noqa: BLE001 - the message is the finding
            refused.append(f"{day}: {why}")
    assert not refused, (
        f"{len(refused)} night(s) the estate finished were refused: {refused}"
    )


def test_the_bracket_refuses_a_night_the_estate_did_not_finish(bracket, unfinished):
    """A night with a failed load is not a night whose history is complete.

    This is what a guard deleted to get the DAG green fails on.
    """
    for day in unfinished:
        with pytest.raises(Exception):
            bracket.check_loads_finished(day.isoformat())


def test_the_bracket_refuses_a_night_the_estate_has_no_record_of(bracket, no_record):
    """And neither is a night that started nothing at all."""
    for day in no_record:
        with pytest.raises(Exception):
            bracket.check_loads_finished(day.isoformat())
