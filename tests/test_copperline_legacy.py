"""The copperline legacy estate agrees with itself and with the fixtures.

`worlds/copperline/workspace/legacy/` is read-only archaeology: a Pentaho job
and its transformations, an SSIS package, an AutoSys export with its calendar,
a spreadsheet inventory, and three Airflow 2 DAGs from the acquired company.
Nothing runs it. Its whole job is to be read, so what it says has to be true.

Three things could rot without anyone noticing, and each one is asserted here:

1. **The job names.** `tools/gen_copperline/extracts/ops_control.py` is the
   source: nine jobs moved in wave 1, thirteen still run. The JIL carries the
   thirteen as boxes and the nine as `delete_job` stanzas, the spreadsheet
   carries all twenty-two, and `docs/runbooks/orchestrator-migration.md` names
   the nine. A name that drifts in one place and not the others turns a
   readable estate into a contradiction.

2. **The three dead jobs.** The spreadsheet lists three jobs that exist nowhere
   else in the estate. That is the find: it is only a find while the count is
   exactly three, so the test cross-references every row.

3. **The Airflow 2 DAGs.** They are text. They must stay valid Python, so an
   agent can read them, and they must stay outside any `dags/` folder the world
   loads, so the DagBag never tries to import them — every import path in them
   was removed in Airflow 3.
"""

import ast
import csv
import datetime as dt
import re
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

import pytest

from de_bench.tasks import repo_root

sys.path.insert(0, str(repo_root() / "tools"))

from gen_copperline.extracts import calendars, ops_control   # noqa: E402

WORKSPACE = repo_root() / "worlds" / "copperline" / "workspace"
LEGACY = WORKSPACE / "legacy"
JIL = LEGACY / "autosys" / "copperline.jil"
CAL = LEGACY / "autosys" / "calendars" / "retail_2026.cal"
TIDAL = LEGACY / "tidal" / "inventory_jobs.csv"
PDI = LEGACY / "pdi"
AF2 = LEGACY / "northwave-airflow" / "dags"

SURVIVING = [name for name, _stream, _cadence in ops_control.SURVIVING]
MIGRATED = [name for name, _stream, _cadence in ops_control.MIGRATED]

XML_FILES = sorted(
    p for p in LEGACY.rglob("*")
    if p.suffix in (".kjb", ".ktr", ".dtsx", ".config")
)


# --- the estate is where it says it is -------------------------------------

def test_the_estate_ships_the_files_the_spec_names():
    expected = {
        "pdi/nightly_load.kjb",
        "pdi/README.txt",
        "pdi/trn/set_constants.ktr",
        "pdi/trn/resolve_load_control.ktr",
        "pdi/trn/load_inventory.ktr",
        "pdi/trn/load_movements.ktr",
        "pdi/trn/load_carrier_scans.ktr",
        "pdi/trn/load_supplier_master.ktr",
        "pdi/trn/history_complete.ktr",
        "ssis/SupplierMaster.dtsx",
        "ssis/SupplierMaster.dtsx.config",
        "autosys/copperline.jil",
        "autosys/calendars/retail_2026.cal",
        "tidal/inventory_jobs.csv",
        "northwave-airflow/requirements.txt",
        "northwave-airflow/dags/nwv_daily_sales.py",
        "northwave-airflow/dags/nwv_account_sync.py",
        "northwave-airflow/dags/nwv_stock_feed.py",
    }
    found = {str(p.relative_to(LEGACY)) for p in LEGACY.rglob("*") if p.is_file()}
    assert found == expected


# --- 1. the XML parses ------------------------------------------------------

@pytest.mark.parametrize("path", XML_FILES, ids=lambda p: p.name)
def test_xml_parses(path):
    ET.parse(path)


def test_the_pdi_job_is_eight_entries_over_seven_transformations():
    """Eight entries: START and the seven child transformations, in one chain.

    The bracket is closed once, at the end, for the whole job. A chain that
    grows a second `history_complete` closes it per step instead.
    """
    job = ET.parse(PDI / "nightly_load.kjb").getroot()
    entries = [e.findtext("name") for e in job.findall("entries/entry")]
    assert entries == [
        "START",
        "set_constants",
        "resolve_load_control",
        "load_inventory",
        "load_movements",
        "load_carrier_scans",
        "load_supplier_master",
        "history_complete",
    ]
    hops = [(h.findtext("from"), h.findtext("to")) for h in job.findall("hops/hop")]
    assert hops == list(zip(entries, entries[1:]))


def test_every_transformation_the_job_calls_exists_and_owns_its_name():
    job = ET.parse(PDI / "nightly_load.kjb").getroot()
    for entry in job.findall("entries/entry"):
        if entry.findtext("type") != "TRANS":
            continue
        name = entry.findtext("name")
        called = entry.findtext("filename")
        assert called.startswith("${Internal.Entry.Current.Directory}/")
        path = PDI / called.split("/", 1)[1]
        assert path.exists(), f"{name} points at a file that is not there"
        assert ET.parse(path).getroot().findtext("info/name") == name


def test_the_control_table_is_read_once_and_only_in_the_control_step():
    """Re-resolving the mark inside a load step loads the same rows twice."""
    readers = [p.name for p in sorted(PDI.glob("trn/*.ktr"))
               if "ops.load_control" in p.read_text()]
    assert readers == ["history_complete.ktr", "resolve_load_control.ktr"]
    sql = (PDI / "trn" / "resolve_load_control.ktr").read_text()
    exported = re.findall(r"<variable_name>(\w+)</variable_name>", sql)
    assert len(exported) == 4


def test_the_ssis_package_keeps_its_or_branch_and_its_disabled_task():
    dts = "{www.microsoft.com/SqlServer/Dts}"
    root = ET.parse(LEGACY / "ssis" / "SupplierMaster.dtsx").getroot()
    disabled = [e.get(dts + "ObjectName")
                for e in root.iter(dts + "Executable")
                if e.get(dts + "Disabled") == "True"]
    assert len(disabled) == 1
    constraints = root.findall(f"{dts}PrecedenceConstraints/{dts}PrecedenceConstraint")
    assert any(c.get(dts + "Value") == "1" for c in constraints), "no failure path"
    ored = [c for c in constraints if c.get(dts + "LogicalAnd") == "False"]
    assert len({c.get(dts + "To") for c in ored}) == 1, "OR precedence needs one target"
    assert len(ored) == 2


# --- 2. the job names cross-reference --------------------------------------

def _jil_jobs():
    """(boxes, commands, deleted) from the JIL export."""
    boxes, commands, deleted = [], [], []
    for line in JIL.read_text().splitlines():
        insert = re.match(r"^insert_job:\s+(\S+)\s+job_type:\s+(\w)", line)
        if insert:
            (boxes if insert.group(2) == "b" else commands).append(insert.group(1))
        delete = re.match(r"^delete_job:\s+(\S+)\s*$", line)
        if delete:
            deleted.append(delete.group(1))
    return boxes, commands, deleted


def test_the_jil_carries_thirty_four_boxes():
    boxes, _commands, _deleted = _jil_jobs()
    assert len(boxes) == 34
    assert len(set(boxes)) == 34


def test_the_surviving_thirteen_are_the_job_boxes_that_are_left():
    boxes, _commands, _deleted = _jil_jobs()
    job_boxes = [b for b in boxes if re.fullmatch(r"cpl\.nightly_\w+", b)]
    assert sorted(job_boxes) == sorted("cpl." + name for name in SURVIVING)


def test_wave_one_is_deleted_from_the_jil_and_nowhere_else_in_it():
    boxes, commands, deleted = _jil_jobs()
    assert sorted(deleted) == sorted("cpl." + name for name in MIGRATED)
    still_scheduled = [j for j in boxes + commands
                       if any(name in j for name in MIGRATED)]
    assert still_scheduled == []


def test_every_kitchen_call_names_a_job_that_still_runs():
    calls = re.findall(r"-param:JOB_NAME=(\S+)", JIL.read_text())
    assert sorted(calls) == sorted(SURVIVING)
    assert "-param:BUSINESS_DATE=" in JIL.read_text()


def test_the_shipped_runbook_names_the_same_nine():
    runbook = (WORKSPACE / "docs" / "runbooks" / "orchestrator-migration.md").read_text()
    named = re.findall(r"`(nightly_\w+)`", runbook)
    assert sorted(named) == sorted(MIGRATED)


# --- the spreadsheet --------------------------------------------------------

def _tidal_rows():
    """The rows a person would call data: the ones with a sequence number.

    Everything else in the file is the residue of a spreadsheet that had
    merged headings, group labels and blank spacers before it was exported.
    """
    with TIDAL.open(newline="") as handle:
        rows = list(csv.reader(handle))
    return [r for r in rows if r and r[0].isdigit()], rows


def test_the_inventory_holds_sixty_one_numbered_rows():
    data, rows = _tidal_rows()
    assert len(data) == 61
    assert [int(r[0]) for r in data] == list(range(1, 62))
    assert all(len(r) == 9 for r in rows), "the export padded every row to 9 columns"


def test_the_inventory_carries_the_merged_header_residue():
    _data, rows = _tidal_rows()
    superheader = next(r for r in rows if r[2] == "Schedule")
    assert superheader[5] == "Migration"
    assert superheader[3] == superheader[4] == superheader[6] == ""
    header = rows[rows.index(superheader) + 1]
    assert header[:3] == ["Seq", "Job Name", "Frequency"]


def test_the_inventory_lists_all_twenty_two_nightly_jobs():
    data, _rows = _tidal_rows()
    listed = {r[1] for r in data}
    assert set(SURVIVING) <= listed
    assert set(MIGRATED) <= listed


def test_three_jobs_in_the_inventory_exist_nowhere_else_in_the_estate():
    data, _rows = _tidal_rows()
    elsewhere = "\n".join(
        p.read_text(errors="replace")
        for p in sorted(LEGACY.rglob("*")) if p.is_file() and p != TIDAL
    )
    dead = sorted(r[1] for r in data if r[1] not in elsewhere)
    assert dead == ["cpl.stage.erp.costing", "nightly_edi_852", "nwv_planogram_feed"]


# --- the run calendar -------------------------------------------------------

def _calendar_days():
    days = []
    for line in CAL.read_text().splitlines():
        if line.startswith("/*") or not line.strip():
            continue
        if line.startswith("calendar:"):
            continue
        for token in line.split():
            days.append(dt.datetime.strptime(token, "%m/%d/%Y").date())
    return days


def test_the_calendar_is_the_us_trading_days_of_2026():
    days = _calendar_days()
    holidays = {dt.date.fromisoformat(d)
                for d in calendars._HOLIDAYS["US"] if d.startswith("2026")}
    day = dt.date(2026, 1, 1)
    expected = []
    while day.year == 2026:
        if day.weekday() < 5 and day not in holidays:
            expected.append(day)
        day += dt.timedelta(days=1)
    assert days == expected


def test_the_boxes_that_name_the_calendar_are_the_ones_it_exists_for():
    """The nightly load runs every night, so it must not carry a calendar that
    skips days the control table has rows for."""
    text = JIL.read_text()
    stanzas = [s for s in text.split("\n\n") if s.startswith("insert_job:")]
    with_calendar = {
        re.match(r"insert_job:\s+(\S+)", s).group(1)
        for s in stanzas if "run_calendar: retail_2026" in s
    }
    assert not any(re.fullmatch(r"cpl\.nightly_\w+", name) for name in with_calendar)
    assert with_calendar


# --- 3. the Airflow 2 DAGs --------------------------------------------------

AF2_FILES = sorted(AF2.glob("*.py"))
AF3_REMOVED = (
    "airflow.operators.python_operator",
    "airflow.operators.bash_operator",
    "airflow.operators.dummy_operator",
    "airflow.operators.postgres_operator",
    "airflow.operators.subdag_operator",
    "airflow.hooks.postgres_hook",
    "airflow.contrib.sensors.file_sensor",
    "airflow.contrib.operators.ssh_operator",
)


def test_there_are_three_of_them():
    assert [p.name for p in AF2_FILES] == [
        "nwv_account_sync.py", "nwv_daily_sales.py", "nwv_stock_feed.py",
    ]


@pytest.mark.parametrize("path", AF2_FILES, ids=lambda p: p.name)
def test_the_af2_dag_is_valid_python(path):
    compile(path.read_text(), str(path), "exec")


@pytest.mark.parametrize("path", AF2_FILES, ids=lambda p: p.name)
def test_the_af2_dag_imports_only_paths_airflow_3_removed(path):
    """Reading them has to work; importing them must not. Every airflow import
    in these files points at a module that is gone at the pinned version."""
    tree = ast.parse(path.read_text())
    modules = {node.module for node in ast.walk(tree)
               if isinstance(node, ast.ImportFrom) and node.module}
    airflow_modules = {m for m in modules if m.split(".")[0] == "airflow"}
    assert airflow_modules & set(AF3_REMOVED), "no Airflow 2 idiom left to read"


def test_the_af2_dags_sit_outside_every_folder_the_world_loads():
    """The world's DagBag walks `projects/<team>/dags/`. These are under
    `legacy/`, which is not one of them, and there is no other `dags` folder
    inside the estate."""
    folders = {p for p in LEGACY.rglob("dags") if p.is_dir()}
    assert folders == {AF2}
    assert not (WORKSPACE / "projects").exists() or not any(
        AF2.samefile(p) for p in (WORKSPACE / "projects").rglob("dags") if p.is_dir()
    )


def test_the_pins_the_northwave_box_ran_are_airflow_2():
    pins = (LEGACY / "northwave-airflow" / "requirements.txt").read_text()
    assert re.search(r"^apache-airflow==2\.\d+\.\d+$", pins, re.M)


# --- the estate holds source only ------------------------------------------

def test_the_estate_carries_no_build_droppings():
    junk = [p for p in LEGACY.rglob("*")
            if p.name in ("__pycache__", ".DS_Store") or p.suffix == ".pyc"]
    assert junk == []
