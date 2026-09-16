"""The platform and finance DAGs: they parse, they are the chapter's list, and
nothing in them reads the wall clock.

Two halves, on the pattern of `test_copperline_lib.py`.

The cheap half runs everywhere: every DAG file compiles, every `*.dag.yaml`
plans, the two `dag_id` sets are exactly the inventory, and no schedule or task
body reaches for `datetime.now()`.

The expensive half needs Airflow and skips without it. It builds a DagBag over
both projects' `dags/` folders and asserts no import errors, which is the same
question `tools/check_world.py` asks of the shipped world.

Everything runs against a COPY of `worlds/copperline/workspace/`, because a DAG
that ran would write into `include/data/` and the world holds source only.
"""

import ast
import os
import py_compile
import re
import shutil
import sys
from pathlib import Path

import pytest

from de_bench.tasks import repo_root

WORLD_TODAY = "2026-06-15"

WORKSPACE = repo_root() / "worlds" / "copperline" / "workspace"
PROJECTS = ("platform", "finance")

#: `05-platform.md` §4.1. Twenty DAGs, exactly these ids.
PLATFORM_DAG_IDS = {
    "plat_bootstrap_raw",
    "plat_dbt_analytics_daily",
    "plat_dbt_test_nightly",
    "plat_dbt_source_freshness",
    "plat_contracts_enforce",
    "plat_config_sync",
    "plat_upgrade_check",
    "plat_warehouse_vacuum",
    "plat_lineage_publish",
    "plat_manifest_check",
    "plat_partition_retention",
    "plat_metrics_export",
    "plat_conn_healthcheck",
    "plat_asset_republish",
    "plat_report_registry_sync",
    "plat_backfill_broker",
    "plat_workbook_inbox",
    "plat_privacy_sweep",
    "plat_dq_profile",
    "plat_secret_rotation_check",
}

#: `05-platform.md` §4.2. Seventeen DAGs, exactly these ids.
FINANCE_DAG_IDS = {
    "fin_revenue_daily",
    "fin_close_monthly",
    "fin_ledger_tie",
    "fin_gl_export",
    "fin_daily_flash",
    "fin_board_pack_weekly",
    "fin_margin_daily",
    "fin_ar_aging",
    "fin_fx_rates_intake",
    "fin_accrual_freight",
    "fin_tax_daily",
    "fin_cash_recon_daily",
    "fin_invoice_dispute_daily",
    "fin_restatement_apply",
    "fin_budget_variance_weekly",
    "fin_report_publish",
    "fin_ledger_archive_monthly",
}

DAG_IDS = PLATFORM_DAG_IDS | FINANCE_DAG_IDS

#: The schedule each DAG is meant to carry, from the same two tables.
SCHEDULES = {
    "plat_bootstrap_raw": "0 3 * * *",
    "plat_dbt_analytics_daily": "0 4 * * *",
    "plat_dbt_test_nightly": "30 5 * * *",
    "plat_dbt_source_freshness": "15 * * * *",
    "plat_contracts_enforce": "0 6 * * *",
    "plat_config_sync": "0 4 * * *",
    "plat_upgrade_check": "0 7 * * 1",
    "plat_warehouse_vacuum": "0 8 * * 6",
    "plat_lineage_publish": "0 9 * * *",
    "plat_manifest_check": "45 3 * * *",
    "plat_partition_retention": "0 1 * * *",
    "plat_metrics_export": "0 10 * * *",
    "plat_conn_healthcheck": "0 * * * *",
    "plat_report_registry_sync": "0 11 * * *",
    "plat_workbook_inbox": "0 5 * * *",
    "plat_privacy_sweep": "0 13 * * *",
    "plat_dq_profile": "0 12 * * *",
    "plat_secret_rotation_check": "0 6 * * 1",
    "fin_revenue_daily": "0 6 * * *",
    "fin_close_monthly": "0 7 1 * *",
    "fin_ledger_tie": "0 8 * * *",
    "fin_gl_export": "0 8 * * *",
    "fin_daily_flash": "45 5 * * *",
    "fin_board_pack_weekly": "0 9 * * 1",
    "fin_margin_daily": "30 6 * * *",
    "fin_ar_aging": "0 7 * * *",
    "fin_fx_rates_intake": "0 5 * * *",
    "fin_accrual_freight": "0 9 * * *",
    "fin_tax_daily": "0 7 * * *",
    "fin_cash_recon_daily": "30 8 * * *",
    "fin_invoice_dispute_daily": "0 10 * * *",
    "fin_budget_variance_weekly": "0 10 * * 2",
    "fin_report_publish": "0 9 * * *",
    "fin_ledger_archive_monthly": "0 2 2 * *",
}

#: The two that are triggered rather than scheduled, and the one on an asset.
NO_SCHEDULE = {"plat_backfill_broker", "fin_restatement_apply"}
ASSET_SCHEDULED = {"plat_asset_republish"}

#: The one DAG that cannot parse without the committed manifest beside it.
#: Cosmos renders it from that file rather than by shelling out to dbt, which
#: is what keeps the parse fast, and it refuses to render without one. The
#: manifest is generated separately; until it lands, this DAG is left out of
#: the DagBag and the expected id set says so out loud rather than passing
#: quietly.
COSMOS_DAG = "plat_dbt_analytics_daily"
DBT_MANIFEST = "dbt/copperline_analytics/manifest/manifest.json"

#: Python calls that read the wall clock. `calendar.today()` reads WORLD_TODAY
#: and is the one clock the tree has.
WALL_CLOCK_CALLS = ("now", "utcnow", "today", "time", "time_ns")

#: The same thing said in SQL.
WALL_CLOCK_SQL = re.compile(
    r"\b(current_date|current_timestamp|current_localtime|get_current_time|now\s*\(\))",
    re.IGNORECASE,
)


def dag_files(root: Path) -> list[Path]:
    """Every DAG module under both projects. `kinds.py` is a builder registry
    and is named in each folder's `.airflowignore`; it is compiled here anyway
    because a syntax error in it breaks the rendered DAGs."""
    return sorted(
        path
        for project in PROJECTS
        for path in (root / "projects" / project).rglob("*.py")
    )


def blueprint_files(root: Path) -> list[Path]:
    return sorted(
        path
        for project in PROJECTS
        for path in (root / "projects" / project / "dags").glob("*.dag.yaml")
    )


@pytest.fixture(scope="module")
def world(tmp_path_factory):
    """A copy of the workspace, on `sys.path`, with the world's environment."""
    root = tmp_path_factory.mktemp("copperline_platfin")
    shutil.copytree(WORKSPACE, root / "ws")
    workspace = root / "ws"
    (workspace / "include" / "data").mkdir(parents=True, exist_ok=True)

    before = dict(os.environ)
    os.environ["WORLD_TODAY"] = WORLD_TODAY
    os.environ["DUCKDB_PATH"] = "include/data/copperline.duckdb"
    sys.path.insert(0, str(workspace))
    try:
        yield workspace
    finally:
        sys.path.remove(str(workspace))
        for name in [m for m in sys.modules
                     if m.split(".")[0] in ("include", "projects")]:
            del sys.modules[name]
        os.environ.clear()
        os.environ.update(before)


def test_every_dag_file_compiles(tmp_path):
    """Unconditional, and the cheapest thing that catches a broken commit.

    The byte code goes to a temporary directory. Compiling in place would put a
    `__pycache__` into a world that holds source only.
    """
    for path in dag_files(WORKSPACE):
        py_compile.compile(
            str(path), cfile=str(tmp_path / f"{path.stem}.pyc"), doraise=True
        )


def test_every_blueprint_plans(world):
    """Each `*.dag.yaml` is a well-formed plan, with no Airflow in the room."""
    import importlib

    blueprint = importlib.import_module("include.lib.blueprint")
    for path in blueprint_files(world):
        plan = blueprint.plan(path)
        assert plan.dag_id in DAG_IDS, f"{path.name} renders an unknown dag_id"
        assert plan.task_ids, f"{path.name} renders no tasks"
        assert plan.schedule, f"{path.name} names no schedule"


def test_the_two_projects_partition_the_namespace():
    """No file in one project mentions the other's prefix as its own `dag_id`,
    and every declared id appears exactly once across the tree."""
    declared: dict[str, list[str]] = {}
    for path in dag_files(WORKSPACE) + blueprint_files(WORKSPACE):
        text = path.read_text(encoding="utf-8")
        for dag_id in re.findall(r'dag_id[=:]\s*"?([a-z0-9_]+)"?', text):
            if dag_id in DAG_IDS:
                declared.setdefault(dag_id, []).append(path.name)
    duplicates = {k: v for k, v in declared.items() if len(set(v)) > 1}
    assert not duplicates, f"a dag_id is declared in two files: {duplicates}"
    assert set(declared) == DAG_IDS, (
        "declared but not in the chapter: "
        f"{sorted(set(declared) - DAG_IDS)}; "
        "in the chapter and not declared: "
        f"{sorted(DAG_IDS - set(declared))}"
    )


def test_nothing_reads_the_wall_clock():
    """`include.lib.calendar.today()` is the one sanctioned clock.

    Matched against the syntax tree rather than the text, so prose about the
    rule is not a breach of it.
    """
    offenders = []
    for path in dag_files(WORKSPACE):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                called = node.func
                name = called.attr if isinstance(called, ast.Attribute) else (
                    called.id if isinstance(called, ast.Name) else "")
                if name in WALL_CLOCK_CALLS and not _house_clock(called):
                    offenders.append(
                        f"{path.relative_to(WORKSPACE)}:{node.lineno}: {name}()"
                    )
            if isinstance(node, ast.Constant) and isinstance(node.value, str):
                if _is_sql(node.value) and WALL_CLOCK_SQL.search(node.value):
                    offenders.append(
                        f"{path.relative_to(WORKSPACE)}:{node.lineno}: SQL clock"
                    )
    assert not offenders, "the wall clock is not the world's clock:\n" + "\n".join(offenders)


def test_no_schedule_is_computed():
    """A schedule is a literal. One built from a clock would move under a
    reader, and Airflow would reparse it into a different DAG."""
    for path in dag_files(WORKSPACE):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, ast.keyword) or node.arg != "schedule":
                continue
            assert isinstance(node.value, (ast.Constant, ast.Name, ast.List)), (
                f"{path.relative_to(WORKSPACE)}:{node.lineno}: "
                "schedule is computed rather than written down"
            )


def test_the_projects_hold_source_only():
    """No run junk under either project."""
    junk = [
        path
        for project in PROJECTS
        for path in (WORKSPACE / "projects" / project).rglob("*")
        if path.suffix in (".pyc", ".duckdb", ".csv", ".json", ".log")
    ]
    assert not junk, f"run junk under projects/: {junk[:3]}"


# --- the half that needs Airflow -------------------------------------------

@pytest.fixture(scope="module")
def expected_ids(world):
    """The ids the DagBag should hold, given what is on disk beside it.

    Every id, once the committed manifest exists. Until it does, the
    cosmos-rendered DAG is ignored for the duration of this module and left out
    of the set: it renders from that file and refuses to parse without it, so
    including it would be a failure about a missing artifact rather than about
    a DAG.
    """
    if (world / DBT_MANIFEST).exists():
        return DAG_IDS
    ignore = world / "projects" / "platform" / "dags" / ".airflowignore"
    ignore.write_text(ignore.read_text(encoding="utf-8") + f"{COSMOS_DAG}.py\n",
                      encoding="utf-8")
    return DAG_IDS - {COSMOS_DAG}


@pytest.fixture(scope="module")
def dagbag(world, expected_ids):
    """A DagBag over both projects' `dags/`, or a skip when Airflow is absent."""
    pytest.importorskip("airflow", reason="a DagBag needs Airflow")
    from airflow.dag_processing.dagbag import DagBag

    bags = []
    for project in PROJECTS:
        folder = world / "projects" / project / "dags"
        bags.append(DagBag(dag_folder=str(folder), safe_mode=True,
                           bundle_path=folder))
    return bags


def test_the_dagbag_has_no_import_errors(dagbag):
    errors = {}
    for bag in dagbag:
        errors.update(bag.import_errors)
    assert not errors, "DAG import errors:\n" + "\n".join(
        f"{path}: {message.splitlines()[-1] if message else message}"
        for path, message in sorted(errors.items())
    )


def test_the_dag_ids_are_the_chapters_list(dagbag, expected_ids):
    found = set()
    for bag in dagbag:
        found |= set(bag.dags)
    assert found == expected_ids, (
        f"missing: {sorted(expected_ids - found)}; "
        f"unexpected: {sorted(found - expected_ids)}"
    )


def test_every_dag_carries_the_house_settings(dagbag):
    """`catchup=False`, one active run, a team owner, tags, and documentation."""
    problems = []
    for bag in dagbag:
        for dag_id, dag in sorted(bag.dags.items()):
            if dag.catchup:
                problems.append(f"{dag_id}: catchup is on")
            if dag.max_active_runs != 1:
                problems.append(f"{dag_id}: max_active_runs is {dag.max_active_runs}")
            if not dag.tags:
                problems.append(f"{dag_id}: no tags")
            if not (dag.doc_md or dag.description):
                problems.append(f"{dag_id}: no doc_md and no description")
    assert not problems, "\n".join(problems)


def test_every_schedule_is_the_one_the_chapter_gives(dagbag, expected_ids):
    found = {}
    for bag in dagbag:
        found.update({dag_id: dag for dag_id, dag in bag.dags.items()})

    problems = []
    for dag_id, expected in SCHEDULES.items():
        dag = found.get(dag_id)
        if dag is None and dag_id == COSMOS_DAG:
            continue
        if dag is None:
            problems.append(f"{dag_id}: not in the bag")
            continue
        actual = getattr(dag.timetable, "summary", None) or str(dag.schedule)
        if expected not in str(actual):
            problems.append(f"{dag_id}: {actual!r}, and the chapter says {expected!r}")
    for dag_id in NO_SCHEDULE:
        dag = found.get(dag_id)
        summary = str(getattr(dag.timetable, "summary", "")) if dag else ""
        if dag is not None and summary not in ("", "None", "Never, external triggers only"):
            problems.append(f"{dag_id}: {summary!r}, and it is triggered by hand")
    assert not problems, "\n".join(problems)


def test_the_asset_scheduled_dag_takes_no_clock(dagbag):
    """`plat_asset_republish` is triggered by an event, so its runs may carry
    no logical date at all. Nothing in it may template `{{ ds }}`."""
    path = WORKSPACE / "projects" / "platform" / "dags" / "plat_asset_republish.py"
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    # The module docstring says why `{{ ds }}` is absent, so it is not part of
    # the search. Everything else is.
    templated = [
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant) and isinstance(node.value, str)
        and node.value != ast.get_docstring(tree, clean=False)
        and "{{ ds }}" in node.value
    ]
    assert not templated, (
        "an asset-triggered run has no data interval, so `{{ ds }}` is not a day: "
        f"{templated}"
    )
    found = {}
    for bag in dagbag:
        found.update(bag.dags)
    dag = found["plat_asset_republish"]
    assert "Asset" in type(dag.timetable).__name__, (
        f"{type(dag.timetable).__name__} is a clock, and this DAG runs on events"
    )


def _house_clock(called) -> bool:
    """`calendar.today()` is the sanctioned clock, and an attribute read such
    as `Path.name` is not a clock at all."""
    return isinstance(called, ast.Attribute) and isinstance(called.value, ast.Name) \
        and called.value.id in ("calendar", "cal")


def _is_sql(text: str) -> bool:
    return bool(re.search(r"\b(select|insert|update|delete|create)\b", text,
                          re.IGNORECASE))
