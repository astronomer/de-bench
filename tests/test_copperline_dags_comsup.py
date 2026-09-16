"""Copperline's commerce and supply-chain DAGs.

Forty-four DAGs across two team projects. This holds them to the three things
that are expensive to get wrong:

1. Every file compiles. No Airflow needed, so this runs everywhere.
2. With Airflow present, a DagBag over both projects has no import errors and
   holds exactly the dag_ids the platform chapter lists. A duplicate or a
   missing one breaks the world, not one DAG.
3. Every `.dag.yaml` round-trips through the factory's `plan()`, which needs no
   Airflow — so a broken blueprint is caught by a check that a review tool can
   run.

Everything runs against a COPY of `worlds/copperline/workspace/`, because the
library resolves `include/data/` from its own file and a run that wrote there
would leave data in a tree that holds source only.
"""

import ast
import os
import py_compile
import shutil
import sys
from pathlib import Path

import pytest

from de_bench.tasks import repo_root

WORLD_TODAY = "2026-06-15"

WORKSPACE = repo_root() / "worlds" / "copperline" / "workspace"
PROJECTS = WORKSPACE / "projects"

#: The commerce inventory, exactly as `docs/copperline-spec/05-platform.md`
#: section 4.3 spells it. Commerce never prefixes a dag_id.
COMMERCE_DAGS = {
    "orders_intake": 6,
    "order_lines_daily": 5,
    "orders_enrich_daily": 8,
    "nightly_close": 34,
    "payments_intake": 5,
    "psp_bridge_build": 4,
    "payments_recon_daily": 9,
    "payment_settlement_weekly": 7,
    "channel_daily_intake": 11,
    "order_economics_daily": 6,
    "cart_abandon_daily": 4,
    "product_snapshot_daily": 4,
    "price_change_intake": 4,
    "returns_daily": 5,
    "refunds_daily": 4,
    "marketplace_orders_na": 5,
    "marketplace_orders_eu": 5,
    "marketplace_settlement_intake": 6,
    "store_pos_intake": 6,
    "store_pos_late_catchup": 3,
    "fct_payments_restore": 4,
    "commerce_export_partner": 5,
}

#: The supply-chain inventory, section 4.4. Prefix `sc_`, cadence last.
SUPPLY_DAGS = {
    "sc_inventory_snapshot_daily": 6,
    "sc_inventory_movements_hourly": 5,
    "sc_carrier_scan_intake": 5,
    "sc_carrier_invoice_intake": 4,
    # 8 since the second platform batch added ensure_tables: the shipped DAG
    # could not run on a fresh warehouse (no marts DDL anywhere).
    "sc_shipping_cost_daily": 8,
    "sc_rate_card_intake": 3,
    "sc_fill_rate_daily": 5,
    "sc_replenishment_daily": 9,
    "sc_shrink_weekly": 4,
    "sc_dc_transfer_daily": 5,
    "sc_lane_performance_daily": 5,
    "sc_supplier_scorecard_weekly": 6,
    "sc_freight_accrual_workbook": 5,
    "sc_partner_share_kestrel": 5,
    "sc_carrier_api_poll": 5,
    "sc_pdi_nightly_load": 8,
    "sc_pdi_history_complete": 2,
    "sc_ssis_supplier_master": 7,
    "sc_autosys_bridge": 4,
    "sc_tidal_inventory_report": 3,
    "sc_dc_capacity_daily": 4,
    "sc_backorder_daily": 5,
}

ALL_DAGS = {**COMMERCE_DAGS, **SUPPLY_DAGS}

#: The two files in a `dags/` folder that are not a DAG.
NOT_A_DAG = ("blueprints.py", "kinds.py")

#: Import paths Airflow 3 deprecated. A DAG that shows one is a 2.x DAG.
DEAD_IMPORTS = ("airflow.models", "airflow.decorators", "airflow.datasets",
                "airflow.operators")

#: Python calls that read the wall clock. `calendar.today()` reads WORLD_TODAY
#: and is the one clock the tree has.
WALL_CLOCK_CALLS = ("now", "utcnow", "today", "time_ns")


def team_files(team: str, suffix: str) -> list[Path]:
    return sorted((PROJECTS / team / "dags").glob(suffix))


def all_python() -> list[Path]:
    return sorted(p for team in ("commerce", "supply")
                  for p in (PROJECTS / team).rglob("*.py"))


def all_blueprints() -> list[Path]:
    return sorted(p for team in ("commerce", "supply")
                  for p in team_files(team, "*.dag.yaml"))


@pytest.fixture(scope="module")
def world(tmp_path_factory):
    """A copy of the workspace, on `sys.path`, with the world's environment."""
    root = tmp_path_factory.mktemp("copperline_dags")
    shutil.copytree(WORKSPACE, root / "ws")
    workspace = root / "ws"
    (workspace / "include" / "data").mkdir(parents=True, exist_ok=True)

    before = dict(os.environ)
    os.environ["WORLD_TODAY"] = WORLD_TODAY
    os.environ["DUCKDB_PATH"] = "include/data/copperline.duckdb"
    os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"] = "False"
    os.environ.setdefault("AIRFLOW_HOME", str(root / "afhome"))
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


@pytest.fixture(scope="module")
def dagbag(world):
    """A DagBag over both team projects, or a skip when Airflow is absent."""
    pytest.importorskip("airflow", reason="a DagBag needs Airflow")
    from airflow.dag_processing.dagbag import DagBag

    bag = DagBag(dag_folder=None, collect_dags=False)
    for team in ("commerce", "supply"):
        bag.collect_dags(dag_folder=str(world / "projects" / team / "dags"),
                         safe_mode=False)
    return bag


# --- what holds without Airflow --------------------------------------------

def test_every_file_compiles(tmp_path):
    """Every Python file in both projects, compiled. Unconditional.

    The bytecode goes to a scratch directory, because a world holds source only
    and a test that leaves a `__pycache__` in it breaks the next one.
    """
    for path in all_python():
        py_compile.compile(str(path), doraise=True,
                           cfile=str(tmp_path / f"{path.stem}.pyc"))


def test_one_file_one_dag():
    """A DAG file is named for the dag_id it holds, and every id has a file.

    A rendered DAG's file is its `.dag.yaml`; the loader and the team kinds are
    the two files in `dags/` that hold no DAG.
    """
    for team, wanted in (("commerce", COMMERCE_DAGS), ("supply", SUPPLY_DAGS)):
        python = {p.stem for p in team_files(team, "*.py")
                  if p.name not in NOT_A_DAG}
        rendered = {p.name[: -len(".dag.yaml")] for p in team_files(team, "*.dag.yaml")}
        assert python & rendered == set(), f"{team}: a DAG written twice"
        assert python | rendered == set(wanted), f"{team}: the file set is wrong"


def test_the_two_namespaces_do_not_collide():
    """One duplicate dag_id records an import error and fails every task in the
    world, so the two teams' lists are checked against each other by name."""
    assert set(COMMERCE_DAGS) & set(SUPPLY_DAGS) == set()
    assert len(ALL_DAGS) == 44


def test_commerce_never_prefixes_and_supply_always_does():
    """The one contradiction between the two `CONVENTIONS.md` files, held to."""
    assert not [d for d in COMMERCE_DAGS if d.startswith(("com_", "commerce_dag"))]
    assert all(d.startswith("sc_") for d in SUPPLY_DAGS)


def test_no_airflow_2_imports():
    for path in all_python():
        text = path.read_text(encoding="utf-8")
        for dead in DEAD_IMPORTS:
            assert f"from {dead} import" not in text, f"{path.name} imports {dead}"
        assert "schedule_interval" not in text, f"{path.name} uses schedule_interval"
        assert "execution_date" not in text, f"{path.name} uses execution_date"


def test_nothing_reads_the_wall_clock():
    """A run is told which dates it owns. `calendar.today()` is the one clock."""
    offenders = []
    for path in all_python():
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            called = node.func
            name = called.attr if isinstance(called, ast.Attribute) else (
                called.id if isinstance(called, ast.Name) else "")
            if name in WALL_CLOCK_CALLS and not _house_clock(called):
                offenders.append(f"{path.name}:{node.lineno}: {name}()")
    assert not offenders, "the wall clock is not the world's clock:\n" + "\n".join(offenders)


def test_module_level_does_no_io():
    """The parser re-imports every DAG file on every parse.

    A call inside a function body is fine however deeply the function is nested,
    so the walk stops at every `def` rather than at the top level only — a
    TaskFlow DAG defines its tasks inside a `with DAG(...)` block, which is
    module level and full of function bodies.
    """
    banned = ("read_text", "read_bytes", "safe_load", "connect", "execute",
              "glob", "rglob", "urlopen", "get_connection")
    offenders = []
    for path in all_python():
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in _module_level_calls(tree):
            called = node.func
            name = called.attr if isinstance(called, ast.Attribute) else (
                called.id if isinstance(called, ast.Name) else "")
            if name in banned:
                offenders.append(f"{path.name}:{node.lineno}: {name}()")
    assert not offenders, "work at import time:\n" + "\n".join(offenders)


def test_the_projects_hold_source_only():
    junk = [p for team in ("commerce", "supply")
            for p in (PROJECTS / team).rglob("*")
            if p.suffix in (".pyc", ".duckdb", ".log")
            or p.name == "__pycache__"]
    assert not junk, f"run junk in a team project: {junk[:3]}"


def test_conventions_hold_no_grading_criterion():
    """A `CONVENTIONS.md` holds style. Nothing a check grades goes in one."""
    forbidden = ("append", "none_failed", "next_page_token", "nwv",
                 "unqualified", "xcom_pull", "timeout of seven days")
    for team in ("commerce", "supply"):
        text = (PROJECTS / team / "CONVENTIONS.md").read_text(encoding="utf-8").lower()
        for word in forbidden:
            assert word not in text, f"{team}/CONVENTIONS.md mentions {word!r}"


# --- the blueprint files, without Airflow ----------------------------------

def test_every_blueprint_plans(world):
    """Each `.dag.yaml` round-trips through `plan()`, which needs no Airflow.

    `plan()` is what a check or a review tool asks, so a blueprint that only
    works when Airflow is importable is a blueprint nothing can review.
    """
    import importlib

    blueprint = importlib.import_module("include.lib.blueprint")
    planned = {}
    for path in all_blueprints():
        result = blueprint.plan(path)
        assert result.dag_id in ALL_DAGS, f"{path.name} renders an unlisted dag_id"
        assert result.dag_id == path.name[: -len(".dag.yaml")]
        assert result.schedule, f"{result.dag_id} names no schedule"
        assert result.steps, f"{result.dag_id} has no steps"
        assert len(result.task_ids) == ALL_DAGS[result.dag_id]
        planned[result.dag_id] = result

    assert len(planned) == 10, "five rendered DAGs per team"
    for result in planned.values():
        names = set(result.task_ids)
        for upstream, downstream in result.edges:
            assert upstream in names and downstream in names


def test_blueprint_kinds_are_registered_or_shipped(world):
    """Every kind a blueprint names is either one of the five or the team's own."""
    import importlib

    blueprint = importlib.import_module("include.lib.blueprint")
    shipped = {"csv_intake", "rollup", "dbt_select", "partition_export",
               "sensor_wait"}
    team_local = {"commerce": {"sql_file"}, "supply": {"sftp_drop"}}
    for team, own in team_local.items():
        for path in team_files(team, "*.dag.yaml"):
            for step in blueprint.plan(path).steps:
                assert step.blueprint in shipped | own, (
                    f"{path.name}: {step.blueprint} is registered by nobody"
                )


# --- what needs Airflow ----------------------------------------------------

def test_the_dagbag_has_no_import_errors(dagbag):
    assert dagbag.import_errors == {}


def test_the_dagbag_holds_exactly_the_chapter_list(dagbag):
    assert set(dagbag.dags) == set(ALL_DAGS)


def test_every_dag_has_the_task_count_the_chapter_gives(dagbag):
    counts = {dag_id: len(dag.tasks) for dag_id, dag in dagbag.dags.items()}
    assert counts == ALL_DAGS


def test_every_dag_carries_its_header(dagbag):
    """Schedule, start date, catchup, tags, docs and a failure callback."""
    for dag_id, dag in dagbag.dags.items():
        assert dag.doc_md, f"{dag_id} has no doc_md"
        assert dag.tags, f"{dag_id} has no tags"
        assert dag.start_date is not None, f"{dag_id} has no start_date"
        assert dag.catchup is False, f"{dag_id} leaves catchup on"
        assert dag.max_active_runs == 1, f"{dag_id} lets two runs race"
        team = "supply" if dag_id.startswith("sc_") else "commerce"
        assert team in dag.tags, f"{dag_id} does not name its team"


def test_every_scheduled_dag_says_when(dagbag):
    """`schedule` is written out, `None` included, and only the two replay DAGs
    are unscheduled."""
    unscheduled = {d for d, dag in dagbag.dags.items() if dag.schedule is None}
    assert unscheduled == {"fct_payments_restore"}


def test_every_sensor_sets_a_timeout(dagbag):
    """Airflow's own default is seven days, which is not a timeout."""
    from airflow.sdk.bases.sensor import BaseSensorOperator

    leaks = [
        f"{dag_id}.{task.task_id}"
        for dag_id, dag in dagbag.dags.items()
        for task in dag.tasks
        if isinstance(task, BaseSensorOperator) and task.timeout >= 60 * 60 * 24 * 7
    ]
    assert not leaks, f"sensors with no timeout: {leaks}"


def test_no_join_after_a_branch_uses_none_failed(dagbag):
    """`none_failed` is satisfied when every parent skipped, so a join under it
    runs on an empty result and says nothing."""
    from airflow.providers.standard.operators.python import (
        BranchPythonOperator,
        ShortCircuitOperator,
    )

    offenders = []
    for dag_id, dag in dagbag.dags.items():
        branches = {t.task_id for t in dag.tasks
                    if isinstance(t, (BranchPythonOperator, ShortCircuitOperator))}
        if not branches:
            continue
        for task in dag.tasks:
            if task.trigger_rule != "none_failed":
                continue
            if _reaches(dag, branches, task.task_id):
                offenders.append(f"{dag_id}.{task.task_id}")
    assert not offenders, f"none_failed joins after a branch: {offenders}"


def test_depends_on_past_only_where_the_team_says(dagbag):
    """Supply sets it on the DAGs that map over locations and nowhere else."""
    on = {dag_id for dag_id, dag in dagbag.dags.items()
          if any(t.depends_on_past for t in dag.tasks)}
    assert on == {"sc_replenishment_daily", "sc_dc_transfer_daily"}


def test_retries_are_written_out_and_modest(dagbag):
    """Two is the house default. Anything over three is a task with a comment,
    and there are none of those."""
    loud = [f"{dag_id}.{task.task_id}"
            for dag_id, dag in dagbag.dags.items()
            for task in dag.tasks if task.retries > 3]
    assert not loud, f"retries masking a failure: {loud}"


def test_the_two_marketplace_siblings_are_siblings(dagbag):
    """A third region is a copy job, so the two must not have drifted."""
    na, eu = dagbag.dags["marketplace_orders_na"], dagbag.dags["marketplace_orders_eu"]
    assert [t.task_id for t in na.tasks] == [t.task_id for t in eu.tasks]
    assert na.schedule == eu.schedule
    assert na.default_args == eu.default_args
    for task_id in (t.task_id for t in na.tasks):
        assert type(na.get_task(task_id)) is type(eu.get_task(task_id))


def _module_level_calls(tree: ast.AST):
    """Every call the interpreter makes at import, with function bodies pruned."""
    stack = list(getattr(tree, "body", []))
    while stack:
        node = stack.pop()
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef,
                             ast.Lambda)):
            continue
        if isinstance(node, ast.Call):
            yield node
        stack.extend(ast.iter_child_nodes(node))


def _house_clock(called) -> bool:
    """`calendar.today()` is the sanctioned clock, and an attribute read on a
    path or a datetime is not a clock at all."""
    return isinstance(called, ast.Attribute) and isinstance(called.value, ast.Name) \
        and called.value.id in ("calendar", "cal")


def _reaches(dag, sources: set, target: str) -> bool:
    """Whether any task in `sources` is upstream of `target`."""
    seen, frontier = set(), [target]
    while frontier:
        current = frontier.pop()
        if current in seen:
            continue
        seen.add(current)
        frontier.extend(dag.get_task(current).upstream_task_ids)
    return bool(seen & sources)
