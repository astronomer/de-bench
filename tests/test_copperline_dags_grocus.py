"""The growth and customer DAGs: they compile, they parse, and they are these.

Two teams share one `dag_id` namespace with four others, so the list below is
the contract: a duplicate anywhere in the world records an import error and
every `dag_parses` check in every task fails at once. The list is written out
rather than counted so that a rename shows up as a diff.

Everything runs against a COPY of `worlds/copperline/workspace/`, because
building a DagBag writes nothing but rendering a blueprint resolves paths
from the tree, and the shipped tree holds source only.

`py_compile` runs unconditionally. The DagBag half needs Airflow and skips
without it.
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
TEAMS = ("growth", "customer")

#: Every DAG these two projects ship, and how many tasks each one holds.
EXPECTED = {
    "growth": {
        "gro_alerting_daily": 5,
        "gro_attribution_daily": 8,
        "gro_audience_export": 4,
        "gro_channel_roi_daily": 6,
        "gro_clickstream_intake": 6,
        "gro_comp_prices_intake": 5,
        "gro_email_engagement_daily": 4,
        "gro_event_replay_repair": 4,
        "gro_experiment_readout_daily": 6,
        "gro_funnel_daily": 5,
        "gro_market_seed_build": 4,
        "gro_marketing_spend_intake": 5,
        "gro_price_index_daily": 5,
        "gro_pricing_mart_daily": 6,
        "gro_reverse_etl_ads": 5,
        "gro_reverse_etl_crm": 5,
        "gro_seo_rank_intake": 4,
        "gro_sessionize_daily": 7,
    },
    "customer": {
        "cus_360_publish": 4,
        "cus_address_normalize_daily": 4,
        "cus_churn_scores_weekly": 6,
        "cus_consent_sync_daily": 4,
        "cus_crosswalk_apply_daily": 5,
        "cus_customer_360_daily": 9,
        "cus_customer_intake": 5,
        "cus_dim_customer_daily": 6,
        "cus_features_daily": 6,
        "cus_loyalty_daily": 5,
        "cus_merge_candidates_review": 5,
        "cus_northwave_accounts_intake": 4,
        "cus_northwave_backfill": 4,
        "cus_nps_weekly": 3,
        "cus_support_sla_daily": 5,
        "cus_support_tickets_intake": 5,
    },
}

#: The DAGs each team renders from YAML rather than writes in Python.
RENDERED = {
    "growth": ("gro_audience_export", "gro_comp_prices_intake",
               "gro_email_engagement_daily", "gro_market_seed_build",
               "gro_price_index_daily", "gro_seo_rank_intake"),
    "customer": ("cus_loyalty_daily", "cus_nps_weekly",
                 "cus_support_sla_daily"),
}

#: Python calls that read the wall clock. A run is told which dates it owns,
#: by its interval or by `calendar.today()`, so none of these belongs here.
WALL_CLOCK_CALLS = ("now", "utcnow", "today", "time", "time_ns")

#: The same thing said in SQL.
WALL_CLOCK_SQL = re.compile(
    r"\b(current_date|current_timestamp|current_localtime|get_current_time|now\s*\(\))",
    re.IGNORECASE,
)

#: Module level may not call any of these. The scheduler re-imports every DAG
#: file on each parse, so a warehouse open or a file read at import costs the
#: whole world.
IO_AT_IMPORT = ("connect", "execute", "read_text", "read_bytes", "glob",
                "rglob", "urlopen", "safe_load", "get_connection", "Variable")

#: Airflow 2 spellings. A DAG showing one of these does not import on 3.x.
DEAD_IMPORTS = ("airflow.models", "airflow.decorators", "airflow.datasets",
                "airflow.operators", "schedule_interval", "execution_date")


def project_files(suffix: str) -> list[Path]:
    return sorted(path for team in TEAMS
                  for path in (WORKSPACE / "projects" / team).rglob(f"*{suffix}"))


@pytest.fixture(scope="module")
def world(tmp_path_factory):
    """A copy of the workspace, on `sys.path`, with the world's environment."""
    root = tmp_path_factory.mktemp("copperline_grocus")
    shutil.copytree(WORKSPACE, root / "ws")
    workspace = root / "ws"
    (workspace / "include" / "data").mkdir(parents=True, exist_ok=True)

    before = dict(os.environ)
    os.environ["WORLD_TODAY"] = WORLD_TODAY
    os.environ["DUCKDB_PATH"] = "include/data/copperline.duckdb"
    os.environ["AIRFLOW_HOME"] = str(root / "afhome")
    os.environ["AIRFLOW__CORE__UNIT_TEST_MODE"] = "True"
    os.environ["PYTHONDONTWRITEBYTECODE"] = "1"
    sys.path.insert(0, str(workspace))
    try:
        yield workspace
    finally:
        sys.path.remove(str(workspace))
        for name in list(sys.modules):
            if name.split(".")[0] in ("include", "projects"):
                del sys.modules[name]
        os.environ.clear()
        os.environ.update(before)


@pytest.fixture(scope="module")
def bags(world):
    """One DagBag per team, built over that team's `dags/` folder."""
    pytest.importorskip("airflow", reason="a DagBag needs Airflow")
    from airflow.dag_processing.dagbag import DagBag

    return {team: DagBag(dag_folder=str(world / "projects" / team / "dags"),
                         safe_mode=True, bundle_path=world)
            for team in TEAMS}


def test_every_file_compiles(tmp_path):
    """No Airflow, no warehouse: the files are syntactically Python.

    The bytecode goes to a temporary directory, because `workspace/` holds
    source only and a stray `__pycache__` fails the tree's own check.
    """
    for number, path in enumerate(project_files(".py")):
        py_compile.compile(str(path), doraise=True,
                           cfile=str(tmp_path / f"{number}.pyc"))


def test_no_dag_file_shows_an_airflow_2_spelling():
    """`schedule_interval` and the `airflow.models` import path do not exist
    on 3.x, so a DAG carrying one is broken rather than old."""
    offenders = []
    for path in project_files(".py"):
        text = path.read_text(encoding="utf-8")
        for dead in DEAD_IMPORTS:
            if re.search(rf"\b{re.escape(dead)}\b", text):
                offenders.append(f"{path.name}: {dead}")
    assert not offenders, "Airflow 2 spellings:\n" + "\n".join(offenders)


def test_nothing_reads_the_wall_clock():
    """A rerun in August must still report the June it was asked for."""
    offenders = []
    for path in project_files(".py"):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                called = node.func
                name = called.attr if isinstance(called, ast.Attribute) else (
                    called.id if isinstance(called, ast.Name) else "")
                if name in WALL_CLOCK_CALLS and not _house_clock(called):
                    offenders.append(f"{path.name}:{node.lineno}: {name}()")
            if isinstance(node, ast.Constant) and isinstance(node.value, str):
                if _is_sql(node.value) and WALL_CLOCK_SQL.search(node.value):
                    offenders.append(f"{path.name}:{node.lineno}: SQL clock")
    assert not offenders, "the wall clock is not the world's clock:\n" + \
        "\n".join(offenders)


def test_module_level_does_no_io():
    """The scheduler re-imports every DAG file on each parse."""
    offenders = []
    for path in project_files(".py"):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for statement in tree.body:
            if isinstance(statement, (ast.FunctionDef, ast.AsyncFunctionDef,
                                      ast.ClassDef)):
                continue
            for node in ast.walk(statement):
                if not isinstance(node, ast.Call):
                    continue
                called = node.func
                name = called.attr if isinstance(called, ast.Attribute) else (
                    called.id if isinstance(called, ast.Name) else "")
                if name in IO_AT_IMPORT:
                    offenders.append(f"{path.name}:{node.lineno}: {name}()")
    assert not offenders, "work at import time:\n" + "\n".join(offenders)


def test_the_projects_hold_source_only():
    junk = [p for p in (WORKSPACE / "projects").rglob("*")
            if p.suffix in (".pyc", ".duckdb", ".log") or p.name == "__pycache__"]
    assert not junk, f"run junk under projects/: {junk[:3]}"


def test_a_dag_file_is_named_after_its_dag_id():
    """A `dag_id` is a contract, and the file it lives in is how it is found."""
    for team, expected in EXPECTED.items():
        folder = WORKSPACE / "projects" / team / "dags"
        for dag_id in expected:
            python = folder / f"{dag_id}.py"
            blueprint = folder / f"{dag_id}.dag.yaml"
            assert python.exists() or blueprint.exists(), \
                f"{dag_id} has no file named after it"
            assert not (python.exists() and blueprint.exists()), \
                f"{dag_id} is both hand-written and rendered"


def test_every_blueprint_plans(world):
    """`plan()` needs no Airflow, so a check or a review tool can read one."""
    import importlib

    blueprint = importlib.import_module("include.lib.blueprint")
    for team, rendered in RENDERED.items():
        folder = world / "projects" / team / "dags"
        planned = {}
        for path in sorted(folder.glob("*.dag.yaml")):
            plan = blueprint.plan(path)
            planned[plan.dag_id] = plan
            assert plan.task_ids, f"{plan.dag_id} plans no steps"
            for upstream, downstream in plan.edges:
                assert upstream in plan.task_ids
                assert downstream in plan.task_ids
        assert sorted(planned) == sorted(rendered), team


def test_the_dag_bags_import_cleanly(bags):
    for team, bag in bags.items():
        assert not bag.import_errors, \
            f"{team} import errors:\n" + "\n".join(
                f"{path}: {error}" for path, error in bag.import_errors.items())


def test_the_dag_ids_are_exactly_these(bags):
    """The world shares one namespace across six teams; one duplicate breaks
    every task in it."""
    for team, bag in bags.items():
        assert sorted(bag.dags) == sorted(EXPECTED[team]), team


def test_the_task_counts_are_the_ones_the_graph_says(bags):
    for team, bag in bags.items():
        counted = {dag_id: len(dag.tasks) for dag_id, dag in bag.dags.items()}
        assert counted == EXPECTED[team], team


def test_a_rendered_dag_round_trips_through_plan(world, bags):
    """What `plan()` says a blueprint is, is what the DagBag built."""
    import importlib

    blueprint = importlib.import_module("include.lib.blueprint")
    for team, rendered in RENDERED.items():
        folder = world / "projects" / team / "dags"
        for dag_id in rendered:
            plan = blueprint.plan(folder / f"{dag_id}.dag.yaml")
            dag = bags[team].dags[dag_id]
            assert sorted(task.task_id for task in dag.tasks) == \
                sorted(plan.task_ids), dag_id
            for upstream, downstream in plan.edges:
                assert upstream in dag.get_task(downstream).upstream_task_ids, \
                    f"{dag_id}: {upstream} -> {downstream}"
            assert dag.catchup is False
            assert dag.max_active_runs == 1


def test_every_dag_says_what_it_is_and_who_owns_it(bags):
    """Every DAG carries docs and its team's tag."""
    for team, bag in bags.items():
        for dag_id, dag in bag.dags.items():
            assert dag.doc_md, f"{dag_id} has no doc_md"
            assert team in dag.tags, f"{dag_id} is not tagged {team}"
            assert dag.default_args.get("owner") or dag.owner, \
                f"{dag_id} names no owner"


def test_every_task_pages_somebody_when_it_fails(bags):
    """A failure with no notifier is a failure nobody hears about.

    The callback sits in `default_args` for the hand-written DAGs and is
    attached after rendering for the blueprint ones, because a blueprint's
    `default_args` comes out of YAML and cannot carry a Python object.
    """
    offenders = []
    for bag in bags.values():
        for dag_id, dag in bag.dags.items():
            for task in dag.tasks:
                if not task.on_failure_callback:
                    offenders.append(f"{dag_id}.{task.task_id}")
    assert not offenders, "tasks that page nobody: " + ", ".join(offenders)


def test_every_sensor_sets_a_timeout(bags):
    """Airflow's default is seven days, which is not a timeout."""
    offenders = []
    for bag in bags.values():
        for dag_id, dag in bag.dags.items():
            for task in dag.tasks:
                if "Sensor" in type(task).__name__ and not getattr(
                        task, "timeout", None):
                    offenders.append(f"{dag_id}.{task.task_id}")
    assert not offenders, "sensors with no timeout: " + ", ".join(offenders)


def test_every_dag_id_carries_its_team_prefix(bags):
    prefixes = {"growth": "gro_", "customer": "cus_"}
    for team, bag in bags.items():
        for dag_id in bag.dags:
            assert dag_id.startswith(prefixes[team]), dag_id


def _house_clock(called) -> bool:
    """`calendar.today()` is the sanctioned clock, and an attribute read on a
    path or a date is not a clock at all."""
    return isinstance(called, ast.Attribute) and isinstance(called.value, ast.Name) \
        and called.value.id in ("calendar", "cal", "retail_calendar")


def _is_sql(text: str) -> bool:
    return bool(re.search(r"\b(select|insert|update|delete|create)\b", text,
                          re.IGNORECASE))
