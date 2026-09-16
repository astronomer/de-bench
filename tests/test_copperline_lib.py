"""Copperline's house library: the semantics its docstrings promise.

The library is a protected surface — tasks drive it and extend it, they never
rewrite it — so what it says it does is what a check later grades. These tests
hold it to the paragraphs.

Everything runs against a COPY of `worlds/copperline/workspace/`, because the
library resolves `include/data/` from its own file and a run that wrote there
would leave data in a tree that holds source only.
"""

import ast
import importlib
import json
import os
import re
import shutil
import sys
from pathlib import Path

import pytest

from de_bench.tasks import repo_root

WORLD_TODAY = "2026-06-15"

LIB = repo_root() / "worlds" / "copperline" / "workspace" / "include" / "lib"

#: Every module in the library, by import name.
MODULES = (
    "include.lib",
    "include.lib.calendar",
    "include.lib.warehouse",
    "include.lib.watermark",
    "include.lib.contracts",
    "include.lib.legacy_runner",
    "include.lib.notify",
    "include.lib.blueprint",
    "include.lib.blueprint.registry",
    "include.lib.blueprint.render",
)

#: The modules that define operators, and so need Airflow importable.
AIRFLOW_MODULES = (
    "include.lib.pipeline",
    "include.lib.loaders",
    "include.lib.blueprint.kinds",
    "include.lib.blueprint.kinds.csv_intake",
    "include.lib.blueprint.kinds.rollup",
    "include.lib.blueprint.kinds.dbt_select",
    "include.lib.blueprint.kinds.partition_export",
    "include.lib.blueprint.kinds.sensor_wait",
)

#: Python calls that read the wall clock. `calendar.today()` reads WORLD_TODAY
#: and is the one clock the tree has. Prose about the rule is not a breach of
#: it, so this is matched against the syntax tree rather than against the text.
WALL_CLOCK_CALLS = ("now", "utcnow", "today", "time", "time_ns")

#: The same thing said in SQL.
WALL_CLOCK_SQL = re.compile(
    r"\b(current_date|current_timestamp|current_localtime|get_current_time|now\s*\(\))",
    re.IGNORECASE,
)


@pytest.fixture(scope="module")
def world(tmp_path_factory):
    """A copy of the workspace, on `sys.path`, with the world's environment."""
    root = tmp_path_factory.mktemp("copperline")
    shutil.copytree(repo_root() / "worlds" / "copperline" / "workspace", root / "ws")
    workspace = root / "ws"
    warehouse = workspace / "include" / "data" / "copperline.duckdb"
    warehouse.parent.mkdir(parents=True, exist_ok=True)

    before = dict(os.environ)
    os.environ["WORLD_TODAY"] = WORLD_TODAY
    os.environ["DUCKDB_PATH"] = "include/data/copperline.duckdb"
    sys.path.insert(0, str(workspace))
    try:
        yield workspace
    finally:
        sys.path.remove(str(workspace))
        for name in [m for m in sys.modules if m.split(".")[0] == "include"]:
            del sys.modules[name]
        os.environ.clear()
        os.environ.update(before)


@pytest.fixture(scope="module")
def seeded(world):
    """The workspace, with the two calendars and one mart in the warehouse."""
    import duckdb

    con = duckdb.connect(str(world / "include" / "data" / "copperline.duckdb"))
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")
    con.execute("""CREATE TABLE raw.fiscal_calendar (
        cal_date DATE, fiscal_year VARCHAR, fiscal_quarter TINYINT,
        fiscal_period TINYINT, fiscal_week TINYINT, week_start DATE,
        week_end DATE, day_of_fiscal_week TINYINT, comp_date_ly DATE,
        comp_week_ly TINYINT, is_53rd_week BOOLEAN)""")
    con.execute("""INSERT INTO raw.fiscal_calendar VALUES
        ('2026-06-15','FY2026',2,5,20,'2026-06-14','2026-06-20',2,'2025-06-16',20,false)""")
    con.execute("""CREATE TABLE raw.market_calendar (
        market_code VARCHAR, calendar_date DATE, is_trading_day BOOLEAN,
        holiday_name VARCHAR, feed_expected BOOLEAN)""")
    con.execute("""INSERT INTO raw.market_calendar VALUES
        ('US','2026-06-15',true,NULL,true), ('US','2026-06-16',true,NULL,true)""")
    con.execute("CREATE SCHEMA IF NOT EXISTS marts")
    con.execute("""CREATE TABLE marts.order_economics (
        order_id VARCHAR, order_date DATE, channel VARCHAR, booked_cents BIGINT,
        net_sales_cents BIGINT, merch_margin_cents BIGINT)""")
    con.execute("INSERT INTO marts.order_economics VALUES ('o-1','2026-06-15','web',100,90,10)")
    con.execute("CREATE SCHEMA IF NOT EXISTS ops")
    con.execute("CREATE TABLE ops.daily (ds DATE, region VARCHAR, orders BIGINT)")
    con.close()
    return world


def test_every_module_imports_without_airflow(world):
    """Half the library is importable with no Airflow at all, which is what
    lets a check, a test or a review tool read a blueprint."""
    for name in MODULES:
        importlib.import_module(name)


def test_the_operator_modules_import(world):
    pytest.importorskip("airflow", reason="the operator modules need Airflow")
    for name in AIRFLOW_MODULES:
        importlib.import_module(name)


def test_today_reads_the_world_clock(seeded):
    calendar = importlib.import_module("include.lib.calendar")
    assert calendar.today().isoformat() == WORLD_TODAY


def test_today_says_so_when_the_clock_is_unset(world):
    calendar = importlib.import_module("include.lib.calendar")
    value = os.environ.pop("WORLD_TODAY")
    try:
        with pytest.raises(RuntimeError, match="WORLD_TODAY"):
            calendar.today()
    finally:
        os.environ["WORLD_TODAY"] = value


def test_calendar_reads_the_shipped_tables(seeded):
    calendar = importlib.import_module("include.lib.calendar")
    assert calendar.fiscal_year(WORLD_TODAY) == "FY2026"
    assert calendar.fiscal_week(WORLD_TODAY) == 20
    assert calendar.fiscal_period(WORLD_TODAY) == 5
    assert calendar.comp_date_ly(WORLD_TODAY).isoformat() == "2025-06-16"
    assert calendar.is_market_holiday(WORLD_TODAY, "US") is False
    assert calendar.feed_expected(WORLD_TODAY, "US") is True
    # Half open: `a` counts, `b` does not.
    assert calendar.business_days_between(WORLD_TODAY, WORLD_TODAY) == 0
    assert calendar.business_days_between(WORLD_TODAY, "2026-06-16") == 1


def test_delete_insert_round_trips(seeded):
    warehouse = importlib.import_module("include.lib.warehouse")
    rows = [{"ds": "2026-06-15", "region": "NA", "orders": 3},
            {"ds": "2026-06-15", "region": "EU", "orders": 5}]
    assert warehouse.delete_insert("ops.daily", "ds", "2026-06-15", rows) == 2
    warehouse.delete_insert("ops.daily", "ds", "2026-06-15", rows)
    warehouse.delete_insert("ops.daily", "ds", "2026-06-16",
                            [{"ds": "2026-06-16", "region": "NA", "orders": 9}])
    with warehouse.connect() as con:
        table = warehouse.qualify("ops.daily")
        assert con.execute(f"SELECT count(*) FROM {table}").fetchone()[0] == 3
        assert con.execute(
            f"SELECT sum(orders) FROM {table} WHERE ds = DATE '2026-06-15'"
        ).fetchone()[0] == 8


def test_delete_insert_refuses_rows_from_another_day(seeded):
    warehouse = importlib.import_module("include.lib.warehouse")
    with pytest.raises(ValueError, match="2026-06-14"):
        warehouse.delete_insert(
            "ops.daily", "ds", "2026-06-15",
            [{"ds": "2026-06-14", "region": "NA", "orders": 1}],
        )


def test_write_partition_lands_where_readers_look(seeded):
    warehouse = importlib.import_module("include.lib.warehouse")
    path = warehouse.write_partition("marts", "toy", "2026-06-15", ["a", "b"], [(1, 2)])
    assert path.relative_to(seeded) == Path("include/data/marts/toy_2026-06-15.csv")
    assert path.read_text().splitlines() == ["a,b", "1,2"]


def test_watermark_moves_forward_and_replays_cleanly(seeded):
    import datetime as dt

    watermark = importlib.import_module("include.lib.watermark")
    first = {"data_interval_start": dt.datetime(2026, 6, 15),
             "data_interval_end": dt.datetime(2026, 6, 16)}
    second = {"data_interval_start": dt.datetime(2026, 6, 16),
              "data_interval_end": dt.datetime(2026, 6, 17)}
    # No mark yet, so the run's own interval start is the floor.
    assert watermark.since("toy", first) == dt.datetime(2026, 6, 15)
    assert watermark.advance("toy", first) == dt.datetime(2026, 6, 16)
    assert watermark.since("toy", second) == dt.datetime(2026, 6, 16)
    # Replaying the first interval reproduces its window and leaves the second
    # run's floor where it was.
    assert watermark.since("toy", first) == dt.datetime(2026, 6, 15)
    watermark.advance("toy", first)
    assert watermark.since("toy", second) == dt.datetime(2026, 6, 16)


def test_contracts_accept_a_shipped_yml(seeded):
    contracts = importlib.import_module("include.lib.contracts")
    contract = contracts.load("order_economics")
    assert contract.model == "marts.order_economics"
    assert contract.grain == ["order_id"]
    assert "booked_cents" in contract.column_names
    assert contracts.check(contract) == []


def test_contracts_reject_a_violation(seeded):
    import duckdb

    contracts = importlib.import_module("include.lib.contracts")
    con = duckdb.connect(str(seeded / "include" / "data" / "copperline.duckdb"))
    con.execute("INSERT INTO marts.order_economics VALUES "
                "('o-1','2026-06-15','kiosk',1,NULL,NULL)")
    con.close()
    rules = {v.rule for v in contracts.check("order_economics")}
    assert rules == {"unique", "not_null", "accepted_values"}
    with pytest.raises(contracts.ContractError):
        contracts.enforce("order_economics")


def test_every_shipped_contract_loads(world):
    contracts = importlib.import_module("include.lib.contracts")
    loaded = contracts.load_all()
    assert len(loaded) >= 10
    for contract in loaded:
        assert contract.consumer and contract.owner
        for column in contract.columns:
            assert column["type"] in contracts.TYPES, contract.name


class _TaskInstance:
    """A task instance's shape, as a failure callback meets it."""

    dag_id = "sc_carrier_scan_intake"
    task_id = "land"
    try_number = 3
    run_id = "scheduled__2026-06-15"


def test_notify_takes_its_team_positionally(world):
    """The fleet calls `notify("supply", "carrier file missing")`, so team and
    summary are positional and stay that way."""
    notify = importlib.import_module("include.lib.notify")
    callback = notify.notify("supply", "carrier file missing")
    assert callable(callback)
    assert callback.__name__ == "notify_supply"
    assert callable(notify.notify("finance", "ledger tie broke",
                                  runbook="ops/runbooks/alerting.md", page=False))


def test_notify_refuses_a_team_that_owns_nothing(world):
    notify = importlib.import_module("include.lib.notify")
    with pytest.raises(ValueError, match="never a person"):
        notify.notify("s.rasmussen", "funnel late")


def test_notify_records_the_eight_fields(world):
    notify = importlib.import_module("include.lib.notify")
    entry = notify.record(
        {"task_instance": _TaskInstance(), "logical_date": "2026-06-15T00:00:00",
         "exception": ValueError("boom\nsecond line")},
        team="supply", summary="carrier file missing",
        runbook="ops/runbooks/alerting.md",
    )
    assert entry["team"] == "supply"
    assert entry["summary"] == "carrier file missing"
    assert entry["dag_id"] == "sc_carrier_scan_intake"
    assert entry["task_id"] == "land"
    assert entry["try_number"] == 3
    assert entry["run_id"] == "scheduled__2026-06-15"
    assert entry["logical_date"] == "2026-06-15T00:00:00"
    assert entry["exception"] == "boom"  # the first line, not the traceback
    line = notify.message(entry)
    assert line.startswith("carrier file missing ")
    assert "team=supply" in line and "try=3" in line


def test_notify_appends_one_line_per_failure(world):
    notify = importlib.import_module("include.lib.notify")
    callback = notify.notify("supply", "carrier file missing")
    context = {"task_instance": _TaskInstance(), "logical_date": "2026-06-15T00:00:00"}
    callback(context)
    callback(context)

    path = notify.notifications_path("sc_carrier_scan_intake")
    assert path.relative_to(world) == Path(
        "include/data/_notifications/sc_carrier_scan_intake.jsonl"
    )
    lines = path.read_text().splitlines()
    assert len(lines) == 2
    assert json.loads(lines[0])["summary"] == "carrier file missing"
    # No wall-clock stamp: a replayed interval is not a new incident.
    assert "at" not in json.loads(lines[0])


def test_notify_never_raises(world):
    """A callback that throws replaces the real failure in the log with its own."""
    notify = importlib.import_module("include.lib.notify")
    callback = notify.notify("supply", "carrier file missing")
    callback({})            # the DAG-level shape, with no task_instance
    callback(None)          # nonsense in, no raise out


def test_plan_reads_a_blueprint_without_airflow(world, tmp_path):
    blueprint = importlib.import_module("include.lib.blueprint")
    path = _toy_blueprint(tmp_path)
    plan = blueprint.plan(path)
    assert plan.dag_id == "gro_toy_daily"
    assert plan.task_ids == ["land", "roll"]
    assert plan.edges == [("land", "roll")]


def test_plan_names_a_bad_dependency(world, tmp_path):
    blueprint = importlib.import_module("include.lib.blueprint")
    path = tmp_path / "bad.dag.yaml"
    path.write_text(
        "dag_id: x\nschedule: null\nsteps:\n  a:\n    blueprint: csv_intake\n"
        "    depends_on: [nowhere]\n"
    )
    with pytest.raises(blueprint.BlueprintError, match="nowhere"):
        blueprint.plan(path)


def test_blueprint_renders_a_csv_intake_dag(world, tmp_path):
    pytest.importorskip("airflow", reason="rendering a DAG needs Airflow")
    blueprint = importlib.import_module("include.lib.blueprint")
    dag = blueprint.render_file(_toy_blueprint(tmp_path))

    assert dag.dag_id == "gro_toy_daily"
    assert sorted(task.task_id for task in dag.tasks) == ["land", "roll"]
    assert dag.get_task("roll").upstream_task_ids == {"land"}
    assert dag.catchup is False
    assert dag.max_active_runs == 1

    land = dag.get_task("land")
    assert land.table == "raw.comp_prices"
    assert land.source == "landing/comp_prices_{{ ds }}.csv"
    assert land.mode == "replace"
    assert land.partition_col == "ds"


@pytest.mark.parametrize(
    "declared,expected",
    [
        ("", {"team": "growth"}),                      # silent: the owning team
        ("notify: finance\n", {"team": "finance"}),    # named team
        ("notify: false\n", None),                     # nobody, deliberately
        ("notify:\n  summary: funnel late\n  page: false\n",
         {"team": "growth", "summary": "funnel late", "page": False}),
    ],
    ids=["default", "named", "off", "mapping"],
)
def test_a_rendered_dag_pages_its_owning_team(world, tmp_path, declared, expected):
    """A blueprint cannot carry a callable, so the factory builds one. A file
    that says nothing still pages the team that owns the directory."""
    blueprint = importlib.import_module("include.lib.blueprint")
    path = _team_blueprint(tmp_path, declared)
    assert blueprint.plan(path).notify == expected


def test_a_rendered_dag_hands_the_callback_to_every_task(world, tmp_path):
    pytest.importorskip("airflow", reason="rendering a DAG needs Airflow")
    blueprint = importlib.import_module("include.lib.blueprint")
    dag = blueprint.render_file(_team_blueprint(tmp_path, ""))
    assert dag.on_failure_callback.__name__ == "notify_growth"
    callbacks = dag.get_task("build").on_failure_callback or []
    assert [c.__name__ for c in callbacks] == ["notify_growth"]


def test_a_rendered_dag_can_page_nobody(world, tmp_path):
    pytest.importorskip("airflow", reason="rendering a DAG needs Airflow")
    blueprint = importlib.import_module("include.lib.blueprint")
    dag = blueprint.render_file(_team_blueprint(tmp_path, "notify: false\n"))
    assert dag.on_failure_callback is None
    assert not (dag.get_task("build").on_failure_callback or [])


def test_a_blueprint_outside_a_team_directory_still_renders(world, tmp_path):
    """No owning team means no notifier, and that is not an error."""
    pytest.importorskip("airflow", reason="rendering a DAG needs Airflow")
    blueprint = importlib.import_module("include.lib.blueprint")
    path = tmp_path / "loose.dag.yaml"
    path.write_text('dag_id: loose_daily\nschedule: null\nsteps:\n'
                    '  build:\n    blueprint: dbt_select\n    select: tag:x\n')
    assert blueprint.plan(path).notify is None
    assert blueprint.render_file(path).on_failure_callback is None


def test_a_team_kind_registers_through_the_documented_entry_point(world, tmp_path):
    pytest.importorskip("airflow", reason="a builder returns an operator")
    blueprint = importlib.import_module("include.lib.blueprint")
    dags = tmp_path / "projects" / "supply" / "dags"
    dags.mkdir(parents=True)
    (dags / "kinds.py").write_text(
        "from airflow.providers.standard.operators.empty import EmptyOperator\n"
        "from include.lib.blueprint import registry\n\n\n"
        "def parcel_drop(step, ctx):\n"
        "    return EmptyOperator(task_id=step.name)\n\n\n"
        'registry.register("parcel_drop", parcel_drop)\n'
    )
    (dags / "drop.dag.yaml").write_text(
        'dag_id: sc_parcel_drop_daily\nschedule: "0 9 * * *"\n'
        "steps:\n  drop:\n    blueprint: parcel_drop\n"
    )
    dag = blueprint.render_file(dags / "drop.dag.yaml")
    assert dag.dag_id == "sc_parcel_drop_daily"
    assert [task.task_id for task in dag.tasks] == ["drop"]
    assert "parcel_drop" in blueprint.registry.names()
    assert "supply" in dag.tags


def test_nothing_in_the_library_reads_the_wall_clock():
    offenders = []
    for path in sorted(LIB.rglob("*.py")):
        tree = ast.parse(path.read_text(), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                called = node.func
                name = called.attr if isinstance(called, ast.Attribute) else (
                    called.id if isinstance(called, ast.Name) else "")
                if name in WALL_CLOCK_CALLS and not _house_clock(called):
                    offenders.append(f"{path.relative_to(LIB)}:{node.lineno}: {name}()")
            if isinstance(node, ast.Constant) and isinstance(node.value, str):
                if _is_sql(node.value) and WALL_CLOCK_SQL.search(node.value):
                    offenders.append(f"{path.relative_to(LIB)}:{node.lineno}: SQL clock")
    assert not offenders, "the wall clock is not the world's clock:\n" + "\n".join(offenders)


def _house_clock(called) -> bool:
    """`calendar.today()` is the sanctioned clock, and `Path.name` style
    attribute reads are not clocks at all."""
    return isinstance(called, ast.Attribute) and isinstance(called.value, ast.Name) \
        and called.value.id in ("calendar", "cal")


def _is_sql(text: str) -> bool:
    return bool(re.search(r"\b(select|insert|update|delete|create)\b", text, re.IGNORECASE))


def test_the_library_holds_source_only():
    junk = [p for p in LIB.rglob("*") if p.suffix in (".pyc", ".duckdb", ".csv", ".json")]
    assert not junk, f"run junk in include/lib: {junk[:3]}"


#: Module level may not call any of these. The scheduler re-imports the
#: library on every parse, so a warehouse open or a file read at import costs
#: every DAG in the world.
IO_AT_IMPORT = ("open", "connect", "execute", "read_text", "read_bytes", "glob",
                "rglob", "urlopen", "run", "safe_load", "load", "get_connection",
                "Variable", "mkdir", "exists")


def test_module_level_does_no_io():
    offenders = []
    for path in sorted(LIB.rglob("*.py")):
        tree = ast.parse(path.read_text(), filename=str(path))
        for statement in tree.body:
            if isinstance(statement, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                continue
            for node in ast.walk(statement):
                if not isinstance(node, ast.Call):
                    continue
                called = node.func
                name = called.attr if isinstance(called, ast.Attribute) else (
                    called.id if isinstance(called, ast.Name) else "")
                if name in IO_AT_IMPORT:
                    offenders.append(f"{path.relative_to(LIB)}:{node.lineno}: {name}()")
    assert not offenders, "work at import time:\n" + "\n".join(offenders)


def _team_blueprint(tmp_path: Path, declared: str) -> Path:
    """A blueprint under `projects/growth/dags/`, so it has an owning team."""
    dags = tmp_path / "projects" / "growth" / "dags"
    dags.mkdir(parents=True, exist_ok=True)
    path = dags / "funnel.dag.yaml"
    path.write_text(
        'dag_id: gro_funnel_daily\nschedule: "0 8 * * *"\n' + declared +
        "steps:\n  build:\n    blueprint: dbt_select\n    select: tag:growth\n"
    )
    return path


def _toy_blueprint(tmp_path: Path) -> Path:
    path = tmp_path / "toy.dag.yaml"
    path.write_text(
        "dag_id: gro_toy_daily\n"
        'schedule: "0 7 * * *"\n'
        "description: a toy\n"
        "steps:\n"
        "  land:\n"
        "    blueprint: csv_intake\n"
        "    source: comp_prices_{{ ds }}\n"
        "    table: raw.comp_prices\n"
        "    mode: replace\n"
        "    partition_col: ds\n"
        "  roll:\n"
        "    blueprint: rollup\n"
        "    depends_on: [land]\n"
        "    source: raw.comp_prices\n"
        "    group_by: [market_id]\n"
        "    agg: median\n"
        "    column: price_cents\n"
        "    target: marts.price_index_daily\n"
    )
    return path
