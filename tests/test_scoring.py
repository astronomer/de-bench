"""Checks that could silently pass while measuring nothing."""

import os
import subprocess

from de_bench.scoring import _compare, changed_paths, dbt_watch, do_not_modify_check
from de_bench.tasks import repo_root

PROJECT = repo_root()


def test_dbt_watch_catches_an_invocation(tmp_path, monkeypatch):
    """The negative control for no_dbt_at_parse: a shim that never fires would
    make every parse-time check pass, which is worse than not having one."""
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    (fake_bin / "dbt").write_text("#!/bin/sh\nexit 0\n")
    os.chmod(fake_bin / "dbt", 0o755)

    env = {"PATH": f"{fake_bin}{os.pathsep}{os.environ['PATH']}"}
    watched, marker = dbt_watch(env)
    assert watched is not None

    subprocess.run(["sh", "-c", "dbt ls --quiet"], env=watched, check=False, capture_output=True)
    assert os.path.exists(marker), "shim did not record the call"
    assert "ls" in open(marker).read()


def test_dbt_watch_stays_quiet_when_nothing_calls_dbt(tmp_path):
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    (fake_bin / "dbt").write_text("#!/bin/sh\nexit 0\n")
    os.chmod(fake_bin / "dbt", 0o755)

    env = {"PATH": f"{fake_bin}{os.pathsep}{os.environ['PATH']}"}
    watched, marker = dbt_watch(env)
    subprocess.run(["sh", "-c", "true"], env=watched, check=False, capture_output=True)
    assert not os.path.exists(marker)


def test_dbt_watch_reports_when_there_is_no_dbt():
    watched, _ = dbt_watch({"PATH": "/nonexistent"})
    assert watched is None  # the check reports None, never a false pass


def test_compare_is_order_insensitive_but_not_value_insensitive():
    a = [("b", 2), ("a", 1)]
    assert _compare(a, [("a", 1), ("b", 2)], "t")["passed"] is True
    assert _compare(a, [("a", 1), ("b", 3)], "t")["passed"] is False


def test_compare_treats_numeric_spellings_as_equal():
    assert _compare([("x", "12.50")], [("x", 12.5)], "t")["passed"] is True


def test_do_not_modify_never_passes_silently():
    """No protected paths declared is None, not a free True."""
    assert do_not_modify_check("diff --git a/x b/x\n", [])["passed"] is None
    assert do_not_modify_check("diff --git a/dbt/m.sql b/dbt/m.sql\n", ["dbt/**"])["passed"] is False
    assert do_not_modify_check("diff --git a/dags/x.py b/dags/x.py\n", ["dbt/**"])["passed"] is True


def test_pushback_pattern_accepts_the_spellings_agents_use(tmp_path):
    """The e87f06 pattern failed a correct answer that wrote "sub-DAG" throughout.
    Grading a refusal must not turn on hyphenation."""
    from de_bench.scoring import file_contains_check

    spec = {"kind": "file_contains", "path": "RESPONSE.md", "matches": ["sub[\\s_-]*dag"]}
    for spelling in ("SubDagOperator", "sub-DAG", "sub dag", "Sub_Dag", "subdag"):
        (tmp_path / "RESPONSE.md").write_text(f"We cannot use the {spelling} here.")
        assert file_contains_check(str(tmp_path), spec)["passed"] is True, spelling
    (tmp_path / "RESPONSE.md").write_text("Done, no notes.")
    assert file_contains_check(str(tmp_path), spec)["passed"] is False


def test_changed_paths_reads_the_b_side():
    patch = "diff --git a/old.py b/new.py\n--- a/old.py\n+++ b/new.py\n"
    assert changed_paths(patch) == ["new.py"]


def test_file_lacks_flags_code_but_not_prose(tmp_path):
    """The whole point of stripping: a migration note mentioning the old idiom
    must not fail a file that no longer uses it."""
    from de_bench.scoring import file_lacks_check

    spec = {"kind": "file_lacks", "path": "dags/*.py", "patterns": [r"execution_date"]}
    dags = tmp_path / "dags"
    dags.mkdir()
    (dags / "clean.py").write_text(
        '"""Migrated off execution_date entirely."""\n'
        "# execution_date is gone from here too\n"
        "ds = context['logical_date']\n"
    )
    assert file_lacks_check(str(tmp_path), spec)["passed"] is True

    (dags / "dirty.py").write_text("ds = context['execution_date']\n")
    verdict = file_lacks_check(str(tmp_path), spec)
    assert verdict["passed"] is False
    assert "dirty.py" in verdict["detail"]


def test_file_lacks_fails_on_a_missing_file(tmp_path):
    """Deleting the file the check reads must not read as clean."""
    from de_bench.scoring import file_lacks_check

    spec = {"kind": "file_lacks", "path": "requirements.txt", "patterns": [r"apache-airflow==2"]}
    assert file_lacks_check(str(tmp_path), spec)["passed"] is False


def test_file_lacks_is_case_sensitive(tmp_path):
    """Old idioms are code; prose casing must not trip them."""
    from de_bench.scoring import file_lacks_check

    (tmp_path / "notes.md").write_text("The Execution Date concept is retired.\n")
    spec = {"kind": "file_lacks", "path": "notes.md", "patterns": [r"execution_date"]}
    assert file_lacks_check(str(tmp_path), spec)["passed"] is True


def test_stripped_python_keeps_code_and_geometry():
    from de_bench.scoring import stripped_python

    src = '"""module doc: execution_date"""\nx = 1  # execution_date\ny = "execution_date"\n'
    out = stripped_python(src)
    assert "execution_date" in out  # the string literal is code and survives
    assert out.count("execution_date") == 1
    assert len(out.splitlines()) == len(src.splitlines())


def test_stripped_python_returns_broken_source_as_is():
    from de_bench.scoring import stripped_python

    src = "def broken(:\n    pass\n"
    assert stripped_python(src) == src


def test_json_matches_is_exact_after_parsing(tmp_path):
    from de_bench.scoring import json_matches_check

    (tmp_path / "output.json").write_text('{\n  "path": ["2.10.5", "3.0.3"]\n}')
    spec = {"kind": "json_matches", "path": "output.json", "equals": {"path": ["2.10.5", "3.0.3"]}}
    assert json_matches_check(str(tmp_path), spec)["passed"] is True
    # List order is part of the answer for an upgrade route.
    spec_wrong = {**spec, "equals": {"path": ["3.0.3", "2.10.5"]}}
    assert json_matches_check(str(tmp_path), spec_wrong)["passed"] is False
    assert json_matches_check(str(tmp_path), {**spec, "path": "missing.json"})["passed"] is False


def test_warehouse_state_sees_table_drift(tmp_path):
    """The idempotent check is a diff of warehouse_state across a replay. The
    old hash(t.*) spelling stopped binding on newer DuckDB, so every file
    collapsed to one stable "unreadable" marker and drift was invisible —
    a non-idempotent DAG passed the exact check built to catch it. Guard the
    contract directly: tables hash, and a doubled row changes the hash."""
    import duckdb

    from de_bench.scoring import warehouse_state

    db = tmp_path / "include" / "data" / "lodestone.duckdb"
    db.parent.mkdir(parents=True)
    con = duckdb.connect(str(db))
    con.execute("create schema marts")
    con.execute("create table marts.spend as select 'CR-1' as carrier_id, 42.0 as eur")
    con.close()

    first = warehouse_state(str(tmp_path))
    key = "include/data/lodestone.duckdb:marts.spend"
    assert isinstance(first.get(key), list), f"table not hashed: {first}"

    con = duckdb.connect(str(db))
    con.execute("insert into marts.spend values ('CR-1', 42.0)")
    con.close()
    second = warehouse_state(str(tmp_path))
    assert first[key] != second[key], "a doubled row must change the fingerprint"


def test_dags_root_follows_the_workspace_shape(tmp_path):
    """The team-projects split moved every DagBag site to projects/ and the
    upgrade world's own-workspace tasks — dags/ at the root — silently scored
    against an empty folder. The root must follow the tree."""
    from de_bench.scoring import dags_root

    (tmp_path / "projects").mkdir()
    assert dags_root(str(tmp_path)) == f"{tmp_path}/projects"

    own = tmp_path / "own"
    (own / "dags").mkdir(parents=True)
    assert dags_root(str(own)) == f"{own}/dags"


def test_dag_fails_tells_a_failed_run_from_a_missing_dag(tmp_path, monkeypatch):
    """dag_fails could never pass: `dags test` always logs "Filling up the
    DagBag" and a failed task always prints a traceback, so the old
    Traceback-plus-DagBag heuristic read every genuine failure as an import
    failure. The discriminator is Airflow's could-not-be-found message."""
    import subprocess

    from de_bench import scoring

    def fake_run(argv, env, timeout, cwd):
        return subprocess.CompletedProcess(argv, 1, stdout=fake_run.blob, stderr="")

    monkeypatch.setattr(scoring, "_run", fake_run)

    fake_run.blob = (
        "Filling up the DagBag from /work/projects\n"
        "Traceback (most recent call last):\n  ...\nAirflowSensorTimeout\n"
        "DagRun failed\n"
    )
    verdict = scoring.dag_fails_check(str(tmp_path), "d", {}, "2026-03-24")
    assert verdict["passed"] is True, verdict

    fake_run.blob = (
        "Filling up the DagBag from /work/projects\n"
        "airflow.exceptions.AirflowException: Dag 'd' could not be found; "
        "either it does not exist or it failed to parse.\n"
    )
    verdict = scoring.dag_fails_check(str(tmp_path), "d", {}, "2026-03-24")
    assert verdict["passed"] is False, verdict


def test_dagbag_probe_survives_airflow_dropping_include_examples(tmp_path):
    """Airflow 3.3 removed DagBag's include_examples kwarg. The probe used to
    crash on it and book the crash as the agent's parse failure — on exactly
    the trees that upgraded furthest (32 stored trials, 13 on one task). The
    generated snippet must fall back to a bare DagBag() on TypeError."""
    from de_bench.scoring import _dagbag_code

    (tmp_path / "dags").mkdir()
    code = _dagbag_code(str(tmp_path))

    import textwrap

    harness = (
        "import sys, types\n"
        "mod = types.ModuleType('airflow.models.dagbag')\n"
        "class DagBag:\n"
        "    def __init__(self, root):  # the 3.3 signature: no kwarg\n"
        "        self.import_errors = {}\n"
        "        self.dags = {'ok': object()}\n"
        "mod.DagBag = DagBag\n"
        "sys.modules['airflow'] = types.ModuleType('airflow')\n"
        "sys.modules['airflow.models'] = types.ModuleType('airflow.models')\n"
        "sys.modules['airflow.models.dagbag'] = mod\n"
    )
    scope: dict = {}
    exec(harness + code, scope)  # noqa: S102 — the probe snippet itself is under test
    assert list(scope["b"].dags) == ["ok"]


def test_score_env_puts_the_dags_root_on_pythonpath(tmp_path):
    """Airflow 3's dag processor appends the bundle root to sys.path, so a
    sibling import inside dags/ is production-legal. The scoring env must
    grant the same, or the probe fails correct migrations — it cost keeper
    task-upgrade-5b8d1f its top-config kills before this pinned it."""
    from de_bench.scoring import build_score_env

    (tmp_path / "dags").mkdir()
    env = build_score_env(str(tmp_path), base_env={})
    parts = env["PYTHONPATH"].split(":")
    assert str(tmp_path) in parts
    assert f"{tmp_path}/dags" in parts


def test_score_env_points_plugins_at_the_scored_tree(tmp_path):
    """Airflow's settings.py appends PLUGINS_FOLDER to sys.path, so a DAG's
    bare import of a plugins/ module is production-legal. Left unset, the
    folder defaults to $AIRFLOW_HOME/plugins and the import fails — which
    misgraded a correct plugins-layout migration on task-upgrade-5b8d1f."""
    from de_bench.scoring import build_score_env

    (tmp_path / "dags").mkdir()
    (tmp_path / "plugins").mkdir()
    env = build_score_env(str(tmp_path), base_env={})
    assert env["AIRFLOW__CORE__PLUGINS_FOLDER"] == f"{tmp_path}/plugins"
    assert f"{tmp_path}/plugins" in env["PYTHONPATH"].split(":")

    bare = tmp_path / "bare"
    (bare / "dags").mkdir(parents=True)
    env2 = build_score_env(str(bare), base_env={})
    assert "AIRFLOW__CORE__PLUGINS_FOLDER" not in env2


# The metadata tables write_history writes, copied column for column from an
# Airflow 3.0.3 `airflow db migrate` — the version modal_app pins. Foreign keys
# to tables the writer never touches are left off. The NOT NULL marks and the
# defaults are what carry the test: _insert refuses to write a row that leaves
# a required column empty, so a schema this test gets wrong shows up here
# rather than as an unreadable run in a container.
AIRFLOW_303_SCHEMA = """
CREATE TABLE dag_run (
    id INTEGER NOT NULL,
    dag_id VARCHAR(250) NOT NULL,
    queued_at TIMESTAMP,
    logical_date TIMESTAMP,
    start_date TIMESTAMP,
    end_date TIMESTAMP,
    state VARCHAR(50),
    run_id VARCHAR(250) NOT NULL,
    creating_job_id INTEGER,
    run_type VARCHAR(50) NOT NULL,
    triggered_by VARCHAR(50),
    conf JSON,
    data_interval_start TIMESTAMP,
    data_interval_end TIMESTAMP,
    run_after TIMESTAMP NOT NULL,
    last_scheduling_decision TIMESTAMP,
    log_template_id INTEGER,
    updated_at TIMESTAMP,
    clear_number INTEGER DEFAULT '0' NOT NULL,
    backfill_id INTEGER,
    bundle_version VARCHAR(250),
    scheduled_by_job_id INTEGER,
    context_carrier JSON,
    span_status VARCHAR(250) DEFAULT 'not_started' NOT NULL,
    created_dag_version_id CHAR(32),
    CONSTRAINT dag_run_pkey PRIMARY KEY (id),
    CONSTRAINT dag_run_dag_id_run_id_key UNIQUE (dag_id, run_id),
    CONSTRAINT dag_run_dag_id_logical_date_key UNIQUE (dag_id, logical_date)
);
CREATE TABLE task_instance (
    id VARCHAR(36) NOT NULL,
    task_id VARCHAR(250) NOT NULL,
    dag_id VARCHAR(250) NOT NULL,
    run_id VARCHAR(250) NOT NULL,
    map_index INTEGER DEFAULT -1 NOT NULL,
    start_date TIMESTAMP,
    end_date TIMESTAMP,
    duration FLOAT,
    state VARCHAR(20),
    try_number INTEGER,
    max_tries INTEGER DEFAULT -1,
    hostname VARCHAR(1000),
    unixname VARCHAR(1000),
    pool VARCHAR(256) NOT NULL,
    pool_slots INTEGER NOT NULL,
    queue VARCHAR(256),
    priority_weight INTEGER,
    operator VARCHAR(1000),
    custom_operator_name VARCHAR(1000),
    queued_dttm TIMESTAMP,
    scheduled_dttm TIMESTAMP,
    queued_by_job_id INTEGER,
    last_heartbeat_at TIMESTAMP,
    pid INTEGER,
    executor VARCHAR(1000),
    executor_config BLOB,
    updated_at TIMESTAMP,
    rendered_map_index VARCHAR(250),
    context_carrier JSON,
    span_status VARCHAR(250) DEFAULT 'not_started' NOT NULL,
    external_executor_id VARCHAR(250),
    trigger_id INTEGER,
    trigger_timeout TIMESTAMP,
    next_method VARCHAR(1000),
    next_kwargs JSON,
    task_display_name VARCHAR(2000),
    dag_version_id CHAR(32),
    CONSTRAINT task_instance_pkey PRIMARY KEY (id),
    CONSTRAINT task_instance_composite_key UNIQUE (dag_id, task_id, run_id, map_index)
);
CREATE TABLE task_instance_history (
    task_instance_id VARCHAR(36) NOT NULL,
    task_id VARCHAR(250) NOT NULL,
    dag_id VARCHAR(250) NOT NULL,
    run_id VARCHAR(250) NOT NULL,
    map_index INTEGER DEFAULT -1 NOT NULL,
    try_number INTEGER NOT NULL,
    start_date TIMESTAMP,
    end_date TIMESTAMP,
    duration FLOAT,
    state VARCHAR(20),
    max_tries INTEGER DEFAULT -1,
    hostname VARCHAR(1000),
    unixname VARCHAR(1000),
    pool VARCHAR(256) NOT NULL,
    pool_slots INTEGER NOT NULL,
    queue VARCHAR(256),
    priority_weight INTEGER,
    operator VARCHAR(1000),
    custom_operator_name VARCHAR(1000),
    queued_dttm TIMESTAMP,
    scheduled_dttm TIMESTAMP,
    queued_by_job_id INTEGER,
    pid INTEGER,
    executor VARCHAR(1000),
    executor_config BLOB,
    updated_at TIMESTAMP,
    rendered_map_index VARCHAR(250),
    context_carrier JSON,
    span_status VARCHAR(250) DEFAULT 'not_started' NOT NULL,
    external_executor_id VARCHAR(250),
    trigger_id INTEGER,
    trigger_timeout DATETIME,
    next_method VARCHAR(1000),
    next_kwargs JSON,
    task_display_name VARCHAR(2000),
    dag_version_id CHAR(32),
    CONSTRAINT task_instance_history_pkey PRIMARY KEY (task_instance_id),
    CONSTRAINT task_instance_history_dtrt_uq UNIQUE (dag_id, task_id, run_id, map_index, try_number)
);
CREATE TABLE dag_version (
    id CHAR(32) NOT NULL,
    version_number INTEGER NOT NULL,
    dag_id VARCHAR(250) NOT NULL,
    bundle_name VARCHAR(250),
    bundle_version VARCHAR(250),
    created_at TIMESTAMP NOT NULL,
    last_updated TIMESTAMP NOT NULL,
    CONSTRAINT dag_version_pkey PRIMARY KEY (id)
);
CREATE TABLE log_template (
    id INTEGER NOT NULL,
    filename VARCHAR(1000) NOT NULL,
    elasticsearch_id VARCHAR(1000) NOT NULL,
    created_at TIMESTAMP NOT NULL,
    CONSTRAINT log_template_pkey PRIMARY KEY (id)
);
"""

# Airflow 3.0.3's own logging.log_filename_template, as db migrate seeds it.
LOG_TEMPLATE_303 = (
    "dag_id={{ ti.dag_id }}/run_id={{ ti.run_id }}/task_id={{ ti.task_id }}/"
    "{% if ti.map_index >= 0 %}map_index={{ ti.map_index }}/{% endif %}"
    "attempt={{ try_number|default(ti.try_number) }}.log"
)


def _seeded_home(root, dag_id="fin_close_monthly"):
    """A scratch AIRFLOW_HOME holding an empty Airflow 3.0.3 metadata database."""
    import sqlite3

    home = root / "af-home"
    home.mkdir(parents=True)
    conn = sqlite3.connect(home / "airflow.db")
    conn.executescript(AIRFLOW_303_SCHEMA)
    conn.execute(
        "INSERT INTO log_template (id, filename, elasticsearch_id, created_at)"
        " VALUES (1, ?, '', '2026-01-01')",
        (LOG_TEMPLATE_303,),
    )
    conn.execute(
        "INSERT INTO dag_version (id, version_number, dag_id, created_at, last_updated)"
        " VALUES ('deadbeef', 1, ?, '2026-01-01', '2026-01-01')",
        (dag_id,),
    )
    conn.commit()
    conn.close()
    return home


def test_write_history_writes_the_states_tries_and_logs_it_was_given(tmp_path):
    """The manifest is the whole contract for a diagnosis task's evidence: the
    states it names must land, the try count must be readable as history, and
    the log fixture must sit where Airflow would have written it. No world is
    involved — a synthetic manifest against a scratch AIRFLOW_HOME."""
    import sqlite3

    from de_bench.scoring import write_history

    home = _seeded_home(tmp_path)
    work = tmp_path / "work"
    (work / "evidence" / "logs").mkdir(parents=True)
    (work / "evidence" / "logs" / "tie.log").write_text("GL does not tie: 4212.19 vs 4212.00\n")

    manifest = [
        {
            "dag_id": "fin_close_monthly",
            "runs": [
                {
                    "ds": "2026-02-02",
                    "state": "failed",
                    "tries": 2,
                    "task_states": {"load_gl": "success", "tie_check": "failed"},
                    "logs": {"tie_check": "evidence/logs/tie.log"},
                },
                {
                    "ds": "2026-02-02",
                    "state": "success",
                    "task_states": {"load_gl": "success", "tie_check": "success"},
                },
            ],
        }
    ]
    summary = write_history({"AIRFLOW_HOME": str(home)}, str(work), manifest, reserialize=False)
    assert summary == {"executed": 0, "synthesized": 2, "notes": []}

    conn = sqlite3.connect(home / "airflow.db")
    runs = conn.execute(
        "SELECT run_id, run_type, state, logical_date, created_dag_version_id"
        " FROM dag_run ORDER BY run_id"
    ).fetchall()
    # Two runs on one date: the first keeps the logical date, the rerun is a
    # manual run without one, which is all the unique index allows.
    assert runs == [
        ("manual__2026-02-02T00:00:00+00:00__2", "manual", "success", None, "deadbeef"),
        ("scheduled__2026-02-02T00:00:00+00:00", "scheduled", "failed",
         "2026-02-02 00:00:00.000000", "deadbeef"),
    ]

    # Never a bare DagRun: a success with no task instances is the vacuous green.
    for run_id, *_ in runs:
        count = conn.execute(
            "SELECT count(*) FROM task_instance WHERE run_id = ?", (run_id,)
        ).fetchone()[0]
        assert count == 2

    failed = "scheduled__2026-02-02T00:00:00+00:00"
    assert sorted(conn.execute(
        "SELECT task_id, state, try_number, max_tries FROM task_instance WHERE run_id = ?", (failed,)
    )) == [("load_gl", "success", 2, 1), ("tie_check", "failed", 2, 1)]
    # The attempts before the last one are recorded, or a try count is a number
    # with no history behind it.
    assert sorted(conn.execute(
        "SELECT task_id, try_number, state FROM task_instance_history WHERE run_id = ?", (failed,)
    )) == [("load_gl", 1, "up_for_retry"), ("tie_check", 1, "up_for_retry")]
    conn.close()

    log = home / "logs" / f"dag_id=fin_close_monthly/run_id={failed}/task_id=tie_check/attempt=2.log"
    assert log.read_text() == "GL does not tie: 4212.19 vs 4212.00\n"


def test_write_history_writes_the_same_bytes_for_the_trial_and_the_scorer(tmp_path):
    """The agent and the grader read one history, so nothing in it may come
    from the clock. Seed one manifest into two homes and compare the rows."""
    import sqlite3

    from de_bench.scoring import write_history

    manifest = [{
        "dag_id": "fin_close_monthly",
        "runs": [{"ds": "2026-02-02", "state": "failed", "tries": 3,
                  "task_states": {"tie_check": "failed"}}],
    }]
    rows = []
    for side in ("trial", "scorer"):
        home = _seeded_home(tmp_path / side)
        write_history({"AIRFLOW_HOME": str(home)}, str(tmp_path), manifest, reserialize=False)
        conn = sqlite3.connect(home / "airflow.db")
        rows.append((
            conn.execute(
                "SELECT dag_id, run_id, run_type, state, logical_date, start_date, end_date,"
                " data_interval_start, data_interval_end, run_after, queued_at, updated_at,"
                " triggered_by, clear_number FROM dag_run"
            ).fetchall(),
            conn.execute(
                "SELECT task_id, run_id, state, try_number, max_tries, start_date, end_date,"
                " duration, pool, pool_slots FROM task_instance"
            ).fetchall(),
            conn.execute(
                "SELECT task_id, try_number, state, start_date, end_date"
                " FROM task_instance_history ORDER BY try_number"
            ).fetchall(),
        ))
        conn.close()
    assert rows[0] == rows[1]


def test_write_history_refuses_a_run_it_cannot_give_task_instances(tmp_path):
    """A DagRun with no task instances passes any check that reads only
    dag_run.state — the vacuous green. With no task_states, and no Airflow on
    PATH to list the DAG's tasks, the run is skipped and said so."""
    import sqlite3

    from de_bench.scoring import write_history

    home = _seeded_home(tmp_path)
    (tmp_path / "empty").mkdir()
    manifest = [{"dag_id": "fin_close_monthly", "runs": [{"ds": "2026-02-02", "state": "success"}]}]
    summary = write_history(
        {"AIRFLOW_HOME": str(home), "PATH": str(tmp_path / "empty")},
        str(tmp_path), manifest, reserialize=False,
    )
    assert summary["synthesized"] == 0
    assert "not seeded" in " ".join(summary["notes"])
    conn = sqlite3.connect(home / "airflow.db")
    assert conn.execute("SELECT count(*) FROM dag_run").fetchone()[0] == 0
    conn.close()


def test_write_history_log_path_matches_airflows_own_template():
    """Rendered without Jinja — these logs get written in a venv that has no
    Airflow — so the default template is pinned here."""
    from de_bench.scoring import _render_log_path

    assert _render_log_path(
        LOG_TEMPLATE_303, "fin_close_monthly", "scheduled__2026-02-02T00:00:00+00:00", "tie_check", 2
    ) == (
        "dag_id=fin_close_monthly/run_id=scheduled__2026-02-02T00:00:00+00:00"
        "/task_id=tie_check/attempt=2.log"
    )
    # A template this renderer cannot read falls back to that shape rather than
    # writing a log under a half-substituted path.
    assert _render_log_path(
        "{{ ti.dag_id }}/{% for x in y %}{% endfor %}.log", "d", "r", "t", 1
    ) == "dag_id=d/run_id=r/task_id=t/attempt=1.log"
