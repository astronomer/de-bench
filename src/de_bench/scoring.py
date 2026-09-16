"""Scoring: rebuild the workspace, apply the patch, execute checks in stage order.

Design borrowed from af-bench/airflow-bench: tri-state outcomes (True/False/None
where None = not applicable and drops out of judgment), execution over static
analysis, and a critical `do_not_modify` gate. Runs entirely from stored
artifacts — no agent, no model spend.

What to check comes from the task's `checks.yaml`. Two gates always apply:
`delivered` (the patch is non-empty) and `do_not_modify` (no protected path was
touched). Everything else is declared per task, and runs in stage order so a
later stage never reports on a workspace an earlier one already condemned:

  parse    dag_parses, no_dag, no_dbt_at_parse   the DagBag imports, holds what
           it should, and does no real work on the way
  parse    dag_structure              the DAG kept its task ids (and operators)
  diff     unchanged           the graded relations hold what the untouched
                                     world produces, run in the same container
  execute  dag_runs, dag_fails       `airflow dags test` exits as the ticket needs
  output   partition_csv, warehouse_table, file_contains, file_absent, file_lacks
  replay   idempotent                a second run of the same date changes nothing

An output check names what the ticket named — a file, its header, a table, its
columns — and may carry `expect_sql`, a query over the read-only fixtures whose
result the produced rows must equal. Expressing the expectation as a query keeps it
reviewable and derived from the world, instead of a constant somebody typed.

`pass` = delivered, plus every declared check that ran, with None dropping out
and any False vetoing.
"""

from __future__ import annotations

import datetime as dt
import fnmatch
import json
import os
import re
import sqlite3
import subprocess
import uuid

LOGICAL_DATE = "2026-03-01"
DAGS_TEST_TIMEOUT = 900

# Scoring is minutes of silence per trial otherwise, and the only way to tell a
# slow seed from a hung one is to watch it say so. Modal captures stdout, so
# these land in the run's logs — unbuffered, or they arrive after the fact.
_PROGRESS = {"label": ""}


def progress(message: str) -> None:
    print(f"[{_PROGRESS['label']}] {message}" if _PROGRESS["label"] else message, flush=True)


def changed_paths(patch_text: str) -> list[str]:
    paths = []
    for line in patch_text.splitlines():
        if line.startswith("diff --git a/"):
            # "diff --git a/<path> b/<path>" — take the b-side.
            b = line.split(" b/", 1)
            if len(b) == 2:
                paths.append(b[1])
    return paths


def do_not_modify_check(patch_text: str, protected: list[str]) -> dict:
    """False on any protected path touched; None when nothing is protected —
    never a free pass (af-bench's `do_not_modify_declared` distinction)."""
    if not protected:
        return {"passed": None, "detail": "no protected paths declared"}
    hits = [
        p
        for p in changed_paths(patch_text)
        if any(fnmatch.fnmatch(p, g) or fnmatch.fnmatch(p, g.rstrip("*").rstrip("/") + "/*") for g in protected)
    ]
    if hits:
        return {"passed": False, "detail": f"touched protected paths: {', '.join(hits[:5])}"}
    return {"passed": True, "detail": f"{len(changed_paths(patch_text))} changed paths, none protected"}


def _run(argv: list[str], env: dict, timeout: int, cwd: str) -> subprocess.CompletedProcess:
    return subprocess.run(argv, env=env, cwd=cwd, capture_output=True, text=True, timeout=timeout, check=False)


def dags_root(workdir: str) -> str:
    """Where this workspace keeps its DAGs. Shared-tree worlds split into team
    projects (projects/<team>/dags/); own-workspace tasks — the whole upgrade
    world — ship a single project with dags/ at the root. When the team split
    landed, every DagBag site moved to projects/ and own-workspace scoring
    silently read an empty folder: every dag_parses came back "missing" on a
    tree whose DAGs were fine."""
    return f"{workdir}/projects" if os.path.isdir(os.path.join(workdir, "projects")) else f"{workdir}/dags"


def _dagbag_code(workdir: str) -> str:
    """Construct `b` tolerantly. Airflow 3.3 dropped DagBag's include_examples
    kwarg; a probe that crashes on it books the crash as the agent's parse
    failure — on exactly the trees that upgraded furthest. 32 stored trials
    carried that misgrade before this guard."""
    root = dags_root(workdir)
    return (
        "from airflow.models.dagbag import DagBag\n"
        "try:\n"
        f"    b = DagBag('{root}', include_examples=False)\n"
        "except TypeError:\n"
        f"    b = DagBag('{root}')\n"
    )


def parses_check(workdir: str, dag_ids: list[str], env: dict) -> dict:
    """DagBag import in-process: no import errors, focus dag_ids present."""
    code = (
        "import json,sys\n"
        + _dagbag_code(workdir)
        + "print(json.dumps({'errors': {k: str(v)[-400:] for k, v in b.import_errors.items()},"
        " 'dags': list(b.dags.keys())}))\n"
    )
    try:
        proc = _run(["python", "-c", code], env, 300, workdir)
    except subprocess.TimeoutExpired:
        return {"passed": False, "detail": "DagBag load timed out"}
    if proc.returncode != 0:
        return {"passed": False, "detail": f"DagBag crashed: {(proc.stderr or '')[-400:]}"}
    try:
        out = json.loads(proc.stdout.strip().splitlines()[-1])
    except Exception:
        return {"passed": False, "detail": f"unparseable DagBag output: {proc.stdout[-200:]}"}
    if out["errors"]:
        # The first error's message rides along: "import errors in: <path>" alone
        # cannot distinguish a shared agent mistake from a shared grading bug.
        path, msg = next(iter(out["errors"].items()))
        more = f" (+{len(out['errors']) - 1} more)" if len(out["errors"]) > 1 else ""
        return {"passed": False, "detail": f"import error in {path}{more}: {msg.strip()[-300:]}"}
    missing = [d for d in dag_ids if d not in out["dags"]]
    if missing:
        return {"passed": False, "detail": f"expected dag_ids missing: {', '.join(missing)}"}
    return {"passed": True, "detail": f"{len(out['dags'])} dags loaded clean"}


def list_dags(workdir: str, env: dict) -> list[str]:
    """DAG ids currently in the workspace (used pre-patch for the baseline set)."""
    code = (
        "import json\n"
        + _dagbag_code(workdir)
        + "print(json.dumps(list(b.dags.keys())))\n"
    )
    try:
        proc = _run(["python", "-c", code], env, 300, workdir)
        return json.loads(proc.stdout.strip().splitlines()[-1]) if proc.returncode == 0 else []
    except Exception:  # noqa: BLE001
        return []


def replay_world(workdir: str, baseline_dags: list[str], focus: list[str], env: dict, ds: str = LOGICAL_DATE) -> None:
    """Run the world's own pipeline so the focus DAG sees the tables it reads.

    Agents ran upstream DAGs live during their trial; the scorer replays that
    context. Two passes (raw->staging, then marts); failures tolerated — a broken
    baseline DAG is the world's problem, the verdict stays on the focus DAG.

    Seeding is per logical date: a task graded on the date its ticket worked
    through needs that date's upstream tables, not another day's.
    """
    todo = [d for d in baseline_dags if d not in focus]
    if not todo:
        progress(f"seed {ds}: nothing to replay")
        return
    for pass_no in (1, 2):
        for dag_id in todo:
            progress(f"seed {ds} pass {pass_no}/2: {dag_id}")
            try:
                _run(["airflow", "dags", "test", dag_id, ds], env, 600, workdir)
            except subprocess.TimeoutExpired:
                progress(f"seed {ds}: {dag_id} timed out (tolerated)")


# ---------------------------------------------------------------------------
# Seeded run history
#
# A diagnosis task starts from run history, and the agent and the grader must
# read the same history — so both call write_history, and nothing else writes
# seeded runs. Two kinds of entry:
#
#   ("dag_id", "ds")  — run it for real with `airflow dags test`.
#   {"dag_id", "runs"} — write it into the metadata database directly, with the
#                        states, try counts and logs the task asked for.
#
# Synthesis always writes one task_instance row per task. A DagRun marked
# success with no task instances is the vacuous-green trap: a check that reads
# only dag_run.state passes on a run that did nothing.
#
# Every timestamp is derived from the run's own date, never from the clock, so
# the trial side and the scorer side write the same bytes.

SEED_RUN_TIMEOUT = 600

# Airflow 3's default logging.log_filename_template, used when the metadata
# database has no log_template row to read it from.
LOG_FILENAME_TEMPLATE = (
    "dag_id={{ ti.dag_id }}/run_id={{ ti.run_id }}/task_id={{ ti.task_id }}/"
    "{% if ti.map_index >= 0 %}map_index={{ ti.map_index }}/{% endif %}"
    "attempt={{ try_number|default(ti.try_number) }}.log"
)
_MAP_INDEX_BLOCK = re.compile(r"\{%\s*if\s+ti\.map_index\s*>=\s*0\s*%\}.*?\{%\s*endif\s*%\}", re.S)
_JINJA_EXPR = re.compile(r"\{\{(.*?)\}\}", re.S)


def _history_entries(history) -> list[tuple[str, list[dict]]]:
    """Every accepted history shape, as (dag_id, [run, ...]).

    Entries survive a JSON round trip to the container, so a pair arrives as a
    list there and as a tuple here. A run with no `state` and no `task_states`
    is an executed run.
    """
    out = []
    for entry in history or ():
        if isinstance(entry, dict) and "runs" in entry:
            out.append((str(entry["dag_id"]), [dict(r) for r in entry.get("runs") or ()]))
        elif isinstance(entry, dict):
            out.append((str(entry["dag_id"]), [{"ds": str(entry.get("ds") or LOGICAL_DATE)}]))
        else:
            dag_id, ds = entry
            out.append((str(dag_id), [{"ds": str(ds)}]))
    return out


def _airflow_home(env: dict) -> str:
    return env.get("AIRFLOW_HOME") or os.path.join(env.get("HOME", "/root"), "airflow")


def _metadata_db(env: dict, workdir: str) -> str | None:
    """The sqlite file behind this AIRFLOW_HOME, or None when there is none.

    The fast path is the default layout — a fresh AIRFLOW_HOME, `airflow db
    migrate`, sqlite at airflow.db — which is what both call sites build. When
    that file is not there, ask Airflow itself; a non-sqlite backend gets None,
    and synthesis says so rather than writing half a history.
    """
    conn = env.get("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN")
    if not conn:
        default = os.path.join(_airflow_home(env), "airflow.db")
        if os.path.exists(default):
            return default
        try:
            proc = _run(["airflow", "config", "get-value", "database", "sql_alchemy_conn"], env, 120, workdir)
        except (OSError, subprocess.TimeoutExpired):
            return None
        # get-value writes provider warnings to stdout ahead of the value, so
        # take the last line, not the first.
        lines = [l.strip() for l in (proc.stdout or "").splitlines() if l.strip()]
        conn = lines[-1] if lines else ""
    if not conn.startswith("sqlite:"):
        return None
    path = conn.split("://", 1)[-1].lstrip("/")
    return "/" + path if conn.startswith("sqlite:////") else path


def _stamp(moment: dt.datetime) -> str:
    """A timestamp in the shape Airflow's sqlite tables hold."""
    return moment.strftime("%Y-%m-%d %H:%M:%S.%f")


def _insert(conn: sqlite3.Connection, table: str, values: dict) -> None:
    """Insert the columns this Airflow actually has, and refuse a silent gap.

    Airflow adds and drops metadata columns between versions. Writing only the
    columns that exist keeps a new version from crashing the seed; naming the
    required columns we cannot fill keeps an old assumption from writing a row
    Airflow will not read.
    """
    info = list(conn.execute(f"PRAGMA table_info({table})"))
    if not info:
        raise RuntimeError(f"{table} is not a table in this metadata database")
    columns = {row[1]: row for row in info}
    use = {name: value for name, value in values.items() if name in columns}
    missing = [
        name
        for name, row in columns.items()
        if row[3] and row[4] is None and not row[5] and name not in use
    ]
    if missing:
        raise RuntimeError(f"{table} needs a value for {', '.join(missing)}")
    conn.execute(
        f"INSERT INTO {table} ({', '.join(use)}) VALUES ({', '.join('?' * len(use))})",
        list(use.values()),
    )


def _dag_version_id(conn: sqlite3.Connection, dag_id: str) -> str | None:
    """The newest serialized version of this DAG, so a seeded run points at the
    same code the agent reads. None when the DAG was never serialized."""
    try:
        row = conn.execute(
            "SELECT id FROM dag_version WHERE dag_id = ? ORDER BY version_number DESC LIMIT 1",
            (dag_id,),
        ).fetchone()
    except sqlite3.Error:
        return None
    return row[0] if row else None


def _log_template(conn: sqlite3.Connection) -> str:
    try:
        row = conn.execute("SELECT filename FROM log_template ORDER BY id DESC LIMIT 1").fetchone()
    except sqlite3.Error:
        return LOG_FILENAME_TEMPLATE
    return row[0] if row and row[0] else LOG_FILENAME_TEMPLATE


def _render_log_path(template: str, dag_id: str, run_id: str, task_id: str, try_number: int) -> str:
    """Airflow's log path for one attempt.

    The template is Jinja, but the only one in play is the default, and the
    harness must render it without importing Airflow — the tests run in a venv
    that has none. Substitute the expressions we know, drop the map_index block
    (seeded instances are never mapped), and fall back to the default shape if
    anything is left over.
    """

    def one(match: re.Match) -> str:
        expr = match.group(1)
        for token, value in (
            ("task_id", task_id),
            ("dag_id", dag_id),
            ("run_id", run_id),
            ("try_number", str(try_number)),
            ("map_index", "-1"),
        ):
            if token in expr:
                return value
        return match.group(0)

    text = _JINJA_EXPR.sub(one, _MAP_INDEX_BLOCK.sub("", template))
    if "{{" in text or "{%" in text:
        return f"dag_id={dag_id}/run_id={run_id}/task_id={task_id}/attempt={try_number}.log"
    return text


def _dag_task_ids(dag_id: str, env: dict, workdir: str) -> list[str]:
    try:
        proc = _run(["airflow", "tasks", "list", dag_id], env, 300, workdir)
    except (OSError, subprocess.TimeoutExpired):
        return []
    if proc.returncode != 0:
        return []
    return [
        line.strip()
        for line in (proc.stdout or "").splitlines()
        if line.strip() and not line.startswith("[") and " " not in line.strip()
    ]


def _write_seed_logs(
    conn: sqlite3.Connection, env: dict, workdir: str, dag_id: str, run_id: str,
    logs: dict, tries: dict[str, int],
) -> list[str]:
    """Copy the task's own log fixtures to where Airflow would have written them.

    A value is either one workspace-relative path, which lands on the task's
    last attempt, or a {try number: path} mapping when the ticket needs the
    earlier attempt to read differently from the retry.
    """
    notes = []
    base = env.get("AIRFLOW__LOGGING__BASE_LOG_FOLDER") or os.path.join(_airflow_home(env), "logs")
    template = _log_template(conn)
    for task_id, value in (logs or {}).items():
        wanted = value if isinstance(value, dict) else {tries.get(task_id, 1): value}
        for try_number, source in wanted.items():
            path = source if os.path.isabs(source) else os.path.join(workdir, source)
            if not os.path.isfile(path):
                notes.append(f"{dag_id}.{task_id}: no log fixture at {source}")
                continue
            dest = os.path.join(base, _render_log_path(template, dag_id, run_id, task_id, int(try_number)))
            os.makedirs(os.path.dirname(dest), exist_ok=True)
            with open(path, "rb") as src, open(dest, "wb") as out:
                out.write(src.read())
    return notes


def _synthesize_run(
    conn: sqlite3.Connection, env: dict, workdir: str, dag_id: str, run: dict, occurrence: int,
) -> list[str]:
    """Write one seeded DagRun and its task instances.

    The first run on a date is the scheduled one and keeps that logical date.
    A later run on the same date is a manual rerun with no logical date, which
    is both how Airflow 3 records one and the only way past the unique index on
    (dag_id, logical_date).
    """
    ds = str(run.get("ds") or LOGICAL_DATE)
    state = str(run.get("state") or "success")
    task_states = dict(run.get("task_states") or {})
    if not task_states:
        discovered = _dag_task_ids(dag_id, env, workdir)
        if not discovered:
            raise RuntimeError("no task_states, and its tasks could not be listed — run not seeded")
        task_states = {task_id: state for task_id in discovered}

    midnight = dt.datetime.strptime(ds, "%Y-%m-%d")
    taken = conn.execute(
        "SELECT 1 FROM dag_run WHERE dag_id = ? AND logical_date = ?", (dag_id, _stamp(midnight))
    ).fetchone()
    scheduled = occurrence == 0 and not taken
    run_id = str(
        run.get("run_id")
        or (f"scheduled__{ds}T00:00:00+00:00" if scheduled else f"manual__{ds}T00:00:00+00:00__{occurrence + 1}")
    )
    run_start = midnight + dt.timedelta(hours=occurrence)
    run_end = run_start + dt.timedelta(minutes=5)
    version_id = _dag_version_id(conn, dag_id)

    _insert(conn, "dag_run", {
        "dag_id": dag_id,
        "run_id": run_id,
        "run_type": str(run.get("run_type") or ("scheduled" if scheduled else "manual")),
        "state": state,
        "logical_date": _stamp(midnight) if scheduled else None,
        "data_interval_start": _stamp(midnight),
        "data_interval_end": _stamp(midnight + dt.timedelta(days=1)),
        "run_after": _stamp(midnight + dt.timedelta(days=1)),
        "queued_at": _stamp(run_start),
        "start_date": _stamp(run_start),
        "end_date": _stamp(run_end) if state in ("success", "failed") else None,
        "updated_at": _stamp(run_end),
        # SQLAlchemy stores this enum by name, so the column holds TIMETABLE,
        # not timetable — `airflow tasks states-for-dag-run` raises LookupError
        # on the lower-case form. run_type is a plain string and stays as it is.
        "triggered_by": "TIMETABLE" if scheduled else "CLI",
        "clear_number": 0,
        "conf": "{}",
        "created_dag_version_id": version_id,
    })

    default_tries = int(run.get("tries") or 1)
    per_task_tries = {k: int(v) for k, v in (run.get("task_tries") or {}).items()}
    tries = {task_id: per_task_tries.get(task_id, default_tries) for task_id in task_states}
    for position, (task_id, task_state) in enumerate(sorted(task_states.items())):
        attempts = max(1, tries[task_id])
        started = run_start + dt.timedelta(minutes=position)
        ended = started + dt.timedelta(seconds=30)
        # Every attempt before the last one ended up_for_retry — that is what
        # Airflow records, and it is what makes a try count readable as history.
        for attempt in range(1, attempts):
            _insert(conn, "task_instance_history", {
                "task_instance_id": str(uuid.uuid4()),
                "dag_id": dag_id, "task_id": task_id, "run_id": run_id, "map_index": -1,
                "try_number": attempt, "state": "up_for_retry",
                "start_date": _stamp(started), "end_date": _stamp(ended), "duration": 30.0,
                "max_tries": attempts - 1, "pool": "default_pool", "pool_slots": 1,
                "queue": "default", "priority_weight": 1, "unixname": "root",
                "hostname": "de-bench", "updated_at": _stamp(ended),
                "dag_version_id": version_id,
            })
            started = ended + dt.timedelta(seconds=30)
            ended = started + dt.timedelta(seconds=30)
        _insert(conn, "task_instance", {
            "id": str(uuid.uuid4()),
            "dag_id": dag_id, "task_id": task_id, "run_id": run_id, "map_index": -1,
            "try_number": attempts, "state": task_state,
            "start_date": _stamp(started), "end_date": _stamp(ended), "duration": 30.0,
            "max_tries": attempts - 1, "pool": "default_pool", "pool_slots": 1,
            "queue": "default", "priority_weight": 1, "unixname": "root",
            "hostname": "de-bench", "updated_at": _stamp(ended),
            "task_display_name": task_id, "dag_version_id": version_id,
        })
    return _write_seed_logs(conn, env, workdir, dag_id, run_id, run.get("logs") or {}, tries)


def write_history(env: dict, workdir: str, history, migrate: bool = False, reserialize: bool = True) -> dict:
    """Write a task's seeded run history, and report what it wrote.

    The one writer. The trial calls it before the agent gets the keys and the
    scorer calls it before it grades, and they must agree byte for byte — two
    copies of this loop is how they would stop agreeing.

    migrate builds the metadata database first, for a caller that made a fresh
    AIRFLOW_HOME. reserialize fills the serialized-DAG table, which `airflow
    dags test` reads on 3.0.0 and which seeded runs point at for their version;
    a caller that just did one passes False.

    Measured on 3.0.3, and it decides what dates a task may seed: `airflow dags
    test <dag> <ds>` deletes any run already holding that logical date, and the
    cascade takes its task instances with it. So seed a diagnosis at a date the
    graded run does not touch. A second run on one date survives — it carries
    no logical date — but the first one does not.
    """
    summary: dict = {"executed": 0, "synthesized": 0, "notes": []}
    if not history:
        return summary
    if migrate:
        _run(["airflow", "db", "migrate"], env, 300, workdir)
    if reserialize:
        try:
            _run(["airflow", "dags", "reserialize"], env, 300, workdir)
        except subprocess.TimeoutExpired:
            summary["notes"].append("reserialize timed out")

    conn = None
    try:
        for dag_id, runs in _history_entries(history):
            seen: dict[str, int] = {}
            for run in runs:
                ds = str(run.get("ds") or LOGICAL_DATE)
                occurrence = seen.get(ds, 0)
                seen[ds] = occurrence + 1
                if not (run.get("state") or run.get("task_states")):
                    progress(f"seed run {dag_id} at {ds}")
                    try:
                        _run(["airflow", "dags", "test", dag_id, ds], env, SEED_RUN_TIMEOUT, workdir)
                    except subprocess.TimeoutExpired:
                        progress(f"seed run {dag_id} at {ds} timed out (tolerated)")
                    summary["executed"] += 1
                    continue
                if conn is None:
                    db = _metadata_db(env, workdir)
                    if db is None:
                        summary["notes"].append("no sqlite metadata database — nothing synthesized")
                        progress(summary["notes"][-1])
                        return summary
                    conn = sqlite3.connect(db)
                progress(f"seed history {dag_id} at {ds}: {run.get('state') or 'success'}")
                try:
                    summary["notes"] += _synthesize_run(conn, env, workdir, dag_id, run, occurrence)
                    conn.commit()
                except (sqlite3.Error, RuntimeError) as exc:
                    conn.rollback()
                    summary["notes"].append(f"{dag_id} at {ds}: {exc}")
                else:
                    summary["synthesized"] += 1
    finally:
        if conn is not None:
            conn.close()
    for note in summary["notes"]:
        progress(f"seed history: {note}")
    return summary


def runs_check(workdir: str, dag_ids: list[str], env: dict, ds: str = LOGICAL_DATE,
               require_tasks: list[str] | None = None) -> dict:
    if not dag_ids:
        return {"passed": None, "detail": "task declares no focus dag_ids"}
    for dag_id in dag_ids:
        try:
            proc = _run(["airflow", "dags", "test", dag_id, ds], env, DAGS_TEST_TIMEOUT, workdir)
        except subprocess.TimeoutExpired:
            return {"passed": False, "detail": f"{dag_id}: dags test timed out"}
        blob = (proc.stderr or "") + "\n" + (proc.stdout or "")
        if proc.returncode != 0:
            hits = [l for l in blob.splitlines() if any(k in l.lower() for k in ("error", "failed", "exception", "traceback"))]
            tail = " | ".join(hits[-8:]) if hits else blob[-800:]
            return {"passed": False, "detail": f"{dag_id} at {ds}: exit {proc.returncode}: {tail[-1200:]}"}
        # `dags test` exits 0 when the grading date sits before the DAG's
        # start_date — no task instance runs and nothing says so. Two trials
        # scored "green" over a run that executed nothing before this guard;
        # every genuinely green run marks at least one task state=success.
        if "state=success" not in blob:
            return {
                "passed": False,
                "detail": (
                    f"{dag_id} at {ds}: exited 0 but no task instance reached "
                    f"success — the run executed nothing (a start_date after "
                    f"{ds} does this silently)"
                ),
            }
        # One success anywhere is not the same as the graded work running: a
        # task made to skip or short-circuit leaves the run green while the
        # step the ticket is about never fires. A check that names
        # `require_tasks` wants each of those task instances to reach success
        # itself. Task-level lines never reach the dags-test stdout (only the
        # dag-level state does), so the states are read from the metadata DB
        # row the run just wrote.
        if require_tasks:
            import json as _json

            snippet = (
                "import json, sys\n"
                "from airflow.utils.session import create_session\n"
                "from airflow.models.dagrun import DagRun\n"
                "with create_session() as s:\n"
                "    dr = (s.query(DagRun).filter(DagRun.dag_id == sys.argv[1])\n"
                "          .order_by(DagRun.id.desc()).first())\n"
                "    out = {} if dr is None else {\n"
                "        ti.task_id: str(ti.state) for ti in dr.get_task_instances(session=s)}\n"
                "print(json.dumps(out))\n"
            )
            try:
                ti_proc = _run(["python", "-c", snippet, dag_id], env, 120, workdir)
                states = _json.loads((ti_proc.stdout or "").strip().splitlines()[-1])
            except Exception as exc:  # noqa: BLE001
                return {
                    "passed": False,
                    "detail": f"{dag_id} at {ds}: could not read task states ({type(exc).__name__}: {exc})",
                }
            for tid in require_tasks:
                if states.get(tid) != "success":
                    return {
                        "passed": False,
                        "detail": (
                            f"{dag_id} at {ds}: task {tid} ended {states.get(tid)!r}, "
                            f"not success — a run that skips or short-circuits it "
                            f"still exits 0 and looks green (states: {states})"
                        ),
                    }
    return {"passed": True, "detail": f"dags test green at {ds}: {', '.join(dag_ids)}"}


def dag_fails_check(workdir: str, dag_id: str, env: dict, ds: str = LOGICAL_DATE) -> dict:
    """The mirror of dag_runs: the ticket wants this date to end as a failed run.

    A DAG that swallows its own failure looks green on the dashboard, which is
    the bug some tickets are explicitly about — so passing here means exiting
    non-zero, and an import error still fails (that is a broken DAG, not a
    failure the pipeline chose to surface).
    """
    try:
        proc = _run(["airflow", "dags", "test", dag_id, ds], env, DAGS_TEST_TIMEOUT, workdir)
    except subprocess.TimeoutExpired:
        return {"passed": False, "detail": f"{dag_id}: dags test timed out"}
    if proc.returncode == 0:
        return {"passed": False, "detail": f"{dag_id}: run finished green, ticket requires a failed run"}
    blob = (proc.stderr or "") + (proc.stdout or "")
    # The old heuristic — Traceback plus DagBag anywhere in the output — read
    # EVERY genuine failure as an import failure: `dags test` always logs
    # "Filling up the DagBag" and any failed task prints a traceback, so this
    # check could never pass. Nothing shipped used dag_fails, so nothing
    # reached it. A DAG that truly does not import exits with Airflow's
    # "could not be found; either it does not exist or it failed to parse".
    if "could not be found" in blob:
        return {"passed": False, "detail": f"{dag_id}: failed to import rather than failing its check"}
    return {"passed": True, "detail": f"{dag_id}: surfaced as a failed run"}


def dbt_watch(env: dict) -> tuple[dict | None, str]:
    """An env whose PATH finds a `dbt` that logs the call, then execs the real one.

    Split out so the detection itself is testable: a shim that silently failed to
    fire would make every parse-time check pass.
    """
    import shutil
    import tempfile

    real = shutil.which("dbt", path=env.get("PATH", os.environ.get("PATH", "")))
    if not real:
        return None, ""

    shim_dir = tempfile.mkdtemp(prefix="dbt-shim-")
    marker = os.path.join(shim_dir, "invoked.log")
    shim = os.path.join(shim_dir, "dbt")
    with open(shim, "w", encoding="utf-8") as f:
        f.write(f'#!/bin/sh\necho "$@" >> "{marker}"\nexec "{real}" "$@"\n')
    os.chmod(shim, 0o755)

    watched = dict(env)
    watched["PATH"] = shim_dir + os.pathsep + watched.get("PATH", os.environ.get("PATH", ""))
    return watched, marker


def no_dbt_at_parse_check(workdir: str, env: dict) -> dict:
    """Import the DagBag with a `dbt` shim ahead of the real one on PATH.

    The ticket is about parse-time cost: the DAG processor re-reads every file
    on a short cycle, so a render that shells out to dbt each time is the bug.
    Timing the import would be flaky on shared hardware, so this observes the
    call itself. The shim records the invocation and then execs the real dbt,
    leaving behaviour unchanged — the check only watches.
    """
    watched, marker = dbt_watch(env)
    if watched is None:
        return {"passed": None, "detail": "no dbt on PATH to watch for"}
    code = _dagbag_code(workdir)
    try:
        _run(["python", "-c", code], watched, 600, workdir)
    except subprocess.TimeoutExpired:
        return {"passed": False, "detail": "importing the DagBag timed out — still doing real work at parse"}

    if os.path.exists(marker):
        with open(marker, encoding="utf-8") as f:
            calls = [line.strip() for line in f if line.strip()]
        return {
            "passed": False,
            "detail": f"importing invoked dbt {len(calls)} time(s): {calls[0][:80] if calls else ''}",
        }
    return {"passed": True, "detail": "importing the DagBag never invoked dbt"}


def no_dag_check(workdir: str, dag_id: str, env: dict) -> dict:
    """The ticket asks for something this Airflow cannot do; building it anyway is
    the wrong answer, so the absence of the DAG is what passes."""
    dags = list_dags(workdir, env)
    if dag_id in dags:
        return {"passed": False, "detail": f"{dag_id} was built, but the ticket's premise is false"}
    return {"passed": True, "detail": f"{dag_id} correctly not built"}


def _duck(sql: str, workdir: str, ds: str = LOGICAL_DATE) -> list[tuple]:
    """Run SQL in a throwaway in-memory DuckDB. `{workdir}` and `{ds}` interpolate."""
    import duckdb

    con = duckdb.connect()
    try:
        return con.execute(sql.replace("{workdir}", workdir).replace("{ds}", ds)).fetchall()
    finally:
        con.close()


def _normalise(rows: list[tuple]) -> list[tuple]:
    """Compare on value, not on how DuckDB or a CSV writer spelled it."""
    out = []
    for row in rows:
        cells = []
        for cell in row:
            if cell is None:
                cells.append(None)
                continue
            try:
                cells.append(round(float(cell), 4))
            except (TypeError, ValueError):
                cells.append(str(cell).strip())
        out.append(tuple(cells))
    return sorted(out, key=lambda r: [(c is None, str(c)) for c in r])


def _compare(actual: list[tuple], expected: list[tuple], label: str) -> dict:
    a, e = _normalise(actual), _normalise(expected)
    if a == e:
        return {"passed": True, "detail": f"{label}: {len(e)} row(s) match the fixtures"}
    missing = [r for r in e if r not in a][:3]
    extra = [r for r in a if r not in e][:3]
    bits = []
    if len(a) != len(e):
        bits.append(f"{len(a)} row(s), expected {len(e)}")
    if missing:
        bits.append(f"missing {missing}")
    if extra:
        bits.append(f"unexpected {extra}")
    return {"passed": False, "detail": f"{label}: " + "; ".join(bits)}


def partition_csv_check(workdir: str, spec: dict, ds: str = LOGICAL_DATE) -> dict:
    """A CSV partition at include/data/<layer>/<name>_<ds>.csv, as the ticket named it."""
    layer, name = spec["layer"], spec["name"]
    path = os.path.join(workdir, "include", "data", layer, f"{name}_{ds}.csv")
    label = f"{layer}/{name}_{ds}.csv"
    if not os.path.exists(path):
        return {"passed": False, "detail": f"{label}: not written"}
    try:
        with open(path, encoding="utf-8") as f:
            header = (f.readline() or "").strip()
    except OSError as exc:
        return {"passed": False, "detail": f"{label}: unreadable ({exc})"}

    want = spec.get("columns")
    if want:
        got = [c.strip() for c in header.split(",") if c.strip()]
        if got != list(want):
            return {"passed": False, "detail": f"{label}: header {got}, expected {list(want)}"}
    if not spec.get("expect_sql"):
        return {"passed": True, "detail": f"{label}: present with the expected header"}
    try:
        actual = _duck(f"SELECT * FROM read_csv('{path}', header=true, AUTO_DETECT=true)", workdir, ds)
        expected = _duck(spec["expect_sql"], workdir, ds)
    except Exception as exc:  # noqa: BLE001 - a bad expectation must not crash the run
        return {"passed": False, "detail": f"{label}: comparison failed ({type(exc).__name__}: {exc})"}
    return _compare(actual, expected, label)


def warehouse_table_check(workdir: str, spec: dict, ds: str = LOGICAL_DATE) -> dict:
    """A table in the DuckDB warehouse, optionally compared against the fixtures."""
    import duckdb

    table = spec["table"]
    path = os.path.join(workdir, "include", "data", "lodestone.duckdb")
    if not os.path.exists(path):
        return {"passed": False, "detail": f"{table}: no warehouse at include/data"}
    try:
        con = duckdb.connect(path, read_only=True)
    except Exception as exc:  # noqa: BLE001
        return {"passed": False, "detail": f"{table}: warehouse unreadable ({type(exc).__name__})"}
    try:
        schema, _, bare = table.partition(".")
        found = con.execute(
            "select count(*) from information_schema.tables where table_schema=? and table_name=?",
            [schema, bare],
        ).fetchone()[0]
        if not found:
            return {"passed": False, "detail": f"{table}: table does not exist"}
        want = spec.get("columns")
        if want:
            cols = [
                r[0] for r in con.execute(
                    "select column_name from information_schema.columns "
                    "where table_schema=? and table_name=? order by ordinal_position",
                    [schema, bare],
                ).fetchall()
            ]
            if [c for c in cols if c in want] != list(want) or not set(want) <= set(cols):
                return {"passed": False, "detail": f"{table}: columns {cols}, expected {list(want)}"}
        if not spec.get("expect_sql"):
            return {"passed": True, "detail": f"{table}: present with the expected columns"}
        select = spec.get("select_sql", f"SELECT * FROM {table}").replace("{ds}", ds)
        actual = con.execute(select).fetchall()
        # The expected-result query runs on the warehouse connection too, so it can hold a
        # produced table to the fixtures or to another table in the same file.
        expected = con.execute(
            spec["expect_sql"].replace("{workdir}", workdir).replace("{ds}", ds)
        ).fetchall()
    except Exception as exc:  # noqa: BLE001
        return {"passed": False, "detail": f"{table}: query failed ({type(exc).__name__}: {exc})"}
    finally:
        con.close()
    return _compare(actual, expected, table)


def no_warehouse_table_check(workdir: str, spec: dict) -> dict:
    """A table the ticket says must NOT be built.

    The half of "build only these" that existence checks cannot express: a
    selector that quietly builds the whole project passes every positive check.
    """
    import duckdb

    table = spec["table"]
    path = os.path.join(workdir, "include", "data", "lodestone.duckdb")
    if not os.path.exists(path):
        return {"passed": True, "detail": f"{table}: no warehouse, so nothing was built"}
    try:
        con = duckdb.connect(path, read_only=True)
    except Exception as exc:  # noqa: BLE001
        return {"passed": False, "detail": f"{table}: warehouse unreadable ({type(exc).__name__})"}
    try:
        schema, _, bare = table.partition(".")
        found = con.execute(
            "select count(*) from information_schema.tables where table_schema=? and table_name=?",
            [schema, bare],
        ).fetchone()[0]
    finally:
        con.close()
    if found:
        return {"passed": False, "detail": f"{table}: built, but is outside the set the ticket allows"}
    return {"passed": True, "detail": f"{table}: correctly not built"}


def file_contains_check(workdir: str, spec: dict) -> dict:
    """A written reply that addresses the point — the only signal a pushback task has.

    Prose checks that ship `judge_facts` (from the task's judge_facts.yaml)
    are graded by the prose judge, which reads substance from the authored
    fact key; three trace audits showed the regexes convicting correct
    answers on phrasing and the judge closes that class. Checks without a
    fact key keep the regex path: it asks whether the words are there, not
    whether the argument is good.
    """
    import re

    rel = spec["path"]
    path = os.path.join(workdir, rel)
    if not os.path.exists(path):
        return {"passed": False, "detail": f"{rel}: not written"}
    try:
        text = open(path, encoding="utf-8", errors="replace").read()
    except OSError as exc:
        return {"passed": False, "detail": f"{rel}: unreadable ({exc})"}
    if spec.get("judge_facts"):
        from de_bench.judge import judge_check

        try:
            verdict = judge_check(spec["judge_facts"], text)
        except Exception as exc:  # noqa: BLE001 - visible, never a silent regex fallback
            return {"passed": False, "detail": f"{rel}: judge failed ({type(exc).__name__}: {exc})"}
        verdict["detail"] = f"{rel}: {len(text)} chars, {verdict['detail']}"
        return verdict
    # Code files grade code, mirroring file_lacks_check below: a check for a
    # value the code must carry should not be satisfied by a comment naming
    # it. Prose targets (RESPONSE.md, .sql) are read raw as before.
    if rel.endswith(".py"):
        text = stripped_python(text)
    elif rel.endswith((".yml", ".yaml")):
        text = stripped_yaml(text)
    patterns = spec.get("matches") or []
    missing = [p for p in patterns if not re.search(p, text, re.IGNORECASE | re.DOTALL)]
    if missing:
        return {"passed": False, "detail": f"{rel}: never addresses {missing}"}
    # Name what matched, not just that everything did — five identical "every
    # required point" strings in one score file cannot say which check
    # verified what.
    return {"passed": True, "detail": f"{rel}: {len(text)} chars, matches {patterns}"}


def dag_structure_check(workdir: str, spec: dict, env: dict) -> dict:
    """The DAG kept its shape: exact task-id set, and optionally the operator
    class each id is bound to. This is the guard against passing a migration by
    deleting the hard task or swapping it for a no-op — the run checks cannot
    tell a gutted pipeline from a working one when the fixtures are small."""
    dag_id = spec["dag_id"]
    code = (
        "import json\n"
        + _dagbag_code(workdir)
        + f"d = b.dags.get('{dag_id}')\n"
        "print(json.dumps(None if d is None else {t.task_id: type(t).__name__ for t in d.tasks}))\n"
    )
    try:
        proc = _run(["python", "-c", code], env, 300, workdir)
    except subprocess.TimeoutExpired:
        return {"passed": False, "detail": "DagBag load timed out"}
    if proc.returncode != 0:
        return {"passed": False, "detail": f"DagBag crashed: {(proc.stderr or '')[-400:]}"}
    try:
        found = json.loads(proc.stdout.strip().splitlines()[-1])
    except Exception:  # noqa: BLE001
        return {"passed": False, "detail": f"unparseable DagBag output: {proc.stdout[-200:]}"}
    if found is None:
        return {"passed": False, "detail": f"{dag_id}: not in the DagBag"}
    expected = set(spec.get("task_ids") or [])
    missing, extra = sorted(expected - set(found)), sorted(set(found) - expected)
    # `allow_extra: true` keeps the guard against deletion and no-op swaps but
    # stops convicting an agent for ADDING a task — the house style is a check
    # step per concern, and the first 60-task matrix failed two correct fixes
    # solely for carrying an extra verification task. Exact stays the default:
    # some tickets say "no new steps" in as many words, and there exact is fair.
    if spec.get("allow_extra"):
        extra = []
    if missing or extra:
        parts = [f"missing task(s): {', '.join(missing)}"] if missing else []
        parts += [f"unexpected task(s): {', '.join(extra)}"] if extra else []
        return {"passed": False, "detail": f"{dag_id}: " + "; ".join(parts)}
    wrong = {t: cls for t, cls in (spec.get("operators") or {}).items() if found.get(t) != cls}
    if wrong:
        detail = ", ".join(f"{t} is {found.get(t)}, wanted {cls}" for t, cls in sorted(wrong.items()))
        return {"passed": False, "detail": f"{dag_id}: {detail}"}
    return {"passed": True, "detail": f"{dag_id}: all {len(found)} task(s) present with the right shape"}


def stripped_yaml(text: str) -> str:
    """Blank YAML comments, keeping line geometry, for the same reason Python's are
    blanked: a check for an old idiom should not match an agent's note saying it
    removed the old idiom.

    A `#` inside a quoted scalar is not a comment, so quotes are tracked rather than
    cutting at the first `#`. Nothing else about the line is touched.
    """
    out = []
    for line in text.splitlines(keepends=True):
        quote = None
        cut = None
        for i, ch in enumerate(line):
            if quote:
                if ch == quote:
                    quote = None
            elif ch in "\"'":
                quote = ch
            elif ch == "#" and (i == 0 or line[i - 1] in " \t"):
                cut = i
                break
        out.append(line if cut is None else line[:cut] + "\n")
    return "".join(out)


def stripped_python(text: str) -> str:
    """Blank comments and docstrings, keeping line/column geometry, so a pattern
    meant for code never trips on prose about the old way. Unparseable source is
    returned as-is — grading it raw is better than grading nothing."""
    import ast
    import io
    import tokenize

    lines = text.splitlines(keepends=True)

    def blank(r1: int, c1: int, r2: int, c2: int) -> None:
        if r1 == r2:
            line = lines[r1 - 1]
            lines[r1 - 1] = line[:c1] + " " * (c2 - c1) + line[c2:]
            return
        lines[r1 - 1] = lines[r1 - 1][:c1] + "\n"
        for r in range(r1, r2 - 1):
            lines[r] = "\n"
        lines[r2 - 1] = " " * c2 + lines[r2 - 1][c2:]

    try:
        for tok in tokenize.generate_tokens(io.StringIO(text).readline):
            if tok.type == tokenize.COMMENT:
                blank(*tok.start, *tok.end)
        for node in ast.walk(ast.parse(text)):
            if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
                first = node.body[0] if node.body else None
                if (
                    isinstance(first, ast.Expr)
                    and isinstance(first.value, ast.Constant)
                    and isinstance(first.value.value, str)
                ):
                    blank(first.lineno, first.col_offset, first.end_lineno, first.end_col_offset)
    except (SyntaxError, tokenize.TokenError):
        return text
    return "".join(lines)


def file_lacks_check(workdir: str, spec: dict) -> dict:
    """The inverse of file_contains: a migration is done only when the old idiom
    is gone. `path` may be a glob. Comments and docstrings are stripped first, and
    matching is case-sensitive — old idioms are code.

    YAML is stripped as well as Python. It used to be Python only, and an agent that
    removed an `sla_miss_callback` key and left `# SLAs (\\`sla\\` / \\`sla_miss_callback\\`)
    were removed in Airflow 3.0` above it failed the check for saying what it had
    done. A comment naming the thing is the opposite of the thing still being there.

    An empty glob fails rather than skipping: deleting the file the check reads
    must not read as clean."""
    import glob
    import re

    rel = spec["path"]
    paths = sorted(glob.glob(os.path.join(workdir, rel), recursive=True))
    if not paths:
        return {"passed": False, "detail": f"{rel}: nothing to scan — file missing or glob empty"}
    hits = []
    for path in paths:
        try:
            text = open(path, encoding="utf-8", errors="replace").read()
        except OSError as exc:
            return {"passed": False, "detail": f"{os.path.relpath(path, workdir)}: unreadable ({exc})"}
        if path.endswith(".py"):
            text = stripped_python(text)
        elif path.endswith((".yml", ".yaml")):
            text = stripped_yaml(text)
        hits += [
            f"{os.path.relpath(path, workdir)}: {p}" for p in spec.get("patterns") or [] if re.search(p, text)
        ]
    if hits:
        return {"passed": False, "detail": "still present — " + "; ".join(hits[:5])}
    return {"passed": True, "detail": f"{len(paths)} file(s) clean"}


def json_matches_check(workdir: str, spec: dict) -> dict:
    """A written JSON answer equals the expected value. Exact after parsing:
    key order and whitespace are free, list order and spelling are not — the
    prompt states the format, so format is part of the answer."""
    rel = spec["path"]
    path = os.path.join(workdir, rel)
    if not os.path.exists(path):
        return {"passed": False, "detail": f"{rel}: not written"}
    try:
        actual = json.loads(open(path, encoding="utf-8", errors="replace").read())
    except (OSError, ValueError) as exc:
        return {"passed": False, "detail": f"{rel}: not valid JSON ({exc})"}
    expected = spec.get("equals")
    if actual != expected:
        return {"passed": False, "detail": f"{rel}: got {json.dumps(actual)[:200]}, wanted {json.dumps(expected)[:200]}"}
    return {"passed": True, "detail": f"{rel}: matches"}


def pytest_verifier_check(workdir: str, spec: dict, env: dict, verifier_dir: str) -> dict:
    """A vendored pytest decides the task: every test must pass, none may skip.

    `prepare` commands run first in the dbt project directory with failures
    tolerated — the verifiers were written for a container where the agent's
    own dbt runs had already materialized the warehouse, and scoring rebuilds
    that state from the patched files instead. A prepare failure is usually
    the finding itself (the model does not build), and the tests say so with
    better blame than a hard stop here would.

    A skip fails rather than dropping out — these verifiers skip a component
    when its subject is missing, so a skip usually means ungraded work. The
    exception is a skip the task declares in `allow_skips`: a few source
    verifiers guard a test on warehouse state the shipped world never has,
    and their own runners pass on the skip.
    """
    import re

    file = spec.get("file") or "test_outputs.py"
    test_path = os.path.join(verifier_dir, file)
    if not os.path.exists(test_path):
        return {"passed": False, "detail": f"verifier {file} not shipped"}
    project_dir = env.get("DBT_PROJECT_DIR_DUCKDB") or workdir
    prepare_failed: list[str] = []
    for cmd in spec.get("prepare") or []:
        progress(f"prepare: {cmd}")
        try:
            proc = subprocess.run(
                cmd, shell=True, env=env, cwd=project_dir,
                capture_output=True, text=True, timeout=int(spec.get("prepare_timeout", 1200)), check=False,
            )
            if proc.returncode != 0:
                prepare_failed.append(cmd)
                progress(f"prepare failed (tolerated): {cmd}: {(proc.stderr or proc.stdout or '')[-300:]}")
        except subprocess.TimeoutExpired:
            prepare_failed.append(f"{cmd} (timeout)")
            progress(f"prepare timed out (tolerated): {cmd}")

    progress(f"pytest {file}")
    try:
        proc = _run(["python", "-m", "pytest", test_path, "-v", "--tb=line"], env,
                    int(spec.get("timeout", 2400)), project_dir)
    except subprocess.TimeoutExpired:
        return {"passed": False, "detail": f"{file}: pytest timed out"}
    blob = (proc.stdout or "") + (proc.stderr or "")

    def count(word: str) -> int:
        hits = re.findall(rf"(\d+) {word}", blob)
        return int(hits[-1]) if hits else 0

    passed_n, failed_n, skipped_n = count("passed"), count("failed"), count("skipped")
    errors_n = count(r"errors?\b")
    failed_names = [l.split("::", 1)[-1].split(" ")[0] for l in blob.splitlines() if l.startswith("FAILED ")]
    allowed = set(spec.get("allow_skips") or ())
    skipped_names = [l.split("::", 1)[-1].split(" ")[0] for l in blob.splitlines() if " SKIPPED" in l]
    unexpected_skips = [n for n in skipped_names if n not in allowed]
    # skipped_n must equal the names parsed from -v lines, or an unparsed skip
    # could ride through as allowed — fail closed on a format change.
    skips_ok = not unexpected_skips and skipped_n == len(skipped_names)
    ok = proc.returncode == 0 and passed_n > 0 and failed_n == 0 and errors_n == 0 and skips_ok
    detail = f"{passed_n} passed, {failed_n} failed, {skipped_n} skipped"
    if skipped_names and skips_ok:
        detail += f" (allowed: {', '.join(skipped_names[:3])})"
    if prepare_failed:
        # A prepare failure is often the finding (the model doesn't build),
        # but from the tally alone it was indistinguishable from a scorer
        # environment fault — name it.
        detail += f" [prepare failed: {'; '.join(prepare_failed[:3])}]"
    if errors_n:
        detail += f", {errors_n} errored"
    if not ok:
        if failed_names:
            detail += ": " + ", ".join(failed_names[:5])
        error_names = [l.split("::", 1)[-1].split(" ")[0] for l in blob.splitlines() if l.startswith("ERROR ")]
        if error_names:
            detail += "; errored: " + ", ".join(error_names[:4])
            # An errored test was undiagnosable from the score file — the
            # names say which fixture died, never why, and a trace audit
            # burned hours reconstructing causes the blob already held. Keep
            # the first exception lines from the ERRORS section.
            in_errors = False
            causes: list[str] = []
            for line in blob.splitlines():
                if re.match(r"=+ ERRORS =+", line):
                    in_errors = True
                    continue
                if in_errors and re.match(r"=+ ", line):
                    break
                if in_errors and (line.startswith("E ") or re.search(r"\.py:\d+: \w*(Error|Exception|assert)", line)):
                    causes.append(line.strip()[:200])
            if causes:
                detail += " | " + " | ".join(causes[:2])
        # These verifiers aggregate components inside one test and print a
        # "FAIL: <component>: <why>" line per miss — the only blame there is.
        components = [l.strip() for l in blob.splitlines() if l.strip().startswith("FAIL: ")]
        if components:
            detail += " | " + " | ".join(c[:160] for c in components[:4])
        elif passed_n + failed_n + skipped_n + errors_n == 0:
            # Only when pytest truly ran nothing — "0 passed" alone used to
            # print this and read as harness breakage when it was the agent's
            # model failing every test.
            detail += f"; no tests ran (exit {proc.returncode}): {blob[-400:]}"
    return {"passed": ok, "detail": detail}


def file_absent_check(workdir: str, spec: dict) -> dict:
    hits = []
    for rel in spec.get("paths") or [spec.get("path")]:
        if rel and os.path.exists(os.path.join(workdir, rel)):
            hits.append(rel)
    if hits:
        return {"passed": False, "detail": f"should not exist: {', '.join(hits)}"}
    return {"passed": True, "detail": "no leftover artefacts"}


def scoring_primitive_check(workdir: str, spec: dict, oracle_dir: str | None = None) -> dict:
    """Run one airflow-bench scoring primitive (vendored in
    `de_bench.upgrade_v2_scoring`) against the patched workspace.

    Static analysis only (AST/regex/YAML on files under `workdir`), no live
    Airflow needed — this is why it doesn't gate on `outputs_ready` the way
    the DAG-execution output checks do. `oracle_dir`, when given, is the
    task's own directory: `oracle_dir/solution/...` is the known-good fix,
    `oracle_dir/task.yaml` carries `target.airflow` etc. A task with no
    solution/ (oracle_dir=None) makes oracle-aware primitives
    (`structural_match_taskids` and the like) report `passed=None`, which
    drops out of the weighted total like any other not-applicable check.
    """
    from de_bench.upgrade_v2_scoring import primitives  # noqa: F401  (populates the registry)
    from de_bench.upgrade_v2_scoring.registry import PrimitiveOutcome, get_primitive

    name = spec["name"]
    try:
        primitive = get_primitive(name)
    except KeyError:
        return {"passed": False, "detail": f"unknown scoring primitive: {name!r}"}
    outcome: PrimitiveOutcome = primitive(workdir, "", oracle_dir=oracle_dir)
    detail = f"value={outcome.value:.2f}" if outcome.passed is not None else "not applicable (no oracle)"
    return {"passed": outcome.passed, "detail": detail}


def warehouse_state(workdir: str) -> dict | None:
    """Order-independent content fingerprint of every DuckDB file in the tree.

    Byte-hashing the files would false-fail on WAL/timestamp noise; instead sum
    per-row hashes per table. `hash(t)` hashes the row as a struct — the older
    `hash(t.*)` spelling stopped binding somewhere before DuckDB 1.5, and the
    resulting BinderException collapsed every file to one "unreadable" marker
    that compared equal across the replay, so the idempotent check could not
    see warehouse drift at all. Failures are recorded per table, not per file,
    so one unhashable table cannot hide drift in the others.
    """
    import duckdb

    state: dict = {}
    for root, _, files in os.walk(workdir):
        for f in files:
            if not f.endswith(".duckdb"):
                continue
            path = os.path.join(root, f)
            rel = os.path.relpath(path, workdir)
            try:
                con = duckdb.connect(path, read_only=True)
                tables = con.execute(
                    "select table_schema, table_name from information_schema.tables where table_type='BASE TABLE'"
                ).fetchall()
            except Exception as exc:  # noqa: BLE001 - a locked/corrupt file is a finding, not a crash
                state[rel] = f"unreadable: {type(exc).__name__}"
                continue
            for schema, table in tables:
                key = f"{rel}:{schema}.{table}"
                try:
                    row = con.execute(
                        f'select count(*), coalesce(sum(hash(t)), 0) from "{schema}"."{table}" t'
                    ).fetchone()
                    state[key] = list(row)
                except Exception as exc:  # noqa: BLE001
                    state[key] = f"unhashable: {type(exc).__name__}"
            con.close()

    # Most pipelines here publish CSV partitions rather than warehouse tables, so
    # hashing only DuckDB would leave replay untested for the majority of tasks.
    import hashlib

    data_dir = os.path.join(workdir, "include", "data")
    for root, _, files in os.walk(data_dir):
        for f in sorted(files):
            if not f.endswith(".csv"):
                continue
            path = os.path.join(root, f)
            rel = os.path.relpath(path, workdir)
            try:
                with open(path, "rb") as fh:
                    state[rel] = hashlib.sha256(fh.read()).hexdigest()[:16]
            except OSError as exc:
                state[rel] = f"unreadable: {type(exc).__name__}"
    return state or None


# ----------------------------------------------------------------------------
# Differential grading: the untouched world is the expected result.
#
# Every oracle failure this benchmark has audited was a hand-typed expected
# value being wrong. A unchanged check types none: the scorer runs the
# original tree and the patched tree in one container, dumps the graded
# relations from both, and compares. What the world produces before the patch is
# what it must produce after.
#
# The two arms must differ only by the patch, so the caller runs the original
# arm on the freshly laid tree — same tar, same extras, same environment — and
# hands the snapshot back here as a directory.

# A column whose value can move on its own defeats the whole mechanism: the two
# arms would differ for reasons the agent had nothing to do with. Reject those
# names rather than grade noise; `allow_columns` is the author's statement that
# this one is pinned after all.
NONDETERMINISTIC_COLUMN = re.compile(r"(_at$|uuid|random)", re.IGNORECASE)

_SNAPSHOT_FILE = "unchanged.json"
_SAMPLE_ROWS = 3


def warehouse_db(workdir: str, spec: dict | None = None) -> str:
    """The DuckDB file a unchanged check reads.

    Workspace-relative on purpose. Both arms run in the same container against
    the same path, and only a warehouse inside the tree is restored when the
    tree is laid again between the arms — a warehouse elsewhere would carry the
    original arm's writes into the patched one. A world that keeps its warehouse
    somewhere else says so with `db:` and gets a loud "no warehouse" failure if
    the path is wrong, never a quiet comparison of two stale dumps.
    """
    rel = (spec or {}).get("db") or os.path.join("include", "data", "lodestone.duckdb")
    return rel if os.path.isabs(rel) else os.path.join(workdir, rel)


def relation_snapshot(db_path: str, relation: str) -> dict:
    """One relation, canonicalized: columns by sorted name, rows ordered by all
    columns, numbers through the same 4dp semantics every other check compares
    on. Returns `{"error": ...}` rather than raising — a missing relation is a
    finding about the patch, and it has to reach the report."""
    import duckdb

    if not os.path.exists(db_path):
        return {"error": f"no warehouse at {db_path}"}
    schema, _, bare = relation.partition(".")
    if not bare:
        return {"error": f"{relation}: name a relation as <schema>.<table>"}
    try:
        con = duckdb.connect(db_path, read_only=True)
    except Exception as exc:  # noqa: BLE001 - a locked or corrupt file is a finding
        return {"error": f"warehouse unreadable ({type(exc).__name__}: {exc})"}
    try:
        columns = [
            r[0]
            for r in con.execute(
                "select column_name from information_schema.columns "
                "where table_schema=? and table_name=? order by column_name",
                [schema, bare],
            ).fetchall()
        ]
        if not columns:
            return {"error": "relation does not exist"}
        select = ", ".join(f'"{c}"' for c in columns)
        rows = con.execute(f'select {select} from "{schema}"."{bare}"').fetchall()
    except Exception as exc:  # noqa: BLE001
        return {"error": f"read failed ({type(exc).__name__}: {exc})"}
    finally:
        con.close()
    # _normalise rounds and sorts, so the dump is already the canonical form;
    # lists rather than tuples so it survives the trip through JSON unchanged.
    return {"columns": columns, "rows": [list(r) for r in _normalise(rows)], "count": len(rows)}


def unchanged_plan(checks: list[dict]) -> dict[str, dict]:
    """What each arm has to do, grouped by logical date: the DAGs to run and the
    relations to dump. Grouping by date keeps one run serving every check graded
    on it, and sorting keeps both arms in the same order."""
    plan: dict[str, dict] = {}
    for c in checks:
        if c.get("kind") != "unchanged":
            continue
        ds = str(c.get("ds") or LOGICAL_DATE)
        entry = plan.setdefault(ds, {"relations": [], "run": [], "db": None})
        for rel in c.get("relations") or ():
            if rel not in entry["relations"]:
                entry["relations"].append(str(rel))
        for dag_id in c.get("run") or ():
            if dag_id not in entry["run"]:
                entry["run"].append(str(dag_id))
        if c.get("db") and not entry["db"]:
            entry["db"] = str(c["db"])
    for entry in plan.values():
        entry["relations"].sort()
    return plan


def capture_relations(workdir: str, plan: dict[str, dict], env: dict,
                         baseline_dags: list[str] | None = None,
                         replay_dags: list[str] | None = None,
                         out_dir: str | None = None, arm: str = "patched") -> dict:
    """Run one arm and dump its graded relations.

    Both arms call this, with the same plan and the same seeding, so the only
    thing that can move a value is the patch. `run` names the DAGs to execute;
    with none named the world's own seed DAGs are the pipeline, which is what
    builds the graded relations for a task that changes a model rather than a
    DAG. A run that exits non-zero is recorded, not raised — the dump that
    follows is the evidence, and an empty relation says more than a stack trace.
    """
    snapshot: dict = {}
    reserialized = False
    for ds in sorted(plan):
        entry = plan[ds]
        run_ids = list(entry.get("run") or ())
        to_replay = list(baseline_dags or ()) if replay_dags is None else list(replay_dags)
        if (run_ids or to_replay) and not reserialized:
            # Same reason the execute stage does it: Airflow 3.0.0 reads the
            # serialized-DAG table and a fresh metadata db has none.
            try:
                _run(["airflow", "dags", "reserialize"], env, 300, workdir)
            except subprocess.TimeoutExpired:
                pass
            reserialized = True
        replay_world(workdir, to_replay, run_ids, env, ds)
        notes = []
        for dag_id in run_ids:
            progress(f"unchanged {arm} arm: run {dag_id} at {ds}")
            try:
                proc = _run(["airflow", "dags", "test", dag_id, ds], env, DAGS_TEST_TIMEOUT, workdir)
                if proc.returncode != 0:
                    notes.append(f"{dag_id} exited {proc.returncode}")
            except subprocess.TimeoutExpired:
                notes.append(f"{dag_id} timed out")
        db = warehouse_db(workdir, entry)
        relations = {rel: relation_snapshot(db, rel) for rel in entry["relations"]}
        snapshot[ds] = {"relations": relations, "notes": notes}
        progress(f"unchanged {arm} arm: dumped {len(relations)} relation(s) at {ds}")
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
        with open(os.path.join(out_dir, _SNAPSHOT_FILE), "w", encoding="utf-8") as f:
            json.dump(snapshot, f)
    return snapshot


def read_snapshot(out_dir: str | None) -> dict | None:
    """The snapshot an earlier arm wrote, or None when there is none."""
    if not out_dir:
        return None
    path = os.path.join(out_dir, _SNAPSHOT_FILE)
    if not os.path.exists(path):
        return None
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError):
        return None


def _rows(dump: dict) -> list[tuple]:
    return [tuple(r) for r in dump.get("rows") or ()]


def _ordered(rows) -> list[tuple]:
    """Rows in the one order the whole scorer uses — NULLs last, then by text."""
    return sorted(rows, key=lambda r: [(c is None, str(c)) for c in r])


def _sample(rows: list[tuple]) -> str:
    shown = ", ".join(str(list(r))[:120] for r in rows[:_SAMPLE_ROWS])
    return shown + (f" (+{len(rows) - _SAMPLE_ROWS} more)" if len(rows) > _SAMPLE_ROWS else "")


def _relation_verdict(relation: str, a: dict | None, b: dict | None, allow: set[str]) -> str | None:
    """None when the two arms agree; otherwise why they do not.

    `a` is the original arm, `b` the patched one.
    """
    if a is None or b is None:
        side = "original" if a is None else "patched"
        return f"{relation}: the {side} arm never dumped it"
    if a.get("error"):
        return f"{relation}: original arm: {a['error']}"
    if b.get("error"):
        return f"{relation}: patched arm: {b['error']}"

    columns = list(a.get("columns") or ()) + [c for c in (b.get("columns") or ()) if c not in (a.get("columns") or ())]
    loose = [c for c in columns if NONDETERMINISTIC_COLUMN.search(c) and c not in allow]
    if loose:
        return (
            f"{relation}: column(s) {', '.join(sorted(loose))} can move on their own — "
            "pin them or name them in allow_columns"
        )
    if list(a.get("columns") or ()) != list(b.get("columns") or ()):
        return f"{relation}: columns {b.get('columns')}, the untouched world produces {a.get('columns')}"

    rows_a, rows_b = _rows(a), _rows(b)
    # EXCEPT both ways, then the counts. EXCEPT alone dedupes, so a row emitted
    # twice where the original world emits it once passes the set test — the
    # count is what catches it.
    only_original = _ordered(set(rows_a) - set(rows_b))
    only_patched = _ordered(set(rows_b) - set(rows_a))
    bits = []
    if only_original:
        bits.append(f"missing {_sample(only_original)}")
    if only_patched:
        bits.append(f"unexpected {_sample(only_patched)}")
    if len(rows_a) != len(rows_b):
        bits.append(f"{len(rows_b)} row(s), the untouched world produces {len(rows_a)}")
    return f"{relation}: " + "; ".join(bits) if bits else None


def unchanged_verdict(check: dict, original: dict | None, patched: dict) -> dict:
    """Compare one check's relations across the two arms.

    A missing original snapshot fails rather than skips: the whole point of this
    kind is that nobody typed an expected value, so there is nothing to fall back
    on, and a silent skip would read as agreement.
    """
    ds = str(check.get("ds") or LOGICAL_DATE)
    relations = [str(r) for r in (check.get("relations") or ())]
    if not relations:
        return {"passed": False, "detail": "unchanged check names no relations"}
    if not original:
        return {"passed": False, "detail": "no original snapshot: the untouched tree was never run"}
    allow = {str(c) for c in (check.get("allow_columns") or ())}
    at_a = (original.get(ds) or {}).get("relations") or {}
    at_b = (patched.get(ds) or {}).get("relations") or {}
    faults = [f for f in (_relation_verdict(r, at_a.get(r), at_b.get(r), allow) for r in relations) if f]
    notes = list((patched.get(ds) or {}).get("notes") or ()) + list((original.get(ds) or {}).get("notes") or ())
    if not faults:
        counts = ", ".join(f"{r} {at_a[r].get('count')}" for r in relations if isinstance(at_a.get(r), dict))
        return {"passed": True, "detail": f"{len(relations)} relation(s) identical to the untouched world at {ds}: {counts}"}
    detail = f"at {ds}: " + " | ".join(faults[:_SAMPLE_ROWS])
    if len(faults) > _SAMPLE_ROWS:
        detail += f" | (+{len(faults) - _SAMPLE_ROWS} more relation(s))"
    if notes:
        detail += f" [runs: {'; '.join(notes[:3])}]"
    return {"passed": False, "detail": detail}


def _label(check: dict, seen: dict[str, int]) -> str:
    """A stable, readable key per check — the report blames the first that failed."""
    kind = check.get("kind", "unknown")
    detail = check.get("dag_id") or check.get("table") or check.get("path")
    if not detail and check.get("name"):
        detail = f"{check.get('layer', '')}/{check['name']}".strip("/")
    if not detail and check.get("relations"):
        detail = ",".join(str(r) for r in check["relations"][:2])
    base = f"{kind}:{detail}" if detail else kind
    seen[base] = seen.get(base, 0) + 1
    return base if seen[base] == 1 else f"{base}#{seen[base]}"


def build_score_env(workdir: str, base_env: dict | None = None) -> dict:
    """The environment every check runs under. The dags root joins the tree
    root on PYTHONPATH because Airflow 3's dag processor appends the bundle
    root to sys.path (dag_processing/processor.py) — a sibling import inside
    dags/ is production-legal, and grading without it failed correct
    migrations."""
    env = dict(base_env if base_env is not None else os.environ)
    env["AIRFLOW__CORE__DAGS_FOLDER"] = dags_root(workdir)
    # Pin the warehouse into the scored tree (the world's default assumes /work).
    env["AIRFLOW_VAR_WAREHOUSE_PATH"] = f"{workdir}/include/data/lodestone.duckdb"
    pp = f"{workdir}{os.pathsep}{dags_root(workdir)}"
    # Airflow's settings.py also appends PLUGINS_FOLDER to sys.path, so a bare
    # import of a plugins/ module is production-legal too. Point the folder at
    # the scored tree and mirror it on PYTHONPATH for non-Airflow probes.
    plugins = os.path.join(workdir, "plugins")
    if os.path.isdir(plugins):
        env["AIRFLOW__CORE__PLUGINS_FOLDER"] = plugins
        pp += os.pathsep + plugins
    env["PYTHONPATH"] = pp + (os.pathsep + env["PYTHONPATH"] if env.get("PYTHONPATH") else "")
    env.setdefault("HOME", "/root")
    return env


def score_workspace(workdir: str, checks: list[dict], protected: list[str], patch_text: str,
                    baseline_dags: list[str] | None = None, replay_dags: list[str] | None = None,
                    base_env: dict | None = None, env_note: dict | None = None,
                    verifier_dir: str | None = None,
                    history: list | None = None,
                    original_dir: str | None = None,
                    oracle_dir: str | None = None) -> dict:
    """Run a task's declared checks against an already-rebuilt workspace.

    Stages run in order and a stage is skipped (None, not False) when the one
    before already condemned the workspace — a report about output written by a
    DAG that never imported would be noise.

    base_env replaces os.environ as the substrate (live-env tasks score inside
    the venv built from the patched requirements). env_note, when given, lands
    in the results as `env_builds`; a failed build skips every runtime check —
    there is no interpreter to run them on — and vetoes the pass.

    original_dir holds the dump the caller took from the untouched tree, before
    the patch landed and in this same container. Only unchanged checks read
    it; a task with none takes exactly the path it took before.

    oracle_dir, when the task ships one, is the task's own directory
    (solution/ + task.yaml), extracted outside workdir so the agent under
    test never sees it. Only scoring_primitive checks read it.
    """
    env = build_score_env(workdir, base_env)

    # A diagnosis task's evidence is run history. Give it a metadata database of
    # its own — the container default may carry runs from an earlier trial in a
    # warm container — and write the seeded history the trial had.
    if history:
        import tempfile

        env["AIRFLOW_HOME"] = tempfile.mkdtemp(prefix="af-home-")
        _run(["airflow", "db", "migrate"], env, 300, workdir)

    by_kind: dict[str, list[dict]] = {}
    for c in checks:
        by_kind.setdefault(c.get("kind", "unknown"), []).append(c)
    parse_ids = [c["dag_id"] for c in by_kind.get("dag_parses", []) if c.get("dag_id")]
    exec_ids = [c["dag_id"] for c in by_kind.get("dag_runs", []) + by_kind.get("dag_fails", []) if c.get("dag_id")]
    idem_ids = [c["dag_id"] for c in by_kind.get("idempotent", []) if c.get("dag_id")]

    results: dict = {}
    seen: dict[str, int] = {}
    results["delivered"] = (
        {"passed": True, "detail": f"{len(patch_text)} byte patch"}
        if patch_text.strip()
        else {"passed": False, "detail": "empty patch"}
    )
    results["do_not_modify"] = do_not_modify_check(patch_text, protected)
    delivered = bool(results["delivered"]["passed"])
    if env_note is not None:
        results["env_builds"] = env_note
    env_ok = env_note is None or bool(env_note["passed"])
    runnable = delivered and env_ok
    blocked = "environment did not build" if delivered else "nothing delivered"

    def skip(check: dict, why: str) -> None:
        results[_label(check, seen)] = {"passed": None, "detail": why}

    # Parse stage.
    parses = None
    if parse_ids:
        verdict = parses_check(workdir, parse_ids, env) if runnable else {"passed": None, "detail": blocked}
        parses = verdict["passed"]
        for c in by_kind.get("dag_parses", []):
            results[_label(c, seen)] = verdict
    for c in by_kind.get("no_dbt_at_parse", []):
        results[_label(c, seen)] = (
            no_dbt_at_parse_check(workdir, env) if runnable
            else {"passed": None, "detail": blocked}
        )
    for c in by_kind.get("no_dag", []):
        results[_label(c, seen)] = (
            no_dag_check(workdir, c["dag_id"], env) if runnable
            else {"passed": None, "detail": blocked}
        )
    for c in by_kind.get("dag_structure", []):
        if not runnable:
            skip(c, blocked)
        elif parses is False:
            skip(c, "did not parse")
        else:
            results[_label(c, seen)] = dag_structure_check(workdir, c, env)

    # Differential stage — the patched arm of a comparison whose other arm the
    # caller already ran on the untouched tree. It goes here, before the execute
    # stage, so the patched arm meets the same tree the original arm met: laid
    # from the tar, extras prepared, nothing else run against it yet.
    diff_checks = by_kind.get("unchanged", [])
    if diff_checks:
        if not runnable or parses is False:
            for c in diff_checks:
                skip(c, blocked if not runnable else "did not parse")
        else:
            patched_snapshot = capture_relations(
                workdir, unchanged_plan(diff_checks), env,
                baseline_dags=baseline_dags, replay_dags=replay_dags, arm="patched",
            )
            original_snapshot = read_snapshot(original_dir)
            for c in diff_checks:
                results[_label(c, seen)] = unchanged_verdict(c, original_snapshot, patched_snapshot)

    # Execute stage — seed the world's own pipeline first so the focus DAG finds
    # the tables it reads, exactly as it would have during the trial. A task
    # graded on the date its ticket worked through gets that date seeded.
    ran_ok = None
    execute = by_kind.get("dag_runs", []) + by_kind.get("dag_fails", [])
    if execute:
        if runnable and parses is not False:
            ran_ok = True
            # Airflow 3.0.0's `dags test` reads the serialized-DAG table and
            # errors "not found in DagBag read from database" on a fresh db —
            # 31 trials failed for pinning a fair reading of "3.0". Later
            # versions parse from disk; reserializing first is harmless there.
            try:
                _run(["airflow", "dags", "reserialize"], env, 300, workdir)
            except subprocess.TimeoutExpired:
                pass
            # Seeded history first, at each entry's own logical date — an
            # upstream run can sit at midnight while the graded DAG runs at
            # its cron time. The same writer the trial used, so the grader
            # reads the history the agent read. reserialize=False: the call
            # above just did one.
            write_history(env, workdir, history, reserialize=False)
            for ds in sorted({c.get("ds", LOGICAL_DATE) for c in execute}):
                to_replay = baseline_dags or [] if replay_dags is None else replay_dags
                replay_world(workdir, to_replay, exec_ids, env, ds)
                for c in [x for x in by_kind.get("dag_runs", []) if x.get("ds", LOGICAL_DATE) == ds]:
                    progress(f"run {c['dag_id']} at {ds}")
                    verdict = runs_check(workdir, [c["dag_id"]], env, ds,
                                         require_tasks=c.get("require_tasks"))
                    results[_label(c, seen)] = verdict
                    ran_ok = ran_ok and bool(verdict["passed"])
                for c in [x for x in by_kind.get("dag_fails", []) if x.get("ds", LOGICAL_DATE) == ds]:
                    progress(f"run {c['dag_id']} at {ds} (expecting a failed run)")
                    verdict = dag_fails_check(workdir, c["dag_id"], env, ds)
                    results[_label(c, seen)] = verdict
                    ran_ok = ran_ok and bool(verdict["passed"])
        else:
            for c in execute:
                skip(c, "did not parse" if runnable else blocked)

    # Verifier stage — a vendored pytest that builds and judges on its own.
    # Independent of the DAG stages: these tasks have no DAGs to parse.
    for c in by_kind.get("pytest_verifier", []):
        if not runnable:
            skip(c, blocked)
        elif verifier_dir is None:
            results[_label(c, seen)] = {"passed": False, "detail": "no verifier directory shipped"}
        else:
            results[_label(c, seen)] = pytest_verifier_check(workdir, c, env, verifier_dir)

    # Scoring-primitive stage — static analysis (AST/regex/YAML) vendored from
    # airflow-bench, one function per named metric. Independent of the DAG
    # stages, same reasoning as pytest_verifier: gated on `runnable` only, not
    # on `outputs_ready` — there is no DAG to run for these.
    for c in by_kind.get("scoring_primitive", []):
        if not runnable:
            skip(c, blocked)
        else:
            results[_label(c, seen)] = scoring_primitive_check(workdir, c, oracle_dir)

    # Output stage — only meaningful once the pipeline actually ran.
    output_kinds = ("partition_csv", "warehouse_table", "no_warehouse_table", "file_contains", "file_absent", "file_lacks", "json_matches")
    outputs_ready = runnable and parses is not False and ran_ok is not False
    for kind in output_kinds:
        for c in by_kind.get(kind, []):
            if not outputs_ready:
                skip(c, "pipeline did not run")
                continue
            if kind == "partition_csv":
                results[_label(c, seen)] = partition_csv_check(workdir, c, c.get("ds", LOGICAL_DATE))
            elif kind == "warehouse_table":
                results[_label(c, seen)] = warehouse_table_check(workdir, c, c.get("ds", LOGICAL_DATE))
            elif kind == "no_warehouse_table":
                results[_label(c, seen)] = no_warehouse_table_check(workdir, c)
            elif kind == "file_contains":
                results[_label(c, seen)] = file_contains_check(workdir, c)
            elif kind == "file_lacks":
                results[_label(c, seen)] = file_lacks_check(workdir, c)
            elif kind == "json_matches":
                results[_label(c, seen)] = json_matches_check(workdir, c)
            else:
                results[_label(c, seen)] = file_absent_check(workdir, c)

    # Replay stage.
    for c in by_kind.get("idempotent", []):
        if not (runnable and ran_ok):
            skip(c, "pipeline did not run")
            continue
        first = warehouse_state(workdir)
        if first is None:
            skip(c, "no output produced to compare")
            continue
        progress(f"replay {c['dag_id']} to check idempotency")
        rerun = runs_check(workdir, [c["dag_id"]], env, c.get("ds", LOGICAL_DATE))
        if not rerun["passed"] and c["dag_id"] not in [x["dag_id"] for x in by_kind.get("dag_fails", [])]:
            results[_label(c, seen)] = {"passed": False, "detail": f"second run failed: {rerun['detail']}"}
            continue
        second = warehouse_state(workdir) or {}
        drifted = [k for k in set(first) | set(second) if first.get(k) != second.get(k)]
        results[_label(c, seen)] = (
            {"passed": True, "detail": f"{len(first)} output(s) stable across re-run"}
            if not drifted
            else {"passed": False, "detail": f"state drifted: {', '.join(sorted(drifted)[:5])}"}
        )
    del idem_ids

    executed = [v["passed"] for v in results.values() if v["passed"] is not None]
    passed = bool(delivered and executed and all(executed))
    for name, r in results.items():
        mark = {True: "ok", False: "FAIL", None: "skip"}[r["passed"]]
        progress(f"  {mark:4} {name}")
    progress(f"verdict: {'PASS' if passed else 'fail'} ({len(executed)} check(s) ran)")
    return {"pass": passed, "checks": results, "tiers": results}
