"""Scoring primitives for the Upgrades category.

Each primitive walks the task directory after the agent has run and
returns a `PrimitiveOutcome(value, passed)`. `value` in [0, 1];
`passed=None` means the metric does not apply to the current task.

The migration-specific data (removed modules, deprecated kwargs,
context vars, etc.) is loaded from
`airflow_bench.scoring.data.af3_migrations.yaml` — add an entry there
when a new AF2->AF3 break is found in the KB; no Python edit needed.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

from de_bench.upgrade_v2_scoring.data import (
    af3_only_modules,
    deprecated_context_vars,
    forbidden_orm_patterns,
    forbidden_ti_attr_patterns,
    in_bundle_forbidden_pattern,
    removed_modules,
    sibling_skip_dirs,
    stepping_stone_floor,
    task_sdk_ipc_method_versions,
    task_sdk_ipc_methods,
)
from de_bench.upgrade_v2_scoring.primitives._common import read_text_capped
from de_bench.upgrade_v2_scoring.primitives._strip import strip_comments_and_docstrings
from de_bench.upgrade_v2_scoring.registry import PrimitiveOutcome, scoring_primitive

_DAGS_SUBDIR = "dags"
_REQUIREMENTS_FILE = "requirements.txt"


@scoring_primitive("no_deprecated_context_vars")
def no_deprecated_context_vars(
    task_dir: str, agent_output: str, *, oracle_dir: str | None = None
) -> PrimitiveOutcome:
    """Fraction of files under `task_dir/dags/` that reference none of
    the removed AF3 context variables. 1.0 only when all files are clean."""
    dags = Path(task_dir) / _DAGS_SUBDIR
    files = sorted(dags.rglob("*.py"))
    if not files:
        return PrimitiveOutcome(value=0.0, passed=False)
    clean = sum(1 for p in files if not _has_deprecated_context_var(p))
    value = clean / len(files)
    return PrimitiveOutcome(value=value, passed=value == 1.0)


def _has_deprecated_context_var(path: Path) -> bool:
    text = read_text_capped(path)
    if text is None:
        # Oversized or unreadable — can't vouch for it. Treat as dirty.
        return True
    # Strip comments + module/function/class docstrings before
    # scanning. The primitive previously ran a raw word-boundary
    # regex over the whole file, which flagged accurate
    # explanatory comments like
    # `# execution_date was removed in Airflow 3.0` as violations.
    # (Bench run 2026-04-26 surfaced this on proj-be18e0 — Otto's
    # correct code lost the metric because of its prose comment.)
    #
    # We deliberately keep regular string literals intact so that
    # `ctx['execution_date']` — a real usage — still matches.
    # That's a narrow false-positive surface (log messages
    # mentioning the var by name) we accept for now; the AST-based
    # alternative is heavier and the prose-in-docstring case is
    # the one that actually bit us.
    code = strip_comments_and_docstrings(text)
    if code is None:
        # Tokeniser refused (e.g. partial Python from an interrupted
        # write). Fall back to the raw scan — same behaviour as the
        # old metric, defence in depth against a stripper bug
        # silently masking real violations.
        code = text
    return any(re.search(rf"\b{re.escape(var)}\b", code) for var in deprecated_context_vars())


@scoring_primitive("siblings_no_deprecated_context_vars")
def siblings_no_deprecated_context_vars(
    task_dir: str, agent_output: str, *, oracle_dir: str | None = None
) -> PrimitiveOutcome:
    """`no_deprecated_context_vars` for sibling Python packages
    (top-level dirs other than `dags/` and the skip-list). Some
    multi-package tasks keep their callable bodies in `libs/`
    where the dags-only context-var scan can't see them; this
    primitive closes that hole.

    Returns `passed=None` when the task has no sibling .py files —
    the metric does not apply to single-tree tasks."""
    skip = sibling_skip_dirs()
    files = _collect_sibling_files(Path(task_dir), skip)
    if not files:
        return PrimitiveOutcome(value=0.0, passed=None)
    clean = sum(1 for p in files if not _has_deprecated_context_var(p))
    value = clean / len(files)
    return PrimitiveOutcome(value=value, passed=value == 1.0)


@scoring_primitive("schedule_interval_replaced")
def schedule_interval_replaced(
    task_dir: str, agent_output: str, *, oracle_dir: str | None = None
) -> PrimitiveOutcome:
    """Every DAG construction under `task_dir/dags/` must:

    1. NOT pass `schedule_interval=` (removed in AF3).
    2. Pass an explicit `schedule=` or `timetable=` replacement.

    Requiring a replacement — not just absence of the legacy kwarg —
    closes the deletion attack (agent removes `schedule_interval=` and
    silently reverts to the default schedule, changing behaviour).
    Files that do not construct a DAG neither pass nor fail on this
    metric; they are excluded from the denominator."""
    dags = Path(task_dir) / _DAGS_SUBDIR
    files = sorted(dags.rglob("*.py"))
    if not files:
        return PrimitiveOutcome(value=0.0, passed=False)
    graded = [_grade_schedule_file(p) for p in files]
    relevant = [status for status in graded if status is not None]
    if not relevant:
        return PrimitiveOutcome(value=0.0, passed=False)
    clean = sum(1 for status in relevant if status)
    value = clean / len(relevant)
    return PrimitiveOutcome(value=value, passed=value == 1.0)


def _file_clean_of_pattern(path: Path, pattern: re.Pattern[str]) -> bool:
    text = read_text_capped(path)
    if text is None:
        return False
    return pattern.search(text) is None


def _grade_schedule_file(path: Path) -> bool | None:
    """Return True/False if the file constructs a DAG; None if not
    (metric doesn't apply). Unreadable files count as False."""
    text = read_text_capped(path)
    if text is None:
        return False
    try:
        tree = ast.parse(text)
    except (SyntaxError, ValueError):
        return False
    dag_kwargs = _collect_dag_kwargs(tree)
    if not dag_kwargs:
        return None
    for kwargs in dag_kwargs:
        if "schedule_interval" in kwargs:
            return False
        if "schedule" not in kwargs and "timetable" not in kwargs:
            return False
    return True


def _collect_dag_kwargs(tree: ast.AST) -> list[set[str]]:
    """Every DAG-construction site in the module and the set of kwarg
    names passed to it. Recognises `DAG(...)` calls and `@dag(...)`
    decorators (both from `airflow` and `airflow.sdk`)."""
    sites: list[set[str]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and _is_dag_call(node.func):
            sites.append({kw.arg for kw in node.keywords if kw.arg})
        elif isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
            for deco in node.decorator_list:
                if isinstance(deco, ast.Call) and _is_dag_decorator(deco.func):
                    sites.append({kw.arg for kw in deco.keywords if kw.arg})
                elif _is_dag_decorator(deco):
                    sites.append(set())
    return sites


def _is_dag_call(func: ast.AST) -> bool:
    if isinstance(func, ast.Name):
        return func.id == "DAG"
    if isinstance(func, ast.Attribute):
        return func.attr == "DAG"
    return False


def _is_dag_decorator(func: ast.AST) -> bool:
    if isinstance(func, ast.Name):
        return func.id == "dag"
    if isinstance(func, ast.Attribute):
        return func.attr == "dag"
    return False


@scoring_primitive("stepping_stone_respected")
def stepping_stone_respected(
    task_dir: str, agent_output: str, *, oracle_dir: str | None = None
) -> PrimitiveOutcome:
    """`requirements.txt` either pins an Airflow stepping-stone version
    (from `af3_migrations.yaml::stepping_stone_floor`) on an uncommented
    line, OR omits the apache-airflow constraint entirely
    (Runtime-supplied semantics). Direct 3.x pins still fail — those
    skip the stepping stone.

    Line-by-line parsing, comment-stripping: a substring match against
    the raw file text would accept a commented-out line like
    `# consider apache-airflow==2.10 next` as a valid pin.

    No `apache-airflow` constraint at all is a pass for the same reason
    `airflow_pin_target_aligned` accepts it: the bench runs every task
    inside an Astro Runtime container, Runtime supplies the
    apache-airflow distribution, and pinning it in requirements.txt is
    anti-idiomatic for Runtime projects. The file must still exist —
    deleting requirements.txt to dodge the metric is rejected above."""
    req = Path(task_dir) / _REQUIREMENTS_FILE
    if not req.is_file():
        return PrimitiveOutcome(value=0.0, passed=False)
    text = read_text_capped(req)
    if text is None:
        return PrimitiveOutcome(value=0.0, passed=False)
    bridges = stepping_stone_floor()
    has_any_airflow_line = False
    has_bridge = False
    has_3x = False
    for raw in text.splitlines():
        line = raw.split("#", 1)[0].strip()
        if not line:
            continue
        match = re.match(r"^apache-airflow\s*==\s*([\w.+-]+)", line)
        if not match:
            continue
        has_any_airflow_line = True
        pin = match.group(1)
        if any(pin == b or pin.startswith(f"{b}.") for b in bridges):
            has_bridge = True
        elif pin.startswith("3."):
            has_3x = True
    if not has_any_airflow_line:
        return PrimitiveOutcome(value=1.0, passed=True)
    passed = has_bridge and not has_3x
    return PrimitiveOutcome(value=1.0 if passed else 0.0, passed=passed)


@scoring_primitive("no_deprecated_imports")
def no_deprecated_imports(
    task_dir: str, agent_output: str, *, oracle_dir: str | None = None
) -> PrimitiveOutcome:
    """Fraction of files with no import paths from the
    `removed_modules` set in `af3_migrations.yaml`. Files with syntax
    errors count as not clean. 1.0 only when every file is clean."""
    dags = Path(task_dir) / _DAGS_SUBDIR
    files = sorted(dags.rglob("*.py"))
    if not files:
        return PrimitiveOutcome(value=0.0, passed=False)
    removed = removed_modules()
    clean = sum(1 for p in files if _file_is_import_clean(p, removed))
    value = clean / len(files)
    return PrimitiveOutcome(value=value, passed=value == 1.0)


@scoring_primitive("no_af3_only_imports")
def no_af3_only_imports(
    task_dir: str, agent_output: str, *, oracle_dir: str | None = None
) -> PrimitiveOutcome:
    """Fraction of files with no imports from the `af3_only_modules`
    set. The inverse of `no_deprecated_imports`: catches agents that
    apply 3.x-shaped changes (e.g. `from airflow.sdk import DAG`) on a
    minor-version bridge where the target runtime is 2.y. Files with
    syntax errors count as not clean. 1.0 only when every file is clean."""
    dags = Path(task_dir) / _DAGS_SUBDIR
    files = sorted(dags.rglob("*.py"))
    if not files:
        return PrimitiveOutcome(value=0.0, passed=False)
    forbidden = af3_only_modules()
    clean = sum(1 for p in files if _file_is_import_clean(p, forbidden))
    value = clean / len(files)
    return PrimitiveOutcome(value=value, passed=value == 1.0)


@scoring_primitive("xcom_pull_explicit_task_ids")
def xcom_pull_explicit_task_ids(
    task_dir: str, agent_output: str, *, oracle_dir: str | None = None
) -> PrimitiveOutcome:
    """Fraction of files where every `.xcom_pull(...)` call passes
    `task_ids` explicitly (as kwarg or first positional arg). AF3
    silently changed the default semantics: 2.x's `xcom_pull(key=...)`
    pulled the latest matching XCom across all upstream tasks; 3.x
    defaults `task_ids=[self.task_id]`, returning None when the
    pusher is a different task. DAGs run green but emit wrong data —
    a silent-semantics break the apache/airflow community surfaced
    in #51821 and many follow-ups.

    Detection: AST walk for `Attribute(...).xcom_pull(...)` calls.
    A call counts as explicit when it has either a `task_ids=`
    keyword OR at least one positional argument (xcom_pull's first
    positional in airflow 2.x and 3.x is task_ids — see
    `TaskInstance.xcom_pull` signature). Files with syntax errors
    count as not clean. 1.0 only when every file is explicit; calls
    inside docstrings / string literals are ignored (AST doesn't
    visit them as Call nodes)."""
    dags = Path(task_dir) / _DAGS_SUBDIR
    files = sorted(dags.rglob("*.py"))
    if not files:
        return PrimitiveOutcome(value=0.0, passed=False)
    clean = sum(1 for p in files if _file_xcom_pulls_are_explicit(p))
    value = clean / len(files)
    return PrimitiveOutcome(value=value, passed=value == 1.0)


@scoring_primitive("baseoperator_from_sdk")
def baseoperator_from_sdk(
    task_dir: str, agent_output: str, *, oracle_dir: str | None = None
) -> PrimitiveOutcome:
    """Files that import the symbol `BaseOperator` must import it
    from `airflow.sdk`, not from `airflow.models` or
    `airflow.models.baseoperator`. apache/airflow#52378 (the Task
    SDK migration tracker, ~70 provider PRs): in AF3 the legacy
    `airflow.models.BaseOperator` is a re-export with a
    DeprecationWarning, but custom operators subclassing via that
    path silently break `BaseOperatorLink.persist` with
    AttributeError. The idiomatic AF3 import is `from airflow.sdk
    import BaseOperator`.

    Two access shapes are graded:
      - **`from ... import BaseOperator`** — the module must be
        `airflow.sdk`. Any other module (`airflow.models`,
        `airflow.models.baseoperator`) fails.
      - **attribute-access base** — a class that subclasses
        `BaseOperator` via attribute access
        (`class X(airflow.models.BaseOperator)`,
        `airflow.models.baseoperator.BaseOperator`) whose dotted
        root resolves under `airflow.models*` fails. Closes the
        dodge where the agent stops importing the symbol and reaches
        for the legacy class by its full dotted path instead.

    Walks dags/ + sibling .py files (custom operators usually live
    in libs/ rather than dags/). Files that don't reference
    `BaseOperator` at all are out of scope (excluded from the
    denominator). 1.0 only when every relevant file uses the SDK
    import.

    Oracle-aware: when the oracle's solution defines a custom
    operator on `BaseOperator` but the candidate references it
    nowhere, the candidate has deleted the custom-operator surface
    (e.g. replaced the subclass with a stock `PythonOperator` stub
    that preserves task_ids) rather than migrating its import. That
    would otherwise drop the metric to `passed=None` and dodge the
    critical gate by attrition. With this guard, a candidate that
    ships no BaseOperator reference on a BaseOperator-required task
    fails the metric. `passed=None` only when neither oracle nor
    candidate references BaseOperator — the metric truly does not
    apply."""
    skip = sibling_skip_dirs()
    root = Path(task_dir)
    files = sorted((root / _DAGS_SUBDIR).rglob("*.py")) + _collect_sibling_files(root, skip)
    if not files:
        return PrimitiveOutcome(value=0.0, passed=False)
    relevant = 0
    clean = 0
    for path in files:
        verdict = _baseoperator_import_check(path)
        if verdict is None:
            continue
        relevant += 1
        if verdict:
            clean += 1
    if relevant == 0:
        if _oracle_references_baseoperator(oracle_dir, skip):
            return PrimitiveOutcome(value=0.0, passed=False)
        return PrimitiveOutcome(value=0.0, passed=None)
    value = clean / relevant
    return PrimitiveOutcome(value=value, passed=value == 1.0)


def _oracle_references_baseoperator(oracle_dir: str | None, skip: frozenset[str]) -> bool:
    """True when the oracle's solution tree (or its task tree, if no
    explicit solution) references `BaseOperator` in any file — the
    same relevance signal the candidate-side `_baseoperator_import_check`
    uses (`None` means "no reference"). Holds the oracle and candidate
    to the same standard, so an oracle that doesn't use BaseOperator
    can't force a candidate to."""
    if oracle_dir is None:
        return False
    root = Path(oracle_dir)
    solution = root / "solution"
    search_root = solution if solution.is_dir() else root
    files = sorted((search_root / _DAGS_SUBDIR).rglob("*.py")) + _collect_sibling_files(
        search_root, skip
    )
    return any(_baseoperator_import_check(path) is not None for path in files)


@scoring_primitive("sensor_uses_deferrable_mode")
def sensor_uses_deferrable_mode(
    task_dir: str, agent_output: str, *, oracle_dir: str | None = None
) -> PrimitiveOutcome:
    """Every sensor instantiation under `dags/` (and sibling pkgs)
    releases its worker slot between polls — either by passing
    `deferrable=True` or by passing `mode="reschedule"`. Both let
    the scheduler reuse the slot during the wait; only the AF2
    default `mode="poke"` pins the slot for the wait's full duration.

    AST walk for any `Call` whose function resolves to a name
    ending in `Sensor` (`S3KeySensor`, `ExternalTaskSensor`,
    `HttpSensor`, custom subclasses, etc.). A call counts as clean
    when at least one of those two kwargs is present in the right
    shape.

    Oracle-aware: when the oracle ships sensors, the candidate must
    too. An agent that "fixes" the worker-slot pinning by replacing
    the wait tasks with non-sensor pollers (PythonOperator that
    sleeps, etc.) would otherwise drop the metric to `passed=None`
    and dodge the test by attrition. With this gate, candidates that
    drop the sensors entirely on a sensor-required task fail the
    metric. `passed=None` only when neither oracle nor candidate
    contains a sensor — the metric truly does not apply."""
    skip = sibling_skip_dirs()
    root = Path(task_dir)
    files = sorted((root / _DAGS_SUBDIR).rglob("*.py")) + _collect_sibling_files(root, skip)
    if not files:
        return PrimitiveOutcome(value=0.0, passed=False)
    relevant = 0
    clean = 0
    for path in files:
        for verdict in _sensor_deferrable_verdicts(path):
            relevant += 1
            if verdict:
                clean += 1
    oracle_expects_sensor = _oracle_has_sensor_call(oracle_dir, skip)
    if relevant == 0:
        if oracle_expects_sensor:
            return PrimitiveOutcome(value=0.0, passed=False)
        return PrimitiveOutcome(value=0.0, passed=None)
    value = clean / relevant
    return PrimitiveOutcome(value=value, passed=value == 1.0)


def _sensor_deferrable_verdicts(path: Path) -> list[bool]:
    """One bool per sensor-instantiation call in the file. Empty list
    when the file constructs no sensors. Files with syntax errors
    return an empty list — `parse_ok` catches those separately."""
    text = read_text_capped(path)
    if text is None:
        return []
    try:
        tree = ast.parse(text)
    except (SyntaxError, ValueError):
        return []
    verdicts: list[bool] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        name = _call_class_name(node.func)
        if name is None or not name.endswith("Sensor"):
            continue
        verdicts.append(_sensor_call_releases_slot(node))
    return verdicts


def _sensor_call_releases_slot(node: ast.Call) -> bool:
    """True when the sensor Call passes either `deferrable=True` or
    `mode="reschedule"`. Both modes release the worker slot between
    polls; only the implicit `mode="poke"` default pins the slot."""
    for kw in node.keywords:
        if kw.arg == "deferrable" and isinstance(kw.value, ast.Constant) and kw.value.value is True:
            return True
        if (
            kw.arg == "mode"
            and isinstance(kw.value, ast.Constant)
            and kw.value.value == "reschedule"
        ):
            return True
    return False


def _oracle_has_sensor_call(oracle_dir: str | None, skip: frozenset[str]) -> bool:
    """True when the oracle's solution tree (or its task tree, if no
    explicit solution) contains at least one sensor Call. Uses the
    same suffix heuristic as the candidate-side check; if the oracle
    masks its sensors behind aliases the metric would accept the
    same masking from the candidate, which is a wash."""
    if oracle_dir is None:
        return False
    root = Path(oracle_dir)
    solution = root / "solution"
    search_root = solution if solution.is_dir() else root
    files = sorted((search_root / _DAGS_SUBDIR).rglob("*.py")) + _collect_sibling_files(
        search_root, skip
    )
    for path in files:
        text = read_text_capped(path)
        if text is None:
            continue
        try:
            tree = ast.parse(text)
        except (SyntaxError, ValueError):
            continue
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            name = _call_class_name(node.func)
            if name and name.endswith("Sensor"):
                return True
    return False


def _call_class_name(func: ast.AST) -> str | None:
    """Extract the class name from a Call's func node — bare
    `Sensor(...)` returns `'Sensor'`, `module.Sensor(...)` returns
    `'Sensor'`, anything else returns None."""
    if isinstance(func, ast.Name):
        return func.id
    if isinstance(func, ast.Attribute):
        return func.attr
    return None


@scoring_primitive("postgreshook_uses_database_kwarg")
def postgreshook_uses_database_kwarg(
    task_dir: str, agent_output: str, *, oracle_dir: str | None = None
) -> PrimitiveOutcome:
    """Every `PostgresHook(...)` call must use `database=` (not the
    legacy `schema=`) to override the connection's default database.
    Postgres provider 6.0 renamed the kwarg without a deprecation
    warning at the call site; old code keeps running but silently
    queries the connection's default database instead of the
    override. Silent-semantics break — no error, wrong results.

    AST walk for `PostgresHook(...)` calls (any caller — bare class,
    attribute access, anywhere). A call counts as clean when no
    `schema=` keyword is present. Files with syntax errors count as
    not clean.

    Oracle-aware: when the oracle's solution constructs a
    `PostgresHook` but the candidate constructs none, the candidate
    has deleted the hook surface (e.g. stubbed the DAG with a no-op
    BashOperator preserving task_ids) rather than migrating the
    kwarg. That would otherwise drop the metric to `passed=None` and
    dodge the critical gate by attrition. With this guard, a
    candidate that ships no PostgresHook on a PostgresHook-required
    task fails the metric. `passed=None` only when neither oracle nor
    candidate constructs a PostgresHook — the metric only applies to
    projects that use it."""
    skip = sibling_skip_dirs()
    root = Path(task_dir)
    files = sorted((root / _DAGS_SUBDIR).rglob("*.py")) + _collect_sibling_files(root, skip)
    if not files:
        return PrimitiveOutcome(value=0.0, passed=False)
    relevant = 0
    clean = 0
    for path in files:
        verdict = _postgreshook_kwarg_check(path)
        if verdict is None:
            continue
        relevant += 1
        if verdict:
            clean += 1
    if relevant == 0:
        if _oracle_has_postgreshook(oracle_dir, skip):
            return PrimitiveOutcome(value=0.0, passed=False)
        return PrimitiveOutcome(value=0.0, passed=None)
    value = clean / relevant
    return PrimitiveOutcome(value=value, passed=value == 1.0)


def _oracle_has_postgreshook(oracle_dir: str | None, skip: frozenset[str]) -> bool:
    """True when the oracle's solution tree (or its task tree, if no
    explicit solution) constructs a `PostgresHook`. Uses the same
    relevance signal as the candidate-side `_postgreshook_kwarg_check`
    (`None` means "no PostgresHook constructed"), holding both to the
    same standard."""
    if oracle_dir is None:
        return False
    root = Path(oracle_dir)
    solution = root / "solution"
    search_root = solution if solution.is_dir() else root
    files = sorted((search_root / _DAGS_SUBDIR).rglob("*.py")) + _collect_sibling_files(
        search_root, skip
    )
    return any(_postgreshook_kwarg_check(path) is not None for path in files)


_LEGACY_SQL_OPERATORS: frozenset[str] = frozenset({"SnowflakeOperator", "PostgresOperator"})
_AF3_SQL_OPERATOR: str = "SQLExecuteQueryOperator"
# Every SQL-operator class in scope for count parity — legacy
# DB-specific operators plus the AF3 replacement.
_ALL_SQL_OPERATORS: frozenset[str] = _LEGACY_SQL_OPERATORS | {_AF3_SQL_OPERATOR}


@scoring_primitive("uses_sql_execute_query_operator")
def uses_sql_execute_query_operator(
    task_dir: str, agent_output: str, *, oracle_dir: str | None = None
) -> PrimitiveOutcome:
    """Every DAG that previously instantiated a removed provider SQL
    operator (`SnowflakeOperator`, `PostgresOperator`) must now use
    `SQLExecuteQueryOperator` from `airflow.providers.common.sql`.

    Snowflake provider 6.0 removed `SnowflakeOperator`; postgres
    provider 6.0 removed `PostgresOperator`. The replacement is
    `SQLExecuteQueryOperator` with `conn_id=` (renamed from
    `snowflake_conn_id` / `postgres_conn_id`). Otto's KB carries
    these removals in `v1/providers/<name>/_annotations.json`;
    general-purpose agents may not pick up the migration without the
    KB.

    Detection (per DAG file):
      - **Forbidden**: any `SnowflakeOperator(...)` or
        `PostgresOperator(...)` instantiation by bare-class name or
        attribute access. The operator is removed; calling it
        raises ImportError or AttributeError on the upgraded
        provider.
      - **Required when forbidden was present in the fixture**: at
        least one `SQLExecuteQueryOperator(...)` instantiation that
        actually carries a non-empty `sql=` body kwarg. A
        name-only `SQLExecuteQueryOperator(task_id='x')` with no real
        `sql=` is a hollow operator that runs no query — it preserves
        the task id but loses observable behaviour. This mirrors the
        `task_bodies_non_trivial` body-kwarg standard (confirmed
        gameable on proj-a3ea1b: a hollow operator with no sql scored
        1.0).

    A file passes when it has zero forbidden calls AND (at least
    one SQLExecuteQueryOperator carrying a real `sql=` OR the file
    never used the legacy operators in the first place). Score is
    fraction of DAG files passing.

    Oracle-aware (two guards):
      - **presence**: when the oracle's solution ships a SQL operator
        surface (a `SQLExecuteQueryOperator`, or a legacy DB-specific
        operator the oracle migrated from) but the candidate's dags
        contain NO SQL operator at all, return `passed=False`. An
        agent that "migrates" by deleting the SQL task and stubbing the
        DAG would otherwise drop the metric to `passed=None` and dodge
        the critical gate by attrition (delete-the-SQL-task-to-stub
        scored 1.0 on proj-a3ea1b / proj-sql01).
      - **count parity**: the candidate must construct at least as
        many in-scope SQL-operator instances as the oracle's solution.
        Without this, an agent can migrate one of two SQL DAGs and stub
        the other with an `EmptyOperator` — the stubbed file drops out
        of the denominator and the partial migration scores 1.0
        (confirmed gameable on proj-a3ea1b, oracle has 5 SQL-operator
        instances). Mirrors the cosmos count-parity guards.

    `passed=None` only when neither the candidate nor the oracle
    touches any SQL operator surface — the metric is genuinely
    out-of-scope for that task shape."""
    dags = Path(task_dir) / _DAGS_SUBDIR
    files = sorted(dags.rglob("*.py"))
    if not files:
        return PrimitiveOutcome(value=0.0, passed=False)
    relevant = 0
    clean = 0
    candidate_count = 0
    for path in files:
        candidate_count += _sql_operator_count(path)
        verdict = _sql_operator_verdict(path)
        if verdict is None:
            continue
        relevant += 1
        if verdict:
            clean += 1
    oracle_count = _oracle_sql_operator_count(oracle_dir)
    if relevant == 0:
        if oracle_count > 0:
            return PrimitiveOutcome(value=0.0, passed=False)
        return PrimitiveOutcome(value=0.0, passed=None)
    # Count parity: a candidate that migrated only some of the oracle's
    # SQL DAGs (stubbing the rest) has fewer SQL-operator instances than
    # the oracle. Fail before the per-file fraction can rubber-stamp it.
    if candidate_count < oracle_count:
        return PrimitiveOutcome(value=0.0, passed=False)
    value = clean / relevant
    return PrimitiveOutcome(value=value, passed=value == 1.0)


def _oracle_sql_operator_count(oracle_dir: str | None) -> int:
    """Number of in-scope SQL-operator instances (legacy DB-specific +
    migrated `SQLExecuteQueryOperator`) the oracle's solution tree (or
    its task tree, if no explicit solution) constructs. Uses the same
    name heuristic as the candidate-side count, so oracle and candidate
    are held to the same standard. `0` means the oracle ships no SQL
    surface — the metric is genuinely N/A for this task shape."""
    if oracle_dir is None:
        return 0
    root = Path(oracle_dir)
    solution = root / "solution"
    search_root = solution if solution.is_dir() else root
    return sum(
        _sql_operator_count(path) for path in sorted((search_root / _DAGS_SUBDIR).rglob("*.py"))
    )


def _sql_operator_count(path: Path) -> int:
    """Count in-scope SQL-operator Call sites in a file: legacy
    `SnowflakeOperator` / `PostgresOperator` plus migrated
    `SQLExecuteQueryOperator`. Unreadable / unparseable files count 0
    (the per-file verdict scores those as `False` separately)."""
    text = read_text_capped(path)
    if text is None:
        return 0
    try:
        tree = ast.parse(text)
    except (SyntaxError, ValueError):
        return 0
    count = 0
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and _sql_call_name(node) in _ALL_SQL_OPERATORS:
            count += 1
    return count


def _sql_operator_verdict(path: Path) -> bool | None:
    """`True` = AF3-compliant (at least one SQLExecuteQueryOperator
    carrying a real `sql=` body kwarg, and no legacy operator).
    `False` = still uses SnowflakeOperator / PostgresOperator, or the
    only `SQLExecuteQueryOperator` in the file is hollow (no non-empty
    `sql=`). `None` = file has none of the SQL operators in scope; out
    of bounds for this metric."""
    text = read_text_capped(path)
    if text is None:
        return False
    try:
        tree = ast.parse(text)
    except (SyntaxError, ValueError):
        return False
    has_legacy = False
    has_af3_with_sql = False
    saw_af3 = False
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        name = _sql_call_name(node)
        if name in _LEGACY_SQL_OPERATORS:
            has_legacy = True
        elif name == _AF3_SQL_OPERATOR:
            saw_af3 = True
            if _has_real_sql_kwarg(node):
                has_af3_with_sql = True
    if not has_legacy and not saw_af3:
        return None
    return (not has_legacy) and has_af3_with_sql


def _sql_call_name(node: ast.Call) -> str | None:
    func = node.func
    if isinstance(func, ast.Name):
        return func.id
    if isinstance(func, ast.Attribute):
        return func.attr
    return None


def _has_real_sql_kwarg(node: ast.Call) -> bool:
    """True when the Call passes a `sql=` kwarg with a non-empty,
    non-trivial value. Mirrors the `task_bodies_non_trivial` body-kwarg
    standard: an empty string, whitespace, or other recognised no-op
    body does not count. A `sql=` built from a variable / expression we
    cannot statically evaluate is accepted (can't prove triviality)."""
    for kw in node.keywords:
        if kw.arg != "sql":
            continue
        value = kw.value
        if isinstance(value, ast.Constant):
            return isinstance(value.value, str) and value.value.strip() != ""
        if isinstance(value, ast.JoinedStr):
            # f-string — non-trivial only if it has a non-empty part.
            return any(
                not (isinstance(v, ast.Constant) and str(v.value).strip() == "")
                for v in value.values
            )
        # Non-literal sql (variable, concatenation, list join, …) — can't
        # prove it's trivial, so accept it.
        return True
    return False


@scoring_primitive("operator_link_uses_af3_signature")
def operator_link_uses_af3_signature(
    task_dir: str, agent_output: str, *, oracle_dir: str | None = None
) -> PrimitiveOutcome:
    """Every operator-link class that defines `persist` must use the
    AF3 signature: `@classmethod` with first parameter `cls` and
    second parameter `context`. AF2 used `@staticmethod` with
    `(context, task_instance/operator, ...)` and called
    `task_instance.xcom_push(context=, key=, value=)` — that shape
    no longer works on AF3 because BaseOperator's bound `xcom_push`
    no longer accepts `context=`. apache/airflow#52378 surfaced this
    across ~70 provider PRs; airflow.providers.common.compat hides
    the rename for dual-version providers, but custom-operator code
    must update directly.

    A class is treated as a link class when its name ends in `Link`
    or it inherits from any base whose name ends in `Link`. Walks
    dags/ + sibling .py files.

    Oracle-aware: when the oracle's solution defines an operator-link
    class with a `persist` method, the candidate must too. An agent
    that deletes the link class (or renames it off the `Link` suffix)
    would otherwise drop the metric to `passed=None` and dodge the
    critical gate by attrition. With this guard, a candidate that
    ships no link-class persist on a link-required task fails the
    metric. `passed=None` only when neither oracle nor candidate
    defines a link class with a `persist` method — the metric truly
    does not apply."""
    skip = sibling_skip_dirs()
    root = Path(task_dir)
    files = sorted((root / _DAGS_SUBDIR).rglob("*.py")) + _collect_sibling_files(root, skip)
    if not files:
        return PrimitiveOutcome(value=0.0, passed=False)
    relevant = 0
    clean = 0
    for path in files:
        for verdict in _operator_link_persist_verdicts(path):
            relevant += 1
            if verdict:
                clean += 1
    if relevant == 0:
        if _oracle_has_link_persist(oracle_dir, skip):
            return PrimitiveOutcome(value=0.0, passed=False)
        return PrimitiveOutcome(value=0.0, passed=None)
    value = clean / relevant
    return PrimitiveOutcome(value=value, passed=value == 1.0)


def _oracle_has_link_persist(oracle_dir: str | None, skip: frozenset[str]) -> bool:
    """True when the oracle's solution tree (or its task tree, if no
    explicit solution) defines an operator-link class with a `persist`
    method. Uses the same link-class heuristic as the candidate-side
    check, so an oracle that masks its link behind a non-`Link` name
    would accept the same masking from the candidate (a wash)."""
    if oracle_dir is None:
        return False
    root = Path(oracle_dir)
    solution = root / "solution"
    search_root = solution if solution.is_dir() else root
    files = sorted((search_root / _DAGS_SUBDIR).rglob("*.py")) + _collect_sibling_files(
        search_root, skip
    )
    return any(_operator_link_persist_verdicts(path) for path in files)


def _operator_link_persist_verdicts(path: Path) -> list[bool]:
    """One bool per link-class persist method found in the file.
    True: AF3 shape. False: any other shape. Empty list when the
    file defines no link-class persist methods."""
    text = read_text_capped(path)
    if text is None:
        return []
    try:
        tree = ast.parse(text)
    except (SyntaxError, ValueError):
        return []
    verdicts: list[bool] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.ClassDef):
            continue
        if not _is_link_class(node):
            continue
        for item in node.body:
            if not isinstance(item, ast.FunctionDef) or item.name != "persist":
                continue
            verdicts.append(_persist_is_af3_shape(item))
    return verdicts


def _is_link_class(node: ast.ClassDef) -> bool:
    if node.name.endswith("Link"):
        return True
    for base in node.bases:
        base_name = None
        if isinstance(base, ast.Name):
            base_name = base.id
        elif isinstance(base, ast.Attribute):
            base_name = base.attr
        if base_name and base_name.endswith("Link"):
            return True
    return False


def _persist_is_af3_shape(func: ast.FunctionDef) -> bool:
    """A `persist` method is AF3-shaped only if BOTH conditions hold:

    1. **Signature**: `@classmethod` decorator, first param `cls`, second
       `context`. Catches the `@staticmethod`-with-`(context, ti, ...)`
       AF2 idiom.
    2. **Body**: contains at least one `xcom_push(...)` call AND no
       AF2-shaped `xcom_push(context=...)` call. AF3 `persist`
       persists the link payload via XCom; a body that pushes nothing
       (the no-op stub that merely lacks the `context=` idiom) renders
       no link and is not a real migration. AF3 also removed the
       `context=` kwarg from `BaseOperator.xcom_push`; bodies that
       still pass it raise `TypeError` at runtime even though the
       signature looks AF3-correct. Bench mistake captured in
       `docs/bench-design-lessons.md` §5.

    Without the body check, an agent could ship the right signature
    around an AF2-shaped body — or an empty body — and rubber-stamp
    pass.
    """
    has_classmethod = any(
        isinstance(deco, ast.Name) and deco.id == "classmethod" for deco in func.decorator_list
    )
    if not has_classmethod:
        return False
    args = func.args.args
    if len(args) < 2:
        return False
    if not (args[0].arg == "cls" and args[1].arg == "context"):
        return False
    return _persist_body_is_af3(func)


def _persist_body_is_af3(func: ast.FunctionDef) -> bool:
    """The persist body is AF3-shaped when it pushes a value via XCom
    and does so without the AF2 `context=` idiom.

    Walks the body for `<expr>.xcom_push(...)` calls:
      - any call whose kwargs include `context=` is the AF2 idiom and
        rejects the method (AF3's `xcom_push` dropped `context=`), and
      - at least one such call must exist — a body that pushes nothing
        is a no-op stub that renders no link, not a real migration.

    Returns True only when the body contains a clean `xcom_push(...)`
    and no AF2-shaped one."""
    pushes_value = False
    for node in ast.walk(func):
        if not isinstance(node, ast.Call):
            continue
        callee = node.func
        if not isinstance(callee, ast.Attribute) or callee.attr != "xcom_push":
            continue
        if any(kw.arg == "context" for kw in node.keywords):
            return False
        pushes_value = True
    return pushes_value


@scoring_primitive("plugin_no_legacy_ui_attrs")
def plugin_no_legacy_ui_attrs(
    task_dir: str, agent_output: str, *, oracle_dir: str | None = None
) -> PrimitiveOutcome:
    """Every AirflowPlugin subclass must not assign non-empty values
    to AF2 Flask-AppBuilder UI attributes (`flask_blueprints`,
    `appbuilder_views`, `appbuilder_menu_items`, `admin_views`).

    AF3 replaced Flask-AppBuilder with FastAPI. Plugins still load
    with these attributes, but the views they advertise render
    nothing in the new UI — silent UX break, no traceback. The AF3
    replacement path is `external_views` (iframe-mounted external
    URLs), `react_apps`, or `fastapi_apps`. An honest migration
    either drops the legacy attrs or migrates them to the AF3
    surface; the metric only enforces the absence of the dead
    attrs, not the presence of a replacement.

    Walks dags/ + sibling .py files (`plugins/` is the conventional
    location). A plugin class is detected by inheritance from
    `AirflowPlugin` (bare or attribute access). Empty list /
    `[]` assignments count as clean (the subclass effectively
    keeps the parent default).

    Oracle-aware (two guards):
      - **presence-of-class**: when the oracle's solution defines an
        `AirflowPlugin` subclass, the candidate must too. An agent
        that deletes the plugin file (or replaces it with a no-op stub
        defining no plugin class) would otherwise drop the metric to
        `passed=None` and dodge the critical gate by attrition.
      - **presence-of-surface**: when the oracle's plugin declares a
        non-empty AF3 UI surface (`fastapi_apps`, `external_views`,
        `react_apps`, `appbuilder_views`, `appbuilder_menu_items`),
        each candidate plugin class must declare at least one non-empty
        AF3 surface attr too. This closes the empty-`AirflowPlugin`
        stub that ships the class (clearing the presence-of-class
        guard) yet renders nothing — a migration that drops the legacy
        attrs without rebuilding the UI surface is not a real fix.

    `passed=None` only when neither oracle nor candidate defines an
    `AirflowPlugin` subclass — the metric truly does not apply."""
    skip = sibling_skip_dirs()
    root = Path(task_dir)
    files = sorted((root / _DAGS_SUBDIR).rglob("*.py")) + _collect_sibling_files(root, skip)
    if not files:
        return PrimitiveOutcome(value=0.0, passed=False)
    require_surface = _oracle_plugin_declares_af3_surface(oracle_dir, skip)
    relevant = 0
    clean = 0
    for path in files:
        for verdict in _plugin_legacy_ui_verdicts(path, require_surface=require_surface):
            relevant += 1
            if verdict:
                clean += 1
    if relevant == 0:
        if _oracle_has_airflow_plugin(oracle_dir, skip):
            return PrimitiveOutcome(value=0.0, passed=False)
        return PrimitiveOutcome(value=0.0, passed=None)
    value = clean / relevant
    return PrimitiveOutcome(value=value, passed=value == 1.0)


def _oracle_has_airflow_plugin(oracle_dir: str | None, skip: frozenset[str]) -> bool:
    """True when the oracle's solution tree (or its task tree, if no
    explicit solution) defines an `AirflowPlugin` subclass."""
    return any(
        _inherits_from_airflow_plugin(node)
        for tree in _oracle_plugin_trees(oracle_dir, skip)
        for node in ast.walk(tree)
        if isinstance(node, ast.ClassDef)
    )


def _oracle_plugin_declares_af3_surface(oracle_dir: str | None, skip: frozenset[str]) -> bool:
    """True when an `AirflowPlugin` subclass in the oracle's solution
    declares a non-empty AF3 UI surface attr. That's the signal that
    the task's intended fix rebuilds the UI on the AF3 surface — so the
    candidate plugin must declare one too, not just clear the legacy
    attrs."""
    for tree in _oracle_plugin_trees(oracle_dir, skip):
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.ClassDef)
                and _inherits_from_airflow_plugin(node)
                and _plugin_nonempty_attrs(node) & _AF3_PLUGIN_UI_ATTRS
            ):
                return True
    return False


def _oracle_plugin_trees(oracle_dir: str | None, skip: frozenset[str]):
    """Parsed ASTs of every candidate plugin-bearing file in the
    oracle's solution tree (or its task tree, if no explicit
    solution)."""
    if oracle_dir is None:
        return
    root = Path(oracle_dir)
    solution = root / "solution"
    search_root = solution if solution.is_dir() else root
    files = sorted((search_root / _DAGS_SUBDIR).rglob("*.py")) + _collect_sibling_files(
        search_root, skip
    )
    for path in files:
        text = read_text_capped(path)
        if text is None:
            continue
        try:
            yield ast.parse(text)
        except (SyntaxError, ValueError):
            continue


_LEGACY_PLUGIN_UI_ATTRS = frozenset(
    {
        "flask_blueprints",
        "appbuilder_views",
        "appbuilder_menu_items",
        "admin_views",
    }
)

# AF3 UI surface attrs that count as a real, rendered replacement.
# `appbuilder_views` / `appbuilder_menu_items` also appear in the
# legacy set — a plugin that uses them trips the legacy-attr fail
# before the surface check helps it, so in practice only
# `fastapi_apps` / `external_views` / `react_apps` satisfy the
# presence-of-surface guard without also failing the legacy gate.
_AF3_PLUGIN_UI_ATTRS = frozenset(
    {
        "fastapi_apps",
        "external_views",
        "react_apps",
        "appbuilder_views",
        "appbuilder_menu_items",
    }
)


def _plugin_legacy_ui_verdicts(path: Path, *, require_surface: bool = False) -> list[bool]:
    text = read_text_capped(path)
    if text is None:
        return []
    try:
        tree = ast.parse(text)
    except (SyntaxError, ValueError):
        return []
    verdicts: list[bool] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.ClassDef):
            continue
        if not _inherits_from_airflow_plugin(node):
            continue
        verdicts.append(_plugin_class_passes(node, require_surface=require_surface))
    return verdicts


def _inherits_from_airflow_plugin(node: ast.ClassDef) -> bool:
    for base in node.bases:
        name = None
        if isinstance(base, ast.Name):
            name = base.id
        elif isinstance(base, ast.Attribute):
            name = base.attr
        if name == "AirflowPlugin":
            return True
    return False


def _plugin_class_passes(node: ast.ClassDef, *, require_surface: bool) -> bool:
    """A plugin class passes when it assigns no non-empty legacy UI
    attr AND — when `require_surface` — declares at least one non-empty
    AF3 UI surface attr."""
    nonempty = _plugin_nonempty_attrs(node)
    if nonempty & _LEGACY_PLUGIN_UI_ATTRS:
        return False
    return not (require_surface and not (nonempty & _AF3_PLUGIN_UI_ATTRS))


def _plugin_nonempty_attrs(node: ast.ClassDef) -> set[str]:
    """Names of class-level UI attrs assigned a non-empty value. An
    empty list (`[]`) assignment is treated as absent — the subclass
    effectively keeps the parent default."""
    found: set[str] = set()
    for item in node.body:
        if isinstance(item, ast.Assign):
            for target in item.targets:
                if isinstance(target, ast.Name) and not _is_empty_list(item.value):
                    found.add(target.id)
        elif isinstance(item, ast.AnnAssign):
            target = item.target
            if (
                isinstance(target, ast.Name)
                and item.value is not None
                and not _is_empty_list(item.value)
            ):
                found.add(target.id)
    return found


def _is_empty_list(node: ast.AST) -> bool:
    return isinstance(node, ast.List) and not node.elts


def _postgreshook_kwarg_check(path: Path) -> bool | None:
    """True if every PostgresHook(...) call avoids `schema=`,
    False if any call passes it, None if the file doesn't
    construct a PostgresHook at all."""
    text = read_text_capped(path)
    if text is None:
        return False
    try:
        tree = ast.parse(text)
    except (SyntaxError, ValueError):
        return False
    saw = False
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        # `PostgresHook(...)` (bare name) and `something.PostgresHook(...)`
        # (attribute access). The class name is the only signal we
        # need — agents legitimately use either spelling depending
        # on import style.
        name = None
        if isinstance(func, ast.Name):
            name = func.id
        elif isinstance(func, ast.Attribute):
            name = func.attr
        if name != "PostgresHook":
            continue
        saw = True
        if any(kw.arg == "schema" for kw in node.keywords):
            return False
    return True if saw else None


def _baseoperator_import_check(path: Path) -> bool | None:
    """True if every BaseOperator reference in the file is AF3-clean,
    False if any uses the legacy path, None if the file doesn't
    reference BaseOperator at all (out of scope).

    Reference shapes inspected:
      - `from <module> import BaseOperator` — the module must be
        `airflow.sdk`.
      - a class base whose dotted path (after resolving any in-file
        alias) ends in `.BaseOperator` and roots under `airflow.models`.
      - an ASSIGNMENT whose resolved RHS is the legacy
        `airflow.models[.baseoperator].BaseOperator`
        (`BO = airflow.models.BaseOperator`, directly or via the alias
        map). The binding itself is the break — the file is relevant
        and FAILS regardless of whether the bound name is later used as
        a base. This subsumes the single-file conditional re-export
        case (`if True:\n    BO = airflow.models.BaseOperator`).

    Alias resolution closes the one-more-indirection dodges. A
    per-file map binds names to the dotted paths they stand for:
      - `import airflow.models as m`  -> m = airflow.models
      - `from airflow import models`  -> models = airflow.models
      - `BO = airflow.models.BaseOperator`  -> BO = airflow.models.BaseOperator
        (RHS resolved through the map first, so `BO = m.BaseOperator`
        also binds the legacy path).
    A class base is resolved through this map before the
    `airflow.models*` test, so `class X(m.BaseOperator)`,
    `class X(models.BaseOperator)`, and `class X(BO)` all fail the
    same way as the literal `class X(airflow.models.BaseOperator)`.

    Cross-file re-export across separate modules is out of scope (a
    documented AST limitation): the alias map is per-file."""
    text = read_text_capped(path)
    if text is None:
        return False
    try:
        tree = ast.parse(text)
    except (SyntaxError, ValueError):
        return False
    aliases = _build_module_alias_map(tree)
    saw = False
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            names = {alias.name for alias in node.names}
            if "BaseOperator" not in names:
                continue
            saw = True
            if node.module != "airflow.sdk":
                return False
        elif isinstance(node, ast.Assign):
            # An assignment binding the legacy BaseOperator to any name
            # (`BO = airflow.models.BaseOperator`) is itself a break,
            # even nested inside an `if`/`try` block and even if the
            # bound name is never used as a base later.
            resolved = _resolve_base_dotted_path(node.value, aliases)
            if resolved is not None and _is_legacy_baseoperator_path(resolved):
                return False
        elif isinstance(node, ast.ClassDef):
            for base in node.bases:
                resolved = _resolve_base_dotted_path(base, aliases)
                if resolved is None or not resolved.endswith(".BaseOperator"):
                    continue
                # `airflow.models.BaseOperator` /
                # `airflow.models.baseoperator.BaseOperator` — the legacy
                # re-export reached directly or through an in-file alias.
                saw = True
                if _is_legacy_baseoperator_path(resolved):
                    return False
    return True if saw else None


def _is_legacy_baseoperator_path(resolved: str) -> bool:
    """True when a resolved dotted path is the legacy
    `airflow.models.BaseOperator` re-export — either exactly or via the
    `airflow.models.baseoperator.BaseOperator` submodule.

    Must match ONLY the BaseOperator re-export, not every
    `airflow.models.*` symbol — otherwise the assignment branch wrongly
    fails a correct solution that imports BaseOperator from airflow.sdk
    but still references e.g. `airflow.models.DAG` in an assignment."""
    return resolved.startswith("airflow.models.") and resolved.endswith(".BaseOperator")


def _build_module_alias_map(tree: ast.AST) -> dict[str, str]:
    """Map in-file names to the dotted paths they stand for.

    Handles three binding shapes:
      - `import airflow.models as m`  -> {"m": "airflow.models"}
      - `from airflow import models`  -> {"models": "airflow.models"}
      - `BO = airflow.models.BaseOperator`  -> {"BO": "airflow.models.BaseOperator"}
        (and `BO = m.BaseOperator` once `m` is known).

    Module bindings are collected first so a later simple assignment
    whose RHS uses an alias (`BO = m.BaseOperator`) resolves through
    them. The whole tree is walked with `ast.walk` (not just
    `tree.body`) so bindings nested inside `if` / `try` / `with` /
    function blocks are captured too — closes the
    `if True:\n    BO = airflow.models.BaseOperator` dodge."""
    aliases: dict[str, str] = {}
    # First pass: import bindings.
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.asname:
                    aliases[alias.asname] = alias.name
        elif isinstance(node, ast.ImportFrom) and node.module is not None and node.level == 0:
            for alias in node.names:
                bound = alias.asname or alias.name
                aliases[bound] = f"{node.module}.{alias.name}"
    # Second pass: simple `NAME = <dotted>` assignments, resolved
    # through the import bindings.
    for node in ast.walk(tree):
        if not isinstance(node, ast.Assign) or len(node.targets) != 1:
            continue
        target = node.targets[0]
        if not isinstance(target, ast.Name):
            continue
        resolved = _resolve_base_dotted_path(node.value, aliases)
        if resolved is not None:
            aliases[target.id] = resolved
    return aliases


def _resolve_base_dotted_path(node: ast.AST, aliases: dict[str, str]) -> str | None:
    """Dotted path a `Name`/`Attribute` chain refers to, after
    rewriting its leftmost name through `aliases`.

    `m.BaseOperator` with `m -> airflow.models` becomes
    `airflow.models.BaseOperator`. A bare `Name` (`BO`) resolves to
    its bound path if present, else to its own id. Returns None for
    anything that isn't a static dotted reference."""
    if isinstance(node, ast.Name):
        return aliases.get(node.id, node.id)
    dotted = _attribute_dotted_path(node)
    if dotted is None:
        return None
    head, _, tail = dotted.partition(".")
    if head in aliases and tail:
        return f"{aliases[head]}.{tail}"
    return dotted


def _attribute_dotted_path(node: ast.AST) -> str | None:
    """Reconstruct the dotted path of a pure `Name`/`Attribute` chain
    (`airflow.models.baseoperator.BaseOperator` -> the string). Returns
    None for anything that isn't a static dotted reference (subscripts,
    calls, etc.)."""
    parts: list[str] = []
    current: ast.AST = node
    while isinstance(current, ast.Attribute):
        parts.append(current.attr)
        current = current.value
    if not isinstance(current, ast.Name):
        return None
    parts.append(current.id)
    return ".".join(reversed(parts))


def _file_xcom_pulls_are_explicit(path: Path) -> bool:
    text = read_text_capped(path)
    if text is None:
        return False
    try:
        tree = ast.parse(text)
    except (SyntaxError, ValueError):
        return False
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if not isinstance(func, ast.Attribute) or func.attr != "xcom_pull":
            continue
        has_task_ids_kw = any(kw.arg == "task_ids" for kw in node.keywords)
        if has_task_ids_kw:
            continue
        if node.args:
            # First positional is task_ids per the API contract.
            continue
        return False
    return True


def _file_is_import_clean(path: Path, removed: frozenset[str]) -> bool:
    text = read_text_capped(path)
    if text is None:
        return False
    try:
        tree = ast.parse(text)
    except (SyntaxError, ValueError):
        return False
    for node in ast.walk(tree):
        for module in _import_modules(node):
            for removed_root in removed:
                if module == removed_root or module.startswith(f"{removed_root}."):
                    return False
    return True


def _import_modules(node: ast.AST) -> list[str]:
    """All module paths a single import statement touches. `import a, b`
    is one node with two names — checking only `names[0]` misses `b`."""
    if isinstance(node, ast.ImportFrom) and node.module is not None:
        return [node.module]
    if isinstance(node, ast.Import):
        return [alias.name for alias in node.names]
    return []


@scoring_primitive("imports_clean_across_sibling_pkgs")
def imports_clean_across_sibling_pkgs(
    task_dir: str, agent_output: str, *, oracle_dir: str | None = None
) -> PrimitiveOutcome:
    """Fraction of files in sibling Python packages (top-level
    directories next to `dags/` and root-level `*.py`, excluding the
    ones in `sibling_skip_dirs`) with no `removed_modules` imports.
    Targets multi-repo / custom-factory shapes where the DAG bundle
    imports from a sibling library package that also needs migrating.

    Oracle-aware: when the source task ships with sibling packages
    (the fixture expects them), the agent cannot game the metric by
    deleting them — that used to flip `passed` to `None` and drop the
    weight from the denominator. If the oracle expected siblings but
    the agent's tree has none, we fail the metric instead.

    `passed=None` only when the oracle itself has no siblings — i.e.
    the metric genuinely does not apply to this task shape."""
    skip = sibling_skip_dirs()
    files = _collect_sibling_files(Path(task_dir), skip)
    oracle_expects = _oracle_has_siblings(oracle_dir, skip)
    if not files:
        if oracle_expects:
            return PrimitiveOutcome(value=0.0, passed=False)
        return PrimitiveOutcome(value=0.0, passed=None)
    removed = removed_modules()
    clean = sum(1 for p in files if _file_is_import_clean(p, removed))
    value = clean / len(files)
    return PrimitiveOutcome(value=value, passed=value == 1.0)


def _collect_sibling_files(root: Path, skip: frozenset[str]) -> list[Path]:
    """Python files outside `dags/` that belong to the project: top-level
    `*.py` files and anything under sibling directories not in `skip`."""
    if not root.is_dir():
        return []
    files: list[Path] = []
    for child in sorted(root.iterdir()):
        if child.is_dir():
            if child.name in skip:
                continue
            files.extend(sorted(child.rglob("*.py")))
        elif child.is_file() and child.suffix == ".py":
            files.append(child)
    return files


def _oracle_has_siblings(oracle_dir: str | None, skip: frozenset[str]) -> bool:
    if oracle_dir is None:
        return False
    return bool(_collect_sibling_files(Path(oracle_dir), skip))


@scoring_primitive("dag_imports_bundle_safe")
def dag_imports_bundle_safe(
    task_dir: str, agent_output: str, *, oracle_dir: str | None = None
) -> PrimitiveOutcome:
    """Every DAG file under `dags/` must reach its shared/sibling code
    through real package paths — not by mutating `sys.path` at parse
    time and importing a sibling module by its bare stem.

    AF3 bundles each DAG folder as its own importable unit; the
    scheduler, workers, and triggerer each load it in isolation. A DAG
    that does `sys.path.insert(0, "../common")` then `import order_utils`
    only resolves because of that parse-time hack — it breaks the
    moment the bundle is loaded from a different process / working
    directory. The idiomatic fix is to make the shared code an
    importable package (`from common.order_utils import …`) installed
    on the base path.

    Two unsafe shapes are graded per DAG file:
      - **sys.path manipulation**: `sys.path.insert(...)`,
        `sys.path.append(...)`, `sys.path.extend(...)`, and the
        equivalent reached via an aliased `path` (`from sys import
        path; path.insert(...)`) or built with `os.path` munging
        feeding any of those calls.
      - **bare-stem sibling import**: `import <stem>` / `from <stem>
        import …` where `<stem>` is the module name of a `.py` file
        in a sibling package (it would only resolve via the hack).

    Oracle-aware: when the oracle's solution imports its siblings via
    real package paths (i.e. the oracle DAGs are bundle-safe and the
    task ships siblings), the candidate must be too. A candidate that
    keeps the sys.path hack / bare-stem import on such a task fails.
    `passed=None` only when the oracle does not expect bundle-safe
    imports — there are no sibling packages, so the metric does not
    apply to this task shape."""
    skip = sibling_skip_dirs()
    root = Path(task_dir)
    dag_files = sorted((root / _DAGS_SUBDIR).rglob("*.py"))
    if not dag_files:
        return PrimitiveOutcome(value=0.0, passed=False)
    if not _oracle_expects_bundle_safe(oracle_dir, skip):
        return PrimitiveOutcome(value=0.0, passed=None)
    sibling_stems = _sibling_module_stems(root, skip)
    clean = sum(1 for p in dag_files if _dag_file_is_bundle_safe(p, sibling_stems))
    value = clean / len(dag_files)
    return PrimitiveOutcome(value=value, passed=value == 1.0)


def _oracle_expects_bundle_safe(oracle_dir: str | None, skip: frozenset[str]) -> bool:
    """True when the oracle's solution ships sibling packages AND its
    own DAG files are bundle-safe (no sys.path hack, no bare-stem
    sibling import). That's the signal that the task's intended fix is
    to import siblings via real package paths — if the oracle itself
    relied on the hack we'd have no clean target to hold the candidate
    to."""
    if oracle_dir is None:
        return False
    root = Path(oracle_dir)
    solution = root / "solution"
    search_root = solution if solution.is_dir() else root
    if not _collect_sibling_files(search_root, skip):
        return False
    dag_files = sorted((search_root / _DAGS_SUBDIR).rglob("*.py"))
    if not dag_files:
        return False
    sibling_stems = _sibling_module_stems(search_root, skip)
    return all(_dag_file_is_bundle_safe(p, sibling_stems) for p in dag_files)


def _sibling_module_stems(root: Path, skip: frozenset[str]) -> frozenset[str]:
    """Module stems of every `.py` file in a sibling package — the set
    of names a DAG could import by bare stem only because the sibling
    folder was pushed onto sys.path. `__init__` is excluded (it's the
    package marker, never imported by stem)."""
    stems: set[str] = set()
    for path in _collect_sibling_files(root, skip):
        if path.stem != "__init__":
            stems.add(path.stem)
    return frozenset(stems)


def _dag_file_is_bundle_safe(path: Path, sibling_stems: frozenset[str]) -> bool:
    """A DAG file is bundle-safe when it reaches its shared code through
    real package paths. Unreadable / unparseable files count as unsafe
    so a syntax error can't mask the hack.

    Two unsafe signals (any one fails the file):
      - **bare-stem sibling import**: `import <stem>` / `from <stem>
        import …` where `<stem>` is a sibling module name, plus the
        dynamic equivalents `importlib.import_module("<stem>")` and
        `__import__("<stem>")` with a constant string arg whose value
        is a sibling stem. These resolve only via the parse-time hack.
      - **relative import**: any `from .x import …` (`ImportFrom` with
        `level > 0`). A bundled DAG folder is loaded in isolation from
        multiple processes; relative imports off the dag module don't
        resolve there.

    A bare `sys.path.append(...)` for an unrelated reason, alongside a
    clean package import, is legitimate and passes — a sys.path
    mutation only ever mattered when paired with a sibling-stem import
    signal, but that signal already fails the file on its own, so the
    verdict reduces to "no sibling-import signal". (Earlier revisions
    carried gated sys.path detection; it was provably redundant —
    `mutates_sys_path AND sibling_import_signal` is subsumed by
    `sibling_import_signal` — so it's been dropped.)"""
    text = read_text_capped(path)
    if text is None:
        return False
    try:
        tree = ast.parse(text)
    except (SyntaxError, ValueError):
        return False
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.level > 0:
            # Relative import in a DAG file — never bundle-safe.
            return False
        if isinstance(node, ast.Call) and _dynamic_imports_sibling_stem(node, sibling_stems):
            return False
        for module in _import_modules(node):
            top = module.split(".", 1)[0]
            if top in sibling_stems:
                return False
    return True


def _dynamic_imports_sibling_stem(node: ast.Call, sibling_stems: frozenset[str]) -> bool:
    """True when the Call is `importlib.import_module("<stem>")` or
    `__import__("<stem>")` with a CONSTANT string first arg whose value
    is a sibling stem. The dynamic equivalent of a bare-stem import —
    it only resolves via the parse-time sys.path hack."""
    func = node.func
    is_import_module = isinstance(func, ast.Attribute) and func.attr == "import_module"
    is_dunder_import = isinstance(func, ast.Name) and func.id == "__import__"
    if not (is_import_module or is_dunder_import):
        return False
    if not node.args:
        return False
    first = node.args[0]
    if not isinstance(first, ast.Constant) or not isinstance(first.value, str):
        return False
    stem = first.value.split(".", 1)[0]
    return stem in sibling_stems


@scoring_primitive("no_orm_access")
def no_orm_access(
    task_dir: str, agent_output: str, *, oracle_dir: str | None = None
) -> PrimitiveOutcome:
    """Fraction of files under `dags/` that do NOT access the Airflow
    ORM directly. AF3's scheduler no longer exposes `settings.Session()`
    to DAG code — task-instance / dag-run queries must go through
    Task-SDK IPC (`ti.get_task_states`, `ti.get_previous_dagrun`,
    `ti.get_dr_count`, …). Patterns driven from
    `af3_migrations.yaml::forbidden_orm_patterns`.

    1.0 only when every DAG file is clean of ORM access. Files that
    fail to parse / read still count as unclean so a syntax error
    can't mask an ORM use."""
    dags = Path(task_dir) / _DAGS_SUBDIR
    files = sorted(dags.rglob("*.py"))
    if not files:
        return PrimitiveOutcome(value=0.0, passed=False)
    patterns = forbidden_orm_patterns()
    clean = sum(1 for p in files if _file_free_of_patterns(p, patterns))
    value = clean / len(files)
    return PrimitiveOutcome(value=value, passed=value == 1.0)


def _file_free_of_patterns(path: Path, patterns: tuple[re.Pattern[str], ...]) -> bool:
    text = read_text_capped(path)
    if text is None:
        return False
    return not any(pattern.search(text) for pattern in patterns)


@scoring_primitive("tz_aware_start_date")
def tz_aware_start_date(
    task_dir: str, agent_output: str, *, oracle_dir: str | None = None
) -> PrimitiveOutcome:
    """Every `start_date=` value passed to a DAG construction must be
    timezone-aware. Airflow 3 requires aware datetimes; naïve
    `datetime(y, m, d)` raises a deprecation warning that becomes an
    error in stricter releases.

    A `start_date` is considered tz-aware when:
      - it's a call to anything under `pendulum.*` (assumed aware), or
      - it's a `datetime(...)` call with an explicit `tzinfo=` keyword.

    Variable references and other expressions are accepted (we can't
    statically prove tz-awareness without import-tracing).

    Oracle-aware: when the oracle's solution declares `start_date=`
    on a DAG construction but the candidate declares none, the
    candidate has dropped the `start_date=` kwarg entirely (reverting
    to the implicit default) rather than making it tz-aware. That
    would otherwise drop the metric to `passed=None` and dodge the
    critical gate by attrition — confirmed gameable: a stub that
    preserves task_ids but omits every `start_date=` scored 1.0.
    With this guard, a candidate that declares no `start_date=` on a
    start_date-required task fails. `passed=None` only when neither
    oracle nor candidate declares a `start_date=` — the metric does
    not apply."""
    dags = Path(task_dir) / _DAGS_SUBDIR
    files = sorted(dags.rglob("*.py"))
    if not files:
        return PrimitiveOutcome(value=0.0, passed=False)
    relevant = 0
    clean = 0
    for path in files:
        text = read_text_capped(path)
        if text is None:
            return PrimitiveOutcome(value=0.0, passed=False)
        try:
            tree = ast.parse(text)
        except (SyntaxError, ValueError):
            return PrimitiveOutcome(value=0.0, passed=False)
        for value in _collect_start_date_values(tree):
            relevant += 1
            if _is_tz_aware(value):
                clean += 1
    if relevant == 0:
        if _oracle_declares_start_date(oracle_dir):
            return PrimitiveOutcome(value=0.0, passed=False)
        return PrimitiveOutcome(value=0.0, passed=None)
    fraction = clean / relevant
    return PrimitiveOutcome(value=fraction, passed=fraction == 1.0)


def _oracle_declares_start_date(oracle_dir: str | None) -> bool:
    """True when the oracle's solution tree (or its task tree, if no
    explicit solution) declares a `start_date=` on at least one DAG
    construction — the same relevance signal the candidate side uses.
    Holds the oracle and candidate to the same standard."""
    if oracle_dir is None:
        return False
    root = Path(oracle_dir)
    solution = root / "solution"
    search_root = solution if solution.is_dir() else root
    for path in sorted((search_root / _DAGS_SUBDIR).rglob("*.py")):
        text = read_text_capped(path)
        if text is None:
            continue
        try:
            tree = ast.parse(text)
        except (SyntaxError, ValueError):
            continue
        if any(True for _ in _collect_start_date_values(tree)):
            return True
    return False


def _collect_start_date_values(tree: ast.AST):
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        for kw in node.keywords:
            if kw.arg == "start_date":
                yield kw.value


def _is_tz_aware(value: ast.AST) -> bool:
    if not isinstance(value, ast.Call):
        # Variable reference, attribute, literal — accept; we can't
        # prove naïveté statically without import-tracing.
        return True
    func = value.func
    # Anything from `pendulum.*` (datetime, yesterday, today, parse,
    # etc.) is tz-aware by default.
    if isinstance(func, ast.Attribute):
        root = func.value
        if isinstance(root, ast.Name) and root.id == "pendulum":
            return True
    # Bare `datetime(...)` only counts as aware when `tzinfo=` is set.
    if isinstance(func, ast.Name) and func.id == "datetime":
        return any(kw.arg == "tzinfo" for kw in value.keywords)
    return True


@scoring_primitive("uses_task_sdk_ipc")
def uses_task_sdk_ipc(
    task_dir: str, agent_output: str, *, oracle_dir: str | None = None
) -> PrimitiveOutcome:
    """At least one Task-SDK IPC method (`ti.get_task_states`,
    `ti.get_previous_dagrun`, `ti.get_dr_count`, …) is actually CALLED
    in a DAG file. Presence-required — closes the deletion-as-fix
    loophole on the paired absence-based `no_orm_access`: an agent
    can't both satisfy no_orm_access (by removing the ORM code) AND
    uses_task_sdk_ipc (by replacing it with a real IPC call).

    Detection is AST-based (via `_agent_ipc_calls`): a method only
    counts when there is an `ast.Call` whose func is an `ast.Attribute`
    with `.attr` in the IPC method set — i.e. `something.get_task_states(...)`
    on a task-instance-shaped object. The method name appearing in a
    comment or a string literal does NOT count (closes the
    delete-the-ORM-but-name-the-method-in-a-comment dodge confirmed on
    proj-d132d2)."""
    dags = Path(task_dir) / _DAGS_SUBDIR
    files = sorted(dags.rglob("*.py"))
    if not files:
        return PrimitiveOutcome(value=0.0, passed=False)
    if _agent_ipc_calls(Path(task_dir)):
        return PrimitiveOutcome(value=1.0, passed=True)
    return PrimitiveOutcome(value=0.0, passed=False)


@scoring_primitive("task_sdk_ipc_version_aware")
def task_sdk_ipc_version_aware(
    task_dir: str, agent_output: str, *, oracle_dir: str | None = None
) -> PrimitiveOutcome:
    """Every Task-SDK IPC method the agent calls must be available
    on the Airflow version the project targets. Calling
    `ti.get_previous_ti(...)` in code pinned to AF 3.0.x / 3.1.x
    raises AttributeError at runtime; the agent should either use
    an alternative (e.g. PostgresHook) or bump the Airflow pin.

    Reads `target.airflow` (falls back to `source.airflow`) from
    `oracle_dir/task.yaml` — the runner keeps task.yaml out of the
    agent's working dir, so we have to peek at the oracle source.
    Returns `passed=None` when no target is available (can't grade)
    or when the agent made no IPC calls at all (absence is the
    complementary `uses_task_sdk_ipc` check's job)."""
    if oracle_dir is None:
        return PrimitiveOutcome(value=0.0, passed=None)
    target = _target_airflow_version(Path(oracle_dir))
    if target is None:
        return PrimitiveOutcome(value=0.0, passed=None)
    calls = _agent_ipc_calls(Path(task_dir))
    if not calls:
        return PrimitiveOutcome(value=0.0, passed=None)
    since = task_sdk_ipc_method_versions()
    incompatible = [m for m in calls if m in since and _version_gt(_parse_v(since[m]), target)]
    if incompatible:
        return PrimitiveOutcome(value=0.0, passed=False)
    return PrimitiveOutcome(value=1.0, passed=True)


def _target_airflow_version(oracle_dir: Path) -> tuple[int, int, int] | None:
    task_yaml = oracle_dir / "task.yaml"
    if not task_yaml.is_file():
        return None
    import yaml  # local import; primitives normally avoid top-level yaml

    try:
        data = yaml.safe_load(task_yaml.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError:
        return None
    target = (data.get("target") or {}).get("airflow")
    source = (data.get("source") or {}).get("airflow")
    chosen = target or source
    if not isinstance(chosen, str):
        return None
    return _parse_v(chosen)


def _parse_v(version: str) -> tuple[int, int, int] | None:
    parts = version.split(".")
    try:
        major = int(parts[0])
        minor = int(parts[1]) if len(parts) > 1 else 0
        patch = int(parts[2]) if len(parts) > 2 else 0
    except (IndexError, ValueError):
        return None
    return (major, minor, patch)


def _version_gt(lhs: tuple[int, int, int] | None, rhs: tuple[int, int, int] | None) -> bool:
    if lhs is None or rhs is None:
        return False
    return lhs > rhs


def _agent_ipc_calls(task_dir: Path) -> set[str]:
    """The set of Task-SDK IPC method names actually CALLED in the
    task's DAG files. AST-based: a method counts only when there is an
    `ast.Call` whose func is an `ast.Attribute` whose `.attr` is in the
    IPC method set (`something.get_task_states(...)`). The method name
    in a comment or string literal is not a Call node, so it never
    counts — this is the load-bearing difference from the old raw-regex
    scan that matched `.method(` anywhere in the file body."""
    dags = task_dir / _DAGS_SUBDIR
    if not dags.is_dir():
        return set()
    methods = frozenset(task_sdk_ipc_methods())
    found: set[str] = set()
    for path in dags.rglob("*.py"):
        text = read_text_capped(path)
        if text is None:
            continue
        try:
            tree = ast.parse(text)
        except (SyntaxError, ValueError):
            continue
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr in methods
            ):
                found.add(node.func.attr)
    return found


@scoring_primitive("no_removed_ti_attrs")
def no_removed_ti_attrs(
    task_dir: str, agent_output: str, *, oracle_dir: str | None = None
) -> PrimitiveOutcome:
    """Fraction of DAG files with no access to TaskInstance attributes
    that were removed in AF3 (`ti.operator`, `ti.execution_date`,
    `ti.task_instance_key_str`, plus the `task_instance.*` aliases).

    Patterns driven from
    `af3_migrations.yaml::forbidden_ti_attr_patterns`. 1.0 only when
    every DAG file is clean.

    Comments and docstrings are stripped before the regex scan (same
    helper as `no_deprecated_context_vars`): a legit migration whose
    explanatory comment or docstring NAMES a removed attr
    (`# dag_run.execution_date was removed in AF3; use logical_date`)
    must not trip the metric, while real attribute access still does."""
    dags = Path(task_dir) / _DAGS_SUBDIR
    files = sorted(dags.rglob("*.py"))
    if not files:
        return PrimitiveOutcome(value=0.0, passed=False)
    patterns = forbidden_ti_attr_patterns()
    clean = sum(1 for p in files if _file_free_of_ti_attr_patterns(p, patterns))
    value = clean / len(files)
    return PrimitiveOutcome(value=value, passed=value == 1.0)


def _file_free_of_ti_attr_patterns(path: Path, patterns: tuple[re.Pattern[str], ...]) -> bool:
    """Like `_file_free_of_patterns` but blanks `#` comments and real
    docstrings first, so prose that merely names a removed attr does
    not count as a violation. Uses `_strip_comments_and_docstrings_inplace`
    rather than the shared `strip_comments_and_docstrings`: the TI-attr
    patterns are receiver-prefixed (`\\bti\\.operator\\b`,
    `\\.execution_date\\b`) and depend on the original inter-token
    spacing, which the shared (token-concatenating) stripper collapses
    (`return ti.operator` -> `returnti.operator`, killing the leading
    word boundary). The in-place variant preserves every code character
    and only overwrites comment / docstring spans with spaces.

    Falls back to the raw text when the stripper refuses (partial /
    unparseable Python) — same defence-in-depth as
    `_has_deprecated_context_var`: an unparseable file should not be
    silently treated as clean."""
    text = read_text_capped(path)
    if text is None:
        return False
    code = _strip_comments_and_docstrings_inplace(text)
    if code is None:
        code = text
    return not any(pattern.search(code) for pattern in patterns)


def _strip_comments_and_docstrings_inplace(source: str) -> str | None:
    """Return `source` with `#` comments and real docstrings
    (Module/ClassDef/FunctionDef/AsyncFunctionDef first-statement string
    expressions) overwritten by spaces, preserving the exact length,
    column offsets, and line layout of every other character.

    This is the position-preserving sibling of
    `strip_comments_and_docstrings`: the shared helper rebuilds the
    source by concatenating token strings, which is fine for bare-word
    patterns scanned inside surviving string literals
    (`ctx['execution_date']`) but collapses the inter-token whitespace
    that receiver-prefixed patterns (`\\bti\\.operator\\b`) rely on.
    Here we mutate a char buffer in place instead, so real code — and
    its spacing — is scanned untouched.

    Regular string literals (dict values, lambda bodies, annotations,
    `if`-block strings) are kept intact, matching the shared helper's
    contract: `ctx['execution_date']` is a real usage and must still be
    scanned. Returns None on parse / tokenise failure so the caller can
    fall back to the raw scan."""
    import io
    import tokenize

    try:
        tree = ast.parse(source)
    except (SyntaxError, ValueError):
        return None
    docstring_positions: set[tuple[int, int]] = set()
    docstring_owners = (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)
    for node in ast.walk(tree):
        if not isinstance(node, docstring_owners):
            continue
        body = getattr(node, "body", None)
        if not body:
            continue
        first = body[0]
        if (
            isinstance(first, ast.Expr)
            and isinstance(first.value, ast.Constant)
            and isinstance(first.value.value, str)
        ):
            docstring_positions.add((first.value.lineno, first.value.col_offset))
    try:
        tokens = list(tokenize.generate_tokens(io.StringIO(source).readline))
    except (tokenize.TokenizeError, IndentationError, SyntaxError):
        return None
    lines = source.splitlines(keepends=True)
    buffer = [list(line) for line in lines]
    for tok in tokens:
        ttype, _tstr, start, end, _line = tok
        is_comment = ttype == tokenize.COMMENT
        is_docstring = ttype == tokenize.STRING and start in docstring_positions
        if not (is_comment or is_docstring):
            continue
        _blank_span(buffer, start, end)
    return "".join("".join(row) for row in buffer)


def _blank_span(buffer: list[list[str]], start: tuple[int, int], end: tuple[int, int]) -> None:
    """Overwrite the half-open span [start, end) in `buffer` (a list of
    per-line char lists, 1-indexed rows) with spaces, leaving newlines
    in place so line offsets are preserved."""
    start_row, start_col = start
    end_row, end_col = end
    for row in range(start_row, end_row + 1):
        line = buffer[row - 1]
        col_lo = start_col if row == start_row else 0
        col_hi = end_col if row == end_row else len(line)
        for col in range(col_lo, min(col_hi, len(line))):
            if line[col] != "\n":
                line[col] = " "


@scoring_primitive("triggers_outside_bundle")
def triggers_outside_bundle(
    task_dir: str, agent_output: str, *, oracle_dir: str | None = None
) -> PrimitiveOutcome:
    """Fraction of files under `dags/` that do NOT define a Trigger or
    AirflowPlugin subclass, and do not import `airflow.triggers.base`.

    In AF3 the triggerer process runs without any dag-bundle on
    `sys.path`, so trigger modules (and anything they transitively
    import) must live outside the bundle — typically in a sibling
    `triggers/` package installed on the worker's base path. Same rule
    for `AirflowPlugin` subclasses. Detection regex sourced from the
    KB entry `af3-triggers-plugins-outside-bundle` via
    `af3_migrations.yaml::in_bundle_forbidden_pattern`.

    Oracle-aware linkage: when the oracle's solution moves a trigger to
    a sibling package, the agent's DAGs must still reference it.
    "Absence in `dags/`" alone accepts "delete the trigger" as a valid
    fix; linkage rejects that."""
    dags = Path(task_dir) / _DAGS_SUBDIR
    files = sorted(dags.rglob("*.py"))
    if not files:
        return PrimitiveOutcome(value=0.0, passed=False)
    pattern = in_bundle_forbidden_pattern()
    clean = sum(1 for p in files if _file_clean_of_pattern(p, pattern))
    value = clean / len(files)
    if clean != len(files):
        return PrimitiveOutcome(value=value, passed=False)
    if not _oracle_expects_sibling_trigger(oracle_dir, pattern):
        return PrimitiveOutcome(value=value, passed=True)
    skip = sibling_skip_dirs()
    found = _find_sibling_with_trigger(Path(task_dir), skip, pattern)
    if found is None:
        return PrimitiveOutcome(value=0.0, passed=False)
    sibling_name, module_stems, relative_imports = found
    if not _dags_import_from_sibling(files, sibling_name, module_stems, relative_imports):
        return PrimitiveOutcome(value=0.0, passed=False)
    return PrimitiveOutcome(value=value, passed=True)


def _oracle_expects_sibling_trigger(oracle_dir: str | None, pattern: re.Pattern[str]) -> bool:
    """True when the oracle's solution defines a Trigger subclass in a
    sibling package (not in `solution/dags/`)."""
    if oracle_dir is None:
        return False
    solution = Path(oracle_dir) / "solution"
    if not solution.is_dir():
        return False
    skip = sibling_skip_dirs()
    for child in sorted(solution.iterdir()):
        if not child.is_dir() or child.name in skip:
            continue
        for py in child.rglob("*.py"):
            text = read_text_capped(py)
            if text is None:
                continue
            if pattern.search(text):
                return True
    return False


def _find_sibling_with_trigger(
    root: Path, skip: frozenset[str], pattern: re.Pattern[str]
) -> tuple[str, frozenset[str], frozenset[str]] | None:
    """`(sibling_dir_name, frozenset of trigger-defining module stems,
    frozenset of relative-path import strings)` for the first
    top-level sibling whose files define a Trigger/Plugin subclass.

    Three import shapes the linkage check then accepts:

    - `from <sibling>.<X>` — sibling-as-Python-package convention
      (works when the sibling has `__init__.py` and the project
      root is on sys.path).
    - `from <stem>` — Airflow `plugins/` convention. The plugins
      folder is on sys.path at scheduler / worker / triggerer
      startup, so trigger modules under it are importable by their
      bare stem without `plugins.` prefix.
    - `from <relative.dotted.path>` — descendent-import convention,
      common on Astro Runtime where `include/` is on sys.path. A
      trigger at `include/triggers/http_trigger.py` is then
      importable as `from triggers.http_trigger import …` because
      `include/` itself is the sys.path entry. The relative-import
      string for each trigger file (`triggers.http_trigger`) is
      tracked so the linkage check accepts that shape too.

    Returns None when no sibling has the pattern."""
    for child in sorted(root.iterdir()):
        if not child.is_dir() or child.name in skip:
            continue
        stems: set[str] = set()
        relative_imports: set[str] = set()
        for py in child.rglob("*.py"):
            text = read_text_capped(py)
            if text is None:
                continue
            if not pattern.search(text):
                continue
            stems.add(py.stem)
            # Relative-path-as-import: e.g. include/triggers/http_trigger.py
            # within sibling `include` → "triggers.http_trigger".
            rel = py.relative_to(child).with_suffix("")
            parts = [p for p in rel.parts if p != "__init__"]
            if parts:
                relative_imports.add(".".join(parts))
        if stems:
            return (child.name, frozenset(stems), frozenset(relative_imports))
    return None


def _dags_import_from_sibling(
    dag_files: list[Path],
    sibling_name: str,
    module_stems: frozenset[str],
    relative_imports: frozenset[str],
) -> bool:
    """Any DAG file imports the trigger by one of the three accepted
    shapes (see `_find_sibling_with_trigger` for the conventions):
    `<sibling>.<X>`, `<stem>` (plugins-folder bare stem), or
    `<relative.dotted.path>` (descendent of an on-sys.path
    sibling)."""
    for path in dag_files:
        text = read_text_capped(path)
        if text is None:
            continue
        try:
            tree = ast.parse(text)
        except (SyntaxError, ValueError):
            continue
        for node in ast.walk(tree):
            for module in _import_modules(node):
                if module == sibling_name or module.startswith(f"{sibling_name}."):
                    return True
                top = module.split(".", 1)[0]
                if top in module_stems:
                    return True
                if module in relative_imports or any(
                    module.startswith(f"{ri}.") for ri in relative_imports
                ):
                    return True
    return False
