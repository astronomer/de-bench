"""Scoring primitives for the DAG Authoring category.

Oracle-aware primitives compare the agent-modified `task_dir/dags/` tree
against the oracle tree at `oracle_dir/solution/dags/` (passed in by
the runner — NOT inside `task_dir`, so the agent can't discover it).

Migration-specific data (deprecated kwargs, removed symbols) is loaded
from `airflow_bench.scoring.data.af3_migrations.yaml`.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

from de_bench.upgrade_v2_scoring.data import deprecated_kwargs, removed_symbols
from de_bench.upgrade_v2_scoring.primitives._common import read_text_capped
from de_bench.upgrade_v2_scoring.primitives._strip import strip_comments_and_docstrings
from de_bench.upgrade_v2_scoring.registry import PrimitiveOutcome, scoring_primitive

_DAGS_SUBDIR = "dags"


@scoring_primitive("dag_has_module_docstring")
def dag_has_module_docstring(
    task_dir: str, agent_output: str, *, oracle_dir: str | None = None
) -> PrimitiveOutcome:
    """Fraction of `dags/*.py` files whose first statement is a
    module-level string literal (a docstring). 1.0 only when every
    file has one.

    Used by authoring tasks that ask the agent to add a docstring
    describing what a DAG does — `parse_ok` already passes on a
    syntactically-valid file, so it can't grade the docstring on
    its own."""
    dags = Path(task_dir) / _DAGS_SUBDIR
    files = sorted(dags.rglob("*.py"))
    if not files:
        return PrimitiveOutcome(value=0.0, passed=False)
    documented = sum(1 for p in files if _has_module_docstring(p))
    value = documented / len(files)
    return PrimitiveOutcome(value=value, passed=value == 1.0)


def _has_module_docstring(path: Path) -> bool:
    text = read_text_capped(path)
    if text is None:
        return False
    try:
        tree = ast.parse(text)
    except (SyntaxError, ValueError):
        return False
    if not tree.body:
        return False
    first = tree.body[0]
    if not isinstance(first, ast.Expr):
        return False
    return isinstance(first.value, ast.Constant) and isinstance(first.value.value, str)


@scoring_primitive("import_ok")
def import_ok(
    task_dir: str, agent_output: str, *, oracle_dir: str | None = None
) -> PrimitiveOutcome:
    """Every `from X import Y` / `import X` in agent DAGs references a
    module path the AST can walk — no syntax errors, no relative imports
    that reach outside the task directory."""
    dags = Path(task_dir) / _DAGS_SUBDIR
    files = sorted(dags.rglob("*.py"))
    if not files:
        return PrimitiveOutcome(value=0.0, passed=False)
    for path in files:
        text = read_text_capped(path)
        if text is None:
            return PrimitiveOutcome(value=0.0, passed=False)
        try:
            tree = ast.parse(text)
        except (SyntaxError, ValueError):
            return PrimitiveOutcome(value=0.0, passed=False)
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.level > 0:
                return PrimitiveOutcome(value=0.0, passed=False)
    return PrimitiveOutcome(value=1.0, passed=True)


@scoring_primitive("structural_match_taskids")
def structural_match_taskids(
    task_dir: str, agent_output: str, *, oracle_dir: str | None = None
) -> PrimitiveOutcome:
    """Jaccard similarity between the set of `task_id=` literals in the
    agent's DAGs and in the oracle solution (`oracle_dir/solution/dags/`).
    1.0 on exact match; 0.0 when either side has zero ids and the
    other has some.

    `oracle_dir` is the unmodified source task directory. When None
    (e.g. floor-baseline scoring), the metric has no reference to
    compare against and returns `passed=None`."""
    if oracle_dir is None:
        return PrimitiveOutcome(value=0.0, passed=None)
    agent = _collect_task_ids(Path(task_dir) / _DAGS_SUBDIR)
    oracle = _collect_task_ids(Path(oracle_dir) / "solution" / _DAGS_SUBDIR)
    score = _jaccard(agent, oracle)
    passed = score == 1.0
    return PrimitiveOutcome(value=score, passed=passed)


@scoring_primitive("task_group_matches_oracle")
def task_group_matches_oracle(
    task_dir: str, agent_output: str, *, oracle_dir: str | None = None
) -> PrimitiveOutcome:
    """If the oracle's solution uses `TaskGroup` in its DAGs, the
    agent's DAGs must too. Guards the AF2 `SubDagOperator` -> AF3
    `TaskGroup` refactor: without this, an agent could delete
    grouping entirely (or replace it with flat tasks) and still
    satisfy `structural_match_taskids`.

    `passed=None` when the oracle's solution has no TaskGroup — the
    task shape does not require grouping."""
    if oracle_dir is None:
        return PrimitiveOutcome(value=0.0, passed=None)
    oracle_uses = _dags_reference_symbol(Path(oracle_dir) / "solution" / _DAGS_SUBDIR, "TaskGroup")
    if not oracle_uses:
        return PrimitiveOutcome(value=0.0, passed=None)
    agent_uses = _dags_reference_symbol(Path(task_dir) / _DAGS_SUBDIR, "TaskGroup")
    return PrimitiveOutcome(value=1.0 if agent_uses else 0.0, passed=agent_uses)


def _dags_reference_symbol(root: Path, symbol: str) -> bool:
    """Any file under `root` imports or calls `symbol` as an AST name."""
    if not root.is_dir():
        return False
    for path in sorted(root.rglob("*.py")):
        text = read_text_capped(path)
        if text is None:
            continue
        try:
            tree = ast.parse(text)
        except (SyntaxError, ValueError):
            continue
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                if any(alias.name == symbol for alias in node.names):
                    return True
            elif (isinstance(node, ast.Name) and node.id == symbol) or (
                isinstance(node, ast.Attribute) and node.attr == symbol
            ):
                return True
    return False


# Body kwargs that carry a task's actual runtime work. When present
# on an operator call, their value must be non-trivial — these are
# the kwargs an empty-stub dodge replaces with `lambda: None` or
# `bash_command=""`. Extend as new operator shapes get added to the
# bench.
_BODY_KWARGS: frozenset[str] = frozenset({"python_callable", "bash_command", "sql"})
# String body values that signal a no-op stub. Stripped, lowercased,
# compared as-is.
_TRIVIAL_STR_BODIES: frozenset[str] = frozenset({"", ":", "true", "exit 0", "echo", "pass", "noop"})


def _operator_call_name(call: ast.Call) -> str | None:
    """Return the operator class name of a Call node, e.g.
    `BashOperator` from `BashOperator(...)` or
    `airflow.providers...BashOperator(...)`."""
    func = call.func
    if isinstance(func, ast.Name):
        return func.id
    if isinstance(func, ast.Attribute):
        return func.attr
    return None


def _body_is_trivial(value: ast.expr) -> bool:
    """True when the body kwarg's value is a recognisable no-op
    stub: empty/echo/`true` strings; `lambda` returning None / a
    constant / a bare arg; `None` literal."""
    if isinstance(value, ast.Constant):
        if value.value is None:
            return True
        if isinstance(value.value, str):
            stripped = value.value.strip().lower()
            return stripped in _TRIVIAL_STR_BODIES
    if isinstance(value, ast.Lambda):
        body = value.body
        if isinstance(body, ast.Constant):
            return True  # lambda: None / lambda: 0 / lambda: ""
        if isinstance(body, ast.Name):
            arg_names = {a.arg for a in value.args.args}
            if body.id in arg_names:
                return True  # lambda x: x — identity
    return False


@scoring_primitive("task_bodies_non_trivial")
def task_bodies_non_trivial(
    task_dir: str, agent_output: str, *, oracle_dir: str | None = None
) -> PrimitiveOutcome:
    """Every operator call in the agent's DAGs whose body kwarg is
    one of `python_callable` / `bash_command` / `sql` carries a
    non-trivial value.

    Catches the empty-stub dodge: an agent that preserves task_ids
    but replaces real callables with `lambda: None` or real bash
    commands with `bash_command=""` would pass
    `structural_match_taskids` (literal id match) and
    `fixture_taskids_preserved_plus_count` (id presence + count)
    while breaking observable behaviour. Pair with one of those
    metrics under `critical:` so the gate covers both id
    preservation AND body non-triviality.

    Oracle-aware: when the oracle's solution has operator calls
    carrying body kwargs but the candidate has none, the candidate
    has stripped the real task bodies — e.g. swapped every
    body-bearing operator for an `EmptyOperator` that preserves
    task_ids but carries no `python_callable` / `bash_command` /
    `sql`. That would otherwise drop the metric to `passed=None` and
    dodge the critical gate by attrition (confirmed gameable: an
    EmptyOperator stub preserving task_ids scored 1.0). With this
    guard, a candidate that ships no body kwargs on a task whose
    oracle has them fails the metric.

    `passed=None` only when neither oracle nor candidate has any
    operator call with a body kwarg — the task shape doesn't have
    anything to grade. Otherwise pass iff every detected body kwarg
    value is non-trivial. Named callables (e.g.
    `python_callable=process_data`) are accepted without chasing the
    function definition; lambdas and string literals are checked
    inline."""
    dags = Path(task_dir) / _DAGS_SUBDIR
    if not dags.is_dir():
        if _oracle_has_body_kwargs(oracle_dir):
            return PrimitiveOutcome(value=0.0, passed=False)
        return PrimitiveOutcome(value=0.0, passed=None)
    body_calls_seen = 0
    for path in sorted(dags.rglob("*.py")):
        text = read_text_capped(path)
        if text is None:
            continue
        try:
            tree = ast.parse(text)
        except (SyntaxError, ValueError):
            return PrimitiveOutcome(value=0.0, passed=False)
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            name = _operator_call_name(node)
            if name is None or not name.endswith("Operator"):
                continue
            for kw in node.keywords:
                if kw.arg in _BODY_KWARGS:
                    body_calls_seen += 1
                    if _body_is_trivial(kw.value):
                        return PrimitiveOutcome(value=0.0, passed=False)
    if body_calls_seen == 0:
        if _oracle_has_body_kwargs(oracle_dir):
            return PrimitiveOutcome(value=0.0, passed=False)
        return PrimitiveOutcome(value=0.0, passed=None)
    return PrimitiveOutcome(value=1.0, passed=True)


def _oracle_has_body_kwargs(oracle_dir: str | None) -> bool:
    """True when the oracle's solution `dags/` (falling back to its
    source `dags/`) contains an operator call carrying one of the
    body kwargs — the same relevance signal the candidate side uses.
    Holds the oracle and candidate to the same standard so an oracle
    with no body-bearing operators can't force a candidate to ship
    one."""
    if oracle_dir is None:
        return False
    root = Path(oracle_dir)
    solution_dags = root / "solution" / _DAGS_SUBDIR
    search_dags = solution_dags if solution_dags.is_dir() else root / _DAGS_SUBDIR
    if not search_dags.is_dir():
        return False
    for path in sorted(search_dags.rglob("*.py")):
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
            name = _operator_call_name(node)
            if name is None or not name.endswith("Operator"):
                continue
            if any(kw.arg in _BODY_KWARGS for kw in node.keywords):
                return True
    return False


@scoring_primitive("fixture_taskids_preserved_plus_count")
def fixture_taskids_preserved_plus_count(
    task_dir: str, agent_output: str, *, oracle_dir: str | None = None
) -> PrimitiveOutcome:
    """Every `task_id=` literal in the untouched source fixture's
    `dags/` tree is still present in the agent's `dags/` tree AND the
    total count of agent task ids matches the oracle solution's count.

    Name-agnostic structural check: useful when the prompt must not
    leak the specific name of a new task the agent is asked to add.
    `structural_match_taskids` would require that name to appear in
    the solution and the agent's output — this check lets the agent
    pick any name as long as they (a) don't rename the existing task
    and (b) add the expected number of new tasks."""
    if oracle_dir is None:
        return PrimitiveOutcome(value=0.0, passed=None)
    agent = _collect_task_ids(Path(task_dir) / _DAGS_SUBDIR)
    fixture = _collect_task_ids(Path(oracle_dir) / _DAGS_SUBDIR)
    oracle = _collect_task_ids(Path(oracle_dir) / "solution" / _DAGS_SUBDIR)
    preserved = fixture.issubset(agent)
    count_matches = len(agent) == len(oracle)
    passed = preserved and count_matches
    return PrimitiveOutcome(value=1.0 if passed else 0.0, passed=passed)


@scoring_primitive("no_deprecated_usage")
def no_deprecated_usage(
    task_dir: str, agent_output: str, *, oracle_dir: str | None = None
) -> PrimitiveOutcome:
    """Fraction of files with no deprecated-in-AF3 kwargs or removed
    operator symbols (from `af3_migrations.yaml`). 1.0 only when every
    file is clean."""
    dags = Path(task_dir) / _DAGS_SUBDIR
    files = sorted(dags.rglob("*.py"))
    if not files:
        return PrimitiveOutcome(value=0.0, passed=False)
    clean = sum(1 for p in files if not _has_deprecated_usage(p))
    value = clean / len(files)
    return PrimitiveOutcome(value=value, passed=value == 1.0)


def _has_deprecated_usage(path: Path) -> bool:
    text = read_text_capped(path)
    if text is None:
        # Oversized or unreadable — can't vouch for it. Treat as dirty.
        return True
    # Scan code only. A comment or docstring naming a removed symbol is
    # almost always an agent explaining the migration it just performed
    # ("`airflow.datasets.Dataset` is now `airflow.sdk.Asset`"), or a
    # workspace docstring it inherited and had no reason to reword —
    # neither is deprecated *usage*. `upgrades.py` already strips before
    # its equivalent scans; this one was missing it, and penalized
    # correct migrations for documenting themselves.
    code = strip_comments_and_docstrings(text)
    if code is None:
        # Unparseable — the stripper can't vouch for it, so fall back to
        # the raw text rather than silently passing a file we can't read.
        code = text
    if any(re.search(rf"\b{kw}\s*=", code) for kw in deprecated_kwargs()):
        return True
    return any(re.search(rf"\b{sym}\b", code) for sym in removed_symbols())


def _taskflow_task_id(node: ast.FunctionDef | ast.AsyncFunctionDef) -> str | None:
    """The task id a `@task`-decorated function declares, or None if the
    function isn't a TaskFlow task.

    TaskFlow spells the task id as the function name, and `@task(task_id=...)`
    overrides it — neither shape carries a bare `task_id="..."` kwarg on an
    operator call, so a literal-only scan counts these real tasks as zero.
    Matches `@task`, `@task(...)`, and dotted forms like `@task.virtualenv`
    or `@decorators.task`, which are all the same decorator."""
    for dec in node.decorator_list:
        call = dec if isinstance(dec, ast.Call) else None
        ref = call.func if call is not None else dec
        # Unwrap `task.virtualenv` / `airflow.decorators.task` to the name
        # that decides whether this is the TaskFlow decorator at all.
        root = ref
        while isinstance(root, ast.Attribute):
            root = root.value
        is_task = (isinstance(ref, ast.Name) and ref.id == "task") or (
            isinstance(ref, ast.Attribute)
            and (ref.attr == "task" or (isinstance(root, ast.Name) and root.id == "task"))
        )
        if not is_task:
            continue
        if call is not None:
            for kw in call.keywords:
                if (
                    kw.arg == "task_id"
                    and isinstance(kw.value, ast.Constant)
                    and isinstance(kw.value.value, str)
                ):
                    return kw.value.value
        return node.name
    return None


def _collect_task_ids(root: Path) -> set[str]:
    """Walk .py files under `root`, find every task id: both
    `task_id="..."` literals on operator calls and TaskFlow
    `@task`-decorated functions."""
    if not root.is_dir():
        return set()
    out: set[str] = set()
    for path in sorted(root.rglob("*.py")):
        text = read_text_capped(path)
        if text is None:
            continue
        try:
            tree = ast.parse(text)
        except (SyntaxError, ValueError):
            continue
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                for kw in node.keywords:
                    if (
                        kw.arg == "task_id"
                        and isinstance(kw.value, ast.Constant)
                        and isinstance(kw.value.value, str)
                    ):
                        out.add(kw.value.value)
            elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                tid = _taskflow_task_id(node)
                if tid is not None:
                    out.add(tid)
    return out


def _jaccard(a: set[str], b: set[str]) -> float:
    # J(∅, ∅) = 1 (both empty is a match, not undefined); a single empty
    # side is 0 (no overlap possible).
    if not a and not b:
        return 1.0
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)
