"""Scoring primitives for the astronomer-cosmos 0.x → 1.x migration.

cosmos 1.0 collapsed a 47-entry import-path rename + a constructor
reshape (top-level kwargs → ProjectConfig / ProfileConfig). Later
1.x point releases added silent behavioural changes — the 1.6
default-parser flip, the 1.7 DatasetAlias rename, the 1.9
install_deps default flip — that don't fail at import time but
silently change a project's behaviour. The primitives here cover
the import / constructor cliff plus the static signals from those
silent transitions.

Data lives in `scoring/data/cosmos_idioms.yaml`. Adding a new
breaking change is a YAML edit; only a new *detection shape* needs
a Python change."""

from __future__ import annotations

import ast
import re
from collections.abc import Iterable
from pathlib import Path

from packaging.version import InvalidVersion, Version

from de_bench.upgrade_v2_scoring.data import cosmos_idioms, sibling_skip_dirs
from de_bench.upgrade_v2_scoring.primitives._common import read_text_capped
from de_bench.upgrade_v2_scoring.primitives.dag_factory import (
    _find_requirement,
    _requirement_active,
    _specifier_admits_floor,
    _target_python,
)
from de_bench.upgrade_v2_scoring.primitives.upgrades import _collect_sibling_files
from de_bench.upgrade_v2_scoring.registry import PrimitiveOutcome, scoring_primitive

_DAGS_SUBDIR = "dags"
_REQUIREMENTS_FILE = "requirements.txt"


def _project_py_files(task_dir: str) -> list[Path]:
    skip = sibling_skip_dirs()
    root = Path(task_dir)
    return sorted((root / _DAGS_SUBDIR).rglob("*.py")) + _collect_sibling_files(root, skip)


def _parse_files(files: Iterable[Path]) -> list[tuple[Path, ast.AST]]:
    """Parse each file or skip silently. Returning a list lets each
    primitive walk the parsed tree once without re-reading."""
    out: list[tuple[Path, ast.AST]] = []
    for path in files:
        text = read_text_capped(path)
        if text is None:
            continue
        try:
            tree = ast.parse(text)
        except (SyntaxError, ValueError):
            continue
        out.append((path, tree))
    return out


@scoring_primitive("cosmos_imports_post_1_0")
def cosmos_imports_post_1_0(
    task_dir: str, agent_output: str, *, oracle_dir: str | None = None
) -> PrimitiveOutcome:
    """No project file imports from a pre-1.0 cosmos namespace
    (`cosmos.providers.dbt.*`, `cosmos.dag.*`, `cosmos.task_group.*`).
    The 1.0 release moved every symbol — keeping a legacy import
    raises ImportError at module load.

    Oracle-aware: when the oracle's solution imports cosmos and the
    candidate doesn't, the candidate has deleted the cosmos surface
    rather than migrating it. Fail-closed instead of returning
    `passed=None`. `passed=None` only when neither candidate nor
    oracle imports cosmos at all."""
    forbidden_prefixes = tuple(cosmos_idioms()["imports"]["forbidden_module_prefixes"])
    files = _project_py_files(task_dir)
    relevant_files, clean_files = _grade_cosmos_imports(files, forbidden_prefixes)
    if relevant_files > 0:
        value = clean_files / relevant_files
        return PrimitiveOutcome(value=value, passed=value == 1.0)
    oracle_relevant, _ = _grade_cosmos_imports(_oracle_py_files(oracle_dir), forbidden_prefixes)
    if oracle_relevant > 0:
        return PrimitiveOutcome(value=0.0, passed=False)
    return PrimitiveOutcome(value=0.0, passed=None)


def _grade_cosmos_imports(
    files: list[Path], forbidden_prefixes: tuple[str, ...]
) -> tuple[int, int]:
    relevant = 0
    clean = 0
    for path in files:
        text = read_text_capped(path)
        if text is None:
            continue
        try:
            tree = ast.parse(text)
        except (SyntaxError, ValueError):
            continue
        cosmos_modules = list(_cosmos_imported_modules(tree))
        if not cosmos_modules:
            continue
        relevant += 1
        if not any(_starts_with_any(m, forbidden_prefixes) for m in cosmos_modules):
            clean += 1
    return relevant, clean


def _cosmos_imported_modules(tree: ast.AST) -> Iterable[str]:
    """Every cosmos-namespace path an `import` statement touches.

    For `from cosmos.foo import bar`, both `cosmos.foo` AND
    `cosmos.foo.bar` are yielded — the latter catches submodule
    imports like `from cosmos.providers import dbt` that would
    otherwise leave the module string `cosmos.providers` (which
    doesn't match a `cosmos.providers.dbt` forbidden prefix).
    Yielding the alias-attached path closes that evasion."""
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module:
            module = node.module
            if module == "cosmos" or module.startswith("cosmos."):
                yield module
                for alias in node.names:
                    yield f"{module}.{alias.name}"
        elif isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name == "cosmos" or alias.name.startswith("cosmos."):
                    yield alias.name


def _starts_with_any(value: str, prefixes: tuple[str, ...]) -> bool:
    return any(value == p or value.startswith(f"{p}.") for p in prefixes)


@scoring_primitive("dbtdag_uses_projectconfig")
def dbtdag_uses_projectconfig(
    task_dir: str, agent_output: str, *, oracle_dir: str | None = None
) -> PrimitiveOutcome:
    """Every `DbtDag(...)` / `DbtTaskGroup(...)` construction passes
    a `project_config=` kwarg. PR #389 moved the project-shape
    arguments (root path, project name, …) into a `ProjectConfig`
    object; the constructor without it raises `TypeError` at module
    load.

    Oracle-aware: fail-closed when the oracle constructs DbtDag and
    the candidate has none — the agent has deleted the surface."""
    classes = frozenset(cosmos_idioms()["constructors"]["dbtdag_classes"])
    return _grade_kwarg_required(task_dir, oracle_dir, classes, required_kwarg="project_config")


@scoring_primitive("dbtdag_uses_profileconfig")
def dbtdag_uses_profileconfig(
    task_dir: str, agent_output: str, *, oracle_dir: str | None = None
) -> PrimitiveOutcome:
    """Every `DbtDag(...)` / `DbtTaskGroup(...)` construction passes
    a `profile_config=` kwarg. PR #389 moved the connection /
    profile-args arguments into a `ProfileConfig` object.

    Oracle-aware: fail-closed when the oracle constructs DbtDag and
    the candidate has none."""
    classes = frozenset(cosmos_idioms()["constructors"]["dbtdag_classes"])
    return _grade_kwarg_required(task_dir, oracle_dir, classes, required_kwarg="profile_config")


def _count_construction_with_kwarg(
    files: list[Path], classes: frozenset[str], required_kwarg: str
) -> tuple[int, int]:
    """(relevant, clean): construction sites whose func name is in
    `classes`, and the subset that pass `required_kwarg`."""
    relevant = 0
    clean = 0
    for _, tree in _parse_files(files):
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            if _call_class_name(node.func) not in classes:
                continue
            relevant += 1
            kwargs = {kw.arg for kw in node.keywords if kw.arg}
            if required_kwarg in kwargs:
                clean += 1
    return relevant, clean


def _grade_kwarg_required(
    task_dir: str,
    oracle_dir: str | None,
    classes: frozenset[str],
    required_kwarg: str,
) -> PrimitiveOutcome:
    """Oracle-aware count parity: the candidate must construct at
    least as many `classes` sites as the oracle's solution, and
    every candidate site must carry `required_kwarg`. Without the
    parity check, the agent can delete one of two DbtDag sites,
    keep the other migrated, and pass — which silently drops a DAG
    the source had. `passed=None` only when neither side constructs
    the class."""
    candidate_relevant, candidate_clean = _count_construction_with_kwarg(
        _project_py_files(task_dir), classes, required_kwarg
    )
    oracle_relevant, _ = _count_construction_with_kwarg(
        _oracle_py_files(oracle_dir), classes, required_kwarg
    )
    if oracle_relevant == 0 and candidate_relevant == 0:
        return PrimitiveOutcome(value=0.0, passed=None)
    if candidate_relevant < oracle_relevant:
        return PrimitiveOutcome(value=0.0, passed=False)
    if candidate_relevant == 0:
        return PrimitiveOutcome(value=0.0, passed=False)
    value = candidate_clean / candidate_relevant
    return PrimitiveOutcome(value=value, passed=value == 1.0)


def _count_construction_clean_of_kwargs(
    files: list[Path], classes: frozenset[str], forbidden: frozenset[str]
) -> tuple[int, int]:
    """(relevant, clean): construction sites whose func name is in
    `classes`, and the subset that pass *no* forbidden kwarg."""
    relevant = 0
    clean = 0
    for _, tree in _parse_files(files):
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            if _call_class_name(node.func) not in classes:
                continue
            relevant += 1
            kwargs = {kw.arg for kw in node.keywords if kw.arg}
            if not (kwargs & forbidden):
                clean += 1
    return relevant, clean


def _count_construction(files: list[Path], class_name: str) -> int:
    """Construction sites whose func name resolves to `class_name`."""
    total = 0
    for _, tree in _parse_files(files):
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and _call_class_name(node.func) == class_name:
                total += 1
    return total


def _call_class_name(func: ast.AST) -> str | None:
    if isinstance(func, ast.Name):
        return func.id
    if isinstance(func, ast.Attribute):
        return func.attr
    return None


@scoring_primitive("dbtdag_no_pre_1_0_kwargs")
def dbtdag_no_pre_1_0_kwargs(
    task_dir: str, agent_output: str, *, oracle_dir: str | None = None
) -> PrimitiveOutcome:
    """No `DbtDag(...)` / `DbtTaskGroup(...)` call passes a removed
    pre-1.0 kwarg (`dbt_root_path`, `dbt_project_name`, `conn_id`,
    `profile_args`, `profile_name_override`, `target_name_override`,
    `dbt_args`). The 1.0 release moved each of these into the
    Project/Profile config objects; passing them at the top level
    raises `TypeError`.

    Oracle-aware count parity: candidate must construct at least
    as many DbtDag sites as the oracle solution does, and each
    site must be free of forbidden kwargs."""
    spec = cosmos_idioms()["constructors"]
    classes = frozenset(spec["dbtdag_classes"])
    forbidden = frozenset(spec["forbidden_dbtdag_kwargs"])
    candidate_relevant, candidate_clean = _count_construction_clean_of_kwargs(
        _project_py_files(task_dir), classes, forbidden
    )
    oracle_relevant, _ = _count_construction_clean_of_kwargs(
        _oracle_py_files(oracle_dir), classes, forbidden
    )
    if oracle_relevant == 0 and candidate_relevant == 0:
        return PrimitiveOutcome(value=0.0, passed=None)
    if candidate_relevant < oracle_relevant:
        return PrimitiveOutcome(value=0.0, passed=False)
    if candidate_relevant == 0:
        return PrimitiveOutcome(value=0.0, passed=False)
    value = candidate_clean / candidate_relevant
    return PrimitiveOutcome(value=value, passed=value == 1.0)


@scoring_primitive("render_config_explicit_load_mode")
def render_config_explicit_load_mode(
    task_dir: str, agent_output: str, *, oracle_dir: str | None = None
) -> PrimitiveOutcome:
    """Every `RenderConfig(...)` construction passes an explicit
    load-mode kwarg (`load_method=` or `load_mode=`). cosmos 1.6
    flipped the default — `dbt ls` now runs on every DAG parse
    when `profile_config` is set unless the caller pins a mode.
    Without an explicit kwarg the project's parse behaviour
    silently changes between 1.5 and 1.6+.

    Oracle-aware on both presence and value: candidate must
    construct RenderConfig at the same count as the oracle, every
    site must carry an explicit load kwarg, and the value must
    match the oracle's choice (so an agent can't switch from
    `LoadMode.CUSTOM` to `LoadMode.DBT_LS` and silently change
    parse-time behaviour while still satisfying the metric).

    Call-site bound (mirrors `downstream_uses_dataset_alias`): only a
    RenderConfig actually passed as the `render_config=` kwarg of a
    `DbtDag` / `DbtTaskGroup` construction counts — inline
    (`render_config=RenderConfig(...)`) or via a name bound to one
    (`rc = RenderConfig(...); ... render_config=rc`). A free-standing
    or dead `_DECOY = RenderConfig(load_mode=...)` that's never wired
    into a DAG does not count, so it can't satisfy the metric while the
    real construction goes unmigrated."""
    explicit_kwargs = frozenset(cosmos_idioms()["render_config"]["explicit_load_kwargs"])
    return _grade_kwarg_value_matches_oracle(
        task_dir, oracle_dir, "RenderConfig", explicit_kwargs, binding_kwarg="render_config"
    )


@scoring_primitive("project_config_explicit_install_deps")
def project_config_explicit_install_deps(
    task_dir: str, agent_output: str, *, oracle_dir: str | None = None
) -> PrimitiveOutcome:
    """Every `ProjectConfig(...)` construction passes an explicit
    `install_deps=` kwarg whose value matches the oracle's
    solution. cosmos 1.9 flipped the default from `False` to
    `True`; without an explicit setting, projects that upgrade
    from 1.8.x silently start running `dbt deps` on every DAG
    parse, which can hit private package indexes the cluster
    can't reach. The value match prevents an agent from satisfying
    "explicit kwarg present" while flipping the value the wrong
    way."""
    required = cosmos_idioms()["project_config"]["explicit_install_deps_kwarg"]
    return _grade_kwarg_value_matches_oracle(
        task_dir, oracle_dir, "ProjectConfig", frozenset({required})
    )


def _grade_kwarg_value_matches_oracle(
    task_dir: str,
    oracle_dir: str | None,
    class_name: str,
    accepted_kwarg_names: frozenset[str],
    *,
    binding_kwarg: str | None = None,
) -> PrimitiveOutcome:
    """Grade `<class_name>(...)` constructions against the oracle:
    same count, every site carries one of `accepted_kwarg_names`,
    and the value (as an AST literal or `Attribute` reference)
    matches a value the oracle solution uses on the same kwarg.
    Falls back to "any value" when the oracle's value is dynamic
    (variable reference) — we can't compare opaque expressions
    statically.

    When `binding_kwarg` is set, only constructions of `class_name`
    that are passed as that kwarg of a DbtDag / DbtTaskGroup call
    (inline or via an intermediate name) count — a dead, unwired
    construction is ignored on both the candidate and oracle side."""
    # Value matching uses the *solution* tree as the reference —
    # the source typically lacks the explicit kwarg, so the
    # un-migrated tree gives the wrong answer for value parity.
    oracle_relevant, oracle_values = _collect_class_kwarg_values(
        _oracle_solution_py_files(oracle_dir), class_name, accepted_kwarg_names, binding_kwarg
    )
    candidate_relevant, candidate_values = _collect_class_kwarg_values(
        _project_py_files(task_dir), class_name, accepted_kwarg_names, binding_kwarg
    )
    if oracle_relevant == 0 and candidate_relevant == 0:
        return PrimitiveOutcome(value=0.0, passed=None)
    if candidate_relevant < oracle_relevant or candidate_relevant == 0:
        return PrimitiveOutcome(value=0.0, passed=False)
    # Each candidate site must carry one of the accepted kwargs.
    sites_with_kwarg = sum(1 for v in candidate_values if v is not _NO_VALUE)
    if sites_with_kwarg < candidate_relevant:
        return PrimitiveOutcome(value=sites_with_kwarg / candidate_relevant, passed=False)
    # Oracle dictates which values are acceptable. Skip value match
    # when the oracle's value is dynamic (we keep `None` in the set
    # to mark "any value"); otherwise require every candidate
    # value to appear in the oracle set.
    oracle_concrete = {v for v in oracle_values if v not in (_NO_VALUE, None)}
    oracle_has_dynamic = any(v is None for v in oracle_values)
    if not oracle_concrete and oracle_has_dynamic:
        return PrimitiveOutcome(value=1.0, passed=True)
    if not oracle_concrete:
        return PrimitiveOutcome(value=1.0, passed=True)
    candidate_concrete = {v for v in candidate_values if v not in (_NO_VALUE, None)}
    candidate_dynamic = any(v is None for v in candidate_values)
    if candidate_dynamic and not oracle_has_dynamic:
        # Candidate hides the value behind a variable; can't verify
        # it matches. Fail closed — explicit literal is the contract.
        return PrimitiveOutcome(value=0.0, passed=False)
    if candidate_concrete and not (candidate_concrete <= oracle_concrete):
        return PrimitiveOutcome(value=0.0, passed=False)
    return PrimitiveOutcome(value=1.0, passed=True)


_NO_VALUE = object()


def _collect_class_kwarg_values(
    files: list[Path],
    class_name: str,
    kwarg_names: frozenset[str],
    binding_kwarg: str | None = None,
) -> tuple[int, list]:
    """(relevant_count, values): every construction site of
    `class_name` and the value passed to one of `kwarg_names` at
    that site (sentinel `_NO_VALUE` when no accepted kwarg is
    present, `None` when the value is dynamic / non-literal,
    otherwise a hashable string form of the literal / attribute
    reference).

    When `binding_kwarg` is set, a construction site only counts if
    it is wired into a DbtDag / DbtTaskGroup call via that kwarg —
    either inline as the kwarg value, or assigned to a name that is
    later passed as the kwarg. Dead / free-standing constructions are
    skipped."""
    relevant = 0
    values: list = []
    for _, tree in _parse_files(files):
        if binding_kwarg is not None:
            bound = _bound_construction_calls(tree, class_name, binding_kwarg)
        else:
            bound = None
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            if _call_class_name(node.func) != class_name:
                continue
            if bound is not None and node not in bound:
                continue
            relevant += 1
            value: object = _NO_VALUE
            for kw in node.keywords:
                if kw.arg in kwarg_names:
                    value = _value_signature(kw.value)
                    break
            values.append(value)
    return relevant, values


def _bound_construction_calls(tree: ast.AST, class_name: str, binding_kwarg: str) -> set[ast.Call]:
    """The set of `class_name(...)` Call nodes in `tree` that are
    passed as `binding_kwarg=` of a DbtDag / DbtTaskGroup
    construction — inline (`render_config=RenderConfig(...)`) or via a
    name bound to one (`rc = RenderConfig(...); ... render_config=rc`).

    Two passes: first map every module-level name that's assigned a
    `class_name(...)` call to that call node; then walk DbtDag /
    DbtTaskGroup constructions and resolve each `binding_kwarg=` value
    to its underlying call (inline Call, or a Name we mapped)."""
    name_to_call: dict[str, ast.Call] = {}
    for node in ast.walk(tree):
        if not isinstance(node, ast.Assign):
            continue
        if not (
            isinstance(node.value, ast.Call) and _call_class_name(node.value.func) == class_name
        ):
            continue
        for target in node.targets:
            if isinstance(target, ast.Name):
                name_to_call[target.id] = node.value
    bound: set[ast.Call] = set()
    for node in ast.walk(tree):
        if not (isinstance(node, ast.Call) and _is_dag_construction(node)):
            continue
        for kw in node.keywords:
            if kw.arg != binding_kwarg:
                continue
            value = kw.value
            if isinstance(value, ast.Call) and _call_class_name(value.func) == class_name:
                bound.add(value)
            elif isinstance(value, ast.Name) and value.id in name_to_call:
                bound.add(name_to_call[value.id])
    return bound


def _value_signature(node: ast.AST) -> str | None:
    """A hashable representation of `node` suitable for cross-site
    equality. Literals serialise to their `repr`; attribute chains
    (e.g. `LoadMode.CUSTOM`) serialise to the dotted name. Anything
    else (variable reference, function call) returns `None` —
    "dynamic, can't compare statically"."""
    if isinstance(node, ast.Constant):
        return repr(node.value)
    if isinstance(node, ast.Attribute):
        parts: list[str] = [node.attr]
        current = node.value
        while isinstance(current, ast.Attribute):
            parts.append(current.attr)
            current = current.value
        if isinstance(current, ast.Name):
            parts.append(current.id)
            return ".".join(reversed(parts))
        return None
    if isinstance(node, ast.Name):
        # Variable reference — dynamic.
        return None
    return None


_DATASET_ALIAS_DISABLE_PATTERN = re.compile(
    # Match the env-var name followed within ~80 chars by a False / 0
    # literal. Covers `os.environ['X'] = 'False'`,
    # `os.environ.setdefault('X', 'False')`, `ENV X=False` (Dockerfile),
    # `X: False` (yaml), and `X=False` (.env). The bounded distance
    # prevents false positives where the env-var name appears in a
    # docstring near an unrelated `False` literal.
    r"AIRFLOW__COSMOS__ENABLE_DATASET_ALIAS\b[^a-zA-Z\n]{0,80}[\"']?(?:False|0)[\"']?\b"
)


def _project_disables_dataset_alias(task_dir: str) -> bool:
    """True when the project sets `AIRFLOW__COSMOS__ENABLE_DATASET_ALIAS=False`
    somewhere — `os.environ[...] = "False"` in a .py file, an `ENV` line
    in a Dockerfile, or a YAML/dotenv config. Disabling the alias is a
    documented Cosmos config knob (`cosmos.settings.enable_dataset_alias`)
    that pins the producer back to bare-Dataset emission, so downstream
    DAGs scheduled on `Dataset(...)` keep firing without an API rewrite.
    The bench accepts either the API rewrite OR the env-var shim; both
    preserve the observable behaviour the migration is meant to protect."""
    root = Path(task_dir)
    candidates: list[Path] = []
    for sub in (_DAGS_SUBDIR, "include"):
        if (root / sub).is_dir():
            candidates.extend((root / sub).rglob("*.py"))
    for name in ("Dockerfile", "requirements.txt", ".env", "airflow_settings.yaml"):
        path = root / name
        if path.is_file():
            candidates.append(path)
    for path in candidates:
        text = read_text_capped(path)
        if text is None:
            continue
        if _DATASET_ALIAS_DISABLE_PATTERN.search(text):
            return True
    return False


@scoring_primitive("downstream_uses_dataset_alias")
def downstream_uses_dataset_alias(
    task_dir: str, agent_output: str, *, oracle_dir: str | None = None
) -> PrimitiveOutcome:
    """Every DAG schedule that consumes a Cosmos-emitted dataset
    URI either (a) uses `DatasetAlias(...)` instead of bare
    `Dataset(...)`, or (b) the project sets
    `AIRFLOW__COSMOS__ENABLE_DATASET_ALIAS=False` to keep the
    producer pinned to bare-Dataset emission. cosmos 1.7
    (PRs #1217, #1240) switched dataset emission to alias form
    on AF >= 2.10; downstream DAGs scheduled on the bare URI silently
    stop firing because the producing task no longer emits the same
    dataset object. Either rewrite (DatasetAlias) or pin
    (env-var) is a valid migration — the bench accepts both.

    Oracle-aware: when the source fixture's DAGs schedule on
    `Dataset(...)`, the candidate must EITHER rewrite at least one
    to `DatasetAlias(...)` OR set the disable env-var. A blanket
    `passed=None` would let the agent delete the schedule entirely
    — silent behavioural regression, opposite of the migration goal.

    `passed=None` only when neither the candidate nor the oracle
    references either class."""
    spec = cosmos_idioms()["datasets"]
    legacy = spec["legacy_dataset_class"]
    required = spec["required_dataset_alias_class"]

    candidate_files = _project_py_files(task_dir)
    candidate_legacy, candidate_alias = _count_dataset_usage(candidate_files, legacy, required)
    oracle_files = _oracle_py_files(oracle_dir)
    oracle_legacy, oracle_alias = _count_dataset_usage(oracle_files, legacy, required)

    if candidate_legacy == 0 and candidate_alias == 0 and oracle_legacy == 0 and oracle_alias == 0:
        return PrimitiveOutcome(value=0.0, passed=None)
    # Env-var shim path: project disables the alias generation
    # entirely. Producer keeps emitting bare Dataset, downstream
    # bare-Dataset schedules keep firing — equivalent migration.
    # Still requires the candidate to NOT have lost the legacy
    # schedule (deletion attack); count parity is what's preserved.
    if _project_disables_dataset_alias(task_dir):
        if candidate_legacy >= oracle_legacy:
            return PrimitiveOutcome(value=1.0, passed=True)
        return PrimitiveOutcome(value=0.0, passed=False)
    # The candidate must ship at least as many alias schedules as
    # the *combined* oracle count — any oracle dataset schedule
    # (legacy or alias) corresponds to one downstream dependency
    # the migration must preserve. Counting only `oracle_legacy`
    # leaves the deletion attack open: drop both fixture files,
    # ship nothing, the metric falls through to passed=True.
    required_alias = oracle_legacy + oracle_alias
    if required_alias > 0 and candidate_alias < required_alias:
        return PrimitiveOutcome(value=0.0, passed=False)
    if candidate_legacy > 0:
        return PrimitiveOutcome(value=0.0, passed=False)
    return PrimitiveOutcome(value=1.0, passed=True)


def _oracle_py_files(oracle_dir: str | None) -> list[Path]:
    """Source-tree .py files at the oracle root. Used for
    *count parity* and *presence* checks — "did the source ship
    this construct" — where the un-migrated state is the right
    reference."""
    if oracle_dir is None:
        return []
    skip = sibling_skip_dirs()
    root = Path(oracle_dir)
    return sorted((root / _DAGS_SUBDIR).rglob("*.py")) + _collect_sibling_files(root, skip)


def _oracle_solution_py_files(oracle_dir: str | None) -> list[Path]:
    """Solution-tree .py files. Used for *value match* checks —
    the source typically lacks the explicit kwarg the migration
    asks for, so the comparison reference is what the hand-written
    answer key uses."""
    if oracle_dir is None:
        return []
    skip = sibling_skip_dirs()
    solution = Path(oracle_dir) / "solution"
    if not solution.is_dir():
        return []
    return sorted((solution / _DAGS_SUBDIR).rglob("*.py")) + _collect_sibling_files(solution, skip)


def _count_dataset_usage(files: list[Path], legacy_class: str, alias_class: str) -> tuple[int, int]:
    """Return (legacy_count, alias_count) — Call sites that appear
    inside a DAG `schedule=` / `schedule_interval=` keyword value.
    Restricting to that context closes the spoof attack where an
    agent removes the real schedule and drops an unused
    `DatasetAlias("junk")` somewhere else in the file to inflate
    the count."""
    legacy = 0
    alias = 0
    for _, tree in _parse_files(files):
        for schedule_value in _dag_schedule_values(tree):
            for node in ast.walk(schedule_value):
                if not isinstance(node, ast.Call):
                    continue
                name = _call_class_name(node.func)
                if name == legacy_class:
                    legacy += 1
                elif name == alias_class:
                    alias += 1
    return legacy, alias


def _dag_schedule_values(tree: ast.AST):
    """Yield every AST subtree passed as `schedule=` /
    `schedule_interval=` to a DAG / DbtDag / DbtTaskGroup
    construction or to a `@dag(...)` decorator."""
    schedule_kw_names = {"schedule", "schedule_interval"}
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and _is_dag_construction(node):
            for kw in node.keywords:
                if kw.arg in schedule_kw_names:
                    yield kw.value
        elif isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
            for deco in node.decorator_list:
                if isinstance(deco, ast.Call) and _is_dag_decorator(deco.func):
                    for kw in deco.keywords:
                        if kw.arg in schedule_kw_names:
                            yield kw.value


def _is_dag_construction(node: ast.Call) -> bool:
    return _call_class_name(node.func) in {"DAG", "DbtDag", "DbtTaskGroup"}


def _is_dag_decorator(func: ast.AST) -> bool:
    if isinstance(func, ast.Name):
        return func.id == "dag"
    if isinstance(func, ast.Attribute):
        return func.attr == "dag"
    return False


@scoring_primitive("dbt_local_operators_use_profileconfig")
def dbt_local_operators_use_profileconfig(
    task_dir: str, agent_output: str, *, oracle_dir: str | None = None
) -> PrimitiveOutcome:
    """Every standalone `Dbt*LocalOperator(...)` (and the Docker /
    Kubernetes / VirtualEnv variants) carries a `profile_config=`
    kwarg and none of the removed pre-1.0 kwargs (`conn_id`,
    `profile_args`). The 1.0 release reshaped these operators the
    same way it reshaped DbtDag — without the metric, an agent can
    leave the standalone operator on the legacy shape while
    migrating just the DbtDag, and the rest of the scoring set
    can't see it.

    Oracle-aware count parity. `passed=None` only when neither
    side constructs any Dbt operator."""
    forbidden = frozenset(cosmos_idioms()["constructors"]["forbidden_dbtdag_kwargs"])
    candidate_relevant, candidate_clean = _grade_dbt_operator_calls(
        _project_py_files(task_dir), forbidden
    )
    oracle_relevant, _ = _grade_dbt_operator_calls(_oracle_py_files(oracle_dir), forbidden)
    if oracle_relevant == 0 and candidate_relevant == 0:
        return PrimitiveOutcome(value=0.0, passed=None)
    if candidate_relevant < oracle_relevant or candidate_relevant == 0:
        return PrimitiveOutcome(value=0.0, passed=False)
    value = candidate_clean / candidate_relevant
    return PrimitiveOutcome(value=value, passed=value == 1.0)


def _grade_dbt_operator_calls(files: list[Path], forbidden: frozenset[str]) -> tuple[int, int]:
    """Construction sites of any Dbt-prefixed operator (excluding
    DbtDag / DbtTaskGroup, which have their own metric) that carry a
    real `profile_config` and avoid every forbidden pre-1.0 kwarg.
    Class names are matched by suffix because cosmos ships a family
    per execution mode (LocalOperator, DockerOperator,
    KubernetesOperator, VirtualEnvOperator, AwsEksOperator, …).

    Identity-bound: the operator name must resolve to a `cosmos.*`
    import binding in the same file. A locally-defined shadow class
    (`class DbtRunLocalOperator(EmptyOperator): ...`) shares the
    name but is not the cosmos operator — it doesn't count, so an
    agent can't dodge the reshape by stubbing a same-named class.

    Value-bound: `profile_config=` must be a real `ProfileConfig(...)`
    call or a name bound to one. A literal `profile_config=None`
    satisfies "kwarg present" but reproduces the pre-config behaviour
    and is rejected."""
    excluded = {"DbtDag", "DbtTaskGroup"}
    relevant = 0
    clean = 0
    for _, tree in _parse_files(files):
        cosmos_operators = _cosmos_operator_names(tree)
        profile_config_names = _profileconfig_bound_names(tree)
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            name = _call_class_name(node.func)
            if not name or not name.startswith("Dbt") or not name.endswith("Operator"):
                continue
            if name in excluded:
                continue
            if name not in cosmos_operators:
                # Same-named local shadow class — not the cosmos operator.
                continue
            relevant += 1
            kwargs = {kw.arg for kw in node.keywords if kw.arg}
            if kwargs & forbidden:
                continue
            if _has_real_profile_config(node, profile_config_names):
                clean += 1
    return relevant, clean


def _cosmos_operator_names(tree: ast.AST) -> set[str]:
    """Names (local binding, including `as` aliases) introduced by an
    `import` from a `cosmos.*` module. Used to confirm a Dbt-prefixed
    operator call actually resolves to a cosmos import rather than a
    locally-defined shadow class."""
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module:
            if node.module == "cosmos" or node.module.startswith("cosmos."):
                for alias in node.names:
                    names.add(alias.asname or alias.name)
        elif isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name == "cosmos" or alias.name.startswith("cosmos."):
                    # `import cosmos.operators.local as X` binds the alias;
                    # bare `import cosmos.operators.local` binds `cosmos`.
                    names.add(alias.asname or alias.name.split(".")[0])
    return names


def _profileconfig_bound_names(tree: ast.AST) -> set[str]:
    """Module-level names assigned a `ProfileConfig(...)` call, so a
    `profile_config=profile_config` reference resolves to a real
    config object rather than a `None` literal."""
    names: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Assign):
            continue
        if not (
            isinstance(node.value, ast.Call)
            and _call_class_name(node.value.func) == "ProfileConfig"
        ):
            continue
        for target in node.targets:
            if isinstance(target, ast.Name):
                names.add(target.id)
    return names


def _has_real_profile_config(node: ast.Call, profile_config_names: set[str]) -> bool:
    """True when `node` passes `profile_config=` and the value is a
    real config — an inline `ProfileConfig(...)` call or a name bound
    to one. A literal `None` (or absent kwarg) is rejected."""
    for kw in node.keywords:
        if kw.arg != "profile_config":
            continue
        value = kw.value
        if isinstance(value, ast.Call) and _call_class_name(value.func) == "ProfileConfig":
            return True
        return isinstance(value, ast.Name) and value.id in profile_config_names
    return False


@scoring_primitive("cosmos_pin_uplifted")
def cosmos_pin_uplifted(
    task_dir: str, agent_output: str, *, oracle_dir: str | None = None
) -> PrimitiveOutcome:
    """`requirements.txt` pins `astronomer-cosmos` at or above the
    1.14 floor *and* the pin actually installs on the task's target
    Python. Pre-1.x pins still install but every shape under test
    breaks at import time (cosmos.providers.dbt is gone).

    Uses `packaging.requirements.Requirement` for full PEP 508
    parsing (markers, contradictory specifier sets, empty pins all
    fail closed). `passed=None` only when no `astronomer-cosmos`
    line is present at all."""
    floor_str = cosmos_idioms()["min_version"]
    try:
        floor = Version(floor_str)
    except InvalidVersion:
        return PrimitiveOutcome(value=0.0, passed=False)
    req_path = Path(task_dir) / _REQUIREMENTS_FILE
    text = read_text_capped(req_path) if req_path.is_file() else None
    if not text:
        return PrimitiveOutcome(value=0.0, passed=None)
    target_python = _target_python(oracle_dir)
    requirement = _find_requirement(text, _is_cosmos_name)
    if requirement is None:
        return PrimitiveOutcome(value=0.0, passed=None)
    if not _requirement_active(requirement, target_python):
        return PrimitiveOutcome(value=0.0, passed=False)
    if not _specifier_admits_floor(requirement, floor):
        return PrimitiveOutcome(value=0.0, passed=False)
    return PrimitiveOutcome(value=1.0, passed=True)


def _is_cosmos_name(name: str) -> bool:
    """PEP 503 canonicalised match for the cosmos distribution.
    Both `astronomer-cosmos` and `astronomer_cosmos` are accepted —
    pip canonicalises them identically."""
    canonical = name.lower().replace("_", "-")
    return canonical == "astronomer-cosmos"
