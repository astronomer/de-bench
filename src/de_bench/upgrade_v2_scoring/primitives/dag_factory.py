"""Scoring primitives for the dag-factory 0.x → 1.0 migration.

dag-factory 1.0 is a from-scratch rewrite of the public API: the
`DagFactory` class went private, every loader call rebrands to
`load_yaml_dags`, and a dozen YAML shapes either renamed or
disappeared. The migration spans both Python loaders and the YAML
config tree, so the primitives here mix AST checks (loaders) with
text/structural YAML checks (configs).

Data lives in `scoring/data/dag_factory_idioms.yaml`. Adding a new
breaking change is a YAML edit; only a new *detection shape* (e.g. a
new file type) requires a Python change here. See
`docs/interpreting-results.md` for the category-level write-up."""

from __future__ import annotations

import ast
import re
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import yaml
from packaging.requirements import InvalidRequirement, Requirement
from packaging.specifiers import InvalidSpecifier, SpecifierSet
from packaging.version import InvalidVersion, Version

from de_bench.upgrade_v2_scoring.data import dag_factory_idioms, sibling_skip_dirs
from de_bench.upgrade_v2_scoring.primitives._common import read_text_capped
from de_bench.upgrade_v2_scoring.primitives.upgrades import _collect_sibling_files
from de_bench.upgrade_v2_scoring.registry import PrimitiveOutcome, scoring_primitive

_DAGS_SUBDIR = "dags"
_REQUIREMENTS_FILE = "requirements.txt"
_YAML_SUFFIXES = (".yml", ".yaml")
# Default marker environment when the task ships no python pin. We
# can't reason about Python-version markers without something to
# evaluate them against, and the corpus's dag-factory tasks pin
# 3.11; this matches the worst case where the agent would gain by
# hiding behind a marker.
_DEFAULT_PYTHON = "3.11"


def _project_py_files(task_dir: str) -> list[Path]:
    skip = sibling_skip_dirs()
    root = Path(task_dir)
    return sorted((root / _DAGS_SUBDIR).rglob("*.py")) + _collect_sibling_files(root, skip)


def _project_yaml_files(task_dir: str) -> list[Path]:
    dags = Path(task_dir) / _DAGS_SUBDIR
    if not dags.is_dir():
        return []
    files: list[Path] = []
    for suffix in _YAML_SUFFIXES:
        files.extend(dags.rglob(f"*{suffix}"))
    return sorted(files)


def _oracle_yaml_files(oracle_dir: str | None) -> list[Path]:
    """Source-fixture YAMLs at the oracle root. The oracle ships
    with the un-migrated state; primitives that need to know
    whether a shape was *originally* present (KPO, custom timetable,
    provider operator) read the oracle's `dags/` rather than the
    candidate's working tree. Lets the primitive fail-closed when
    the agent deletes a problematic block instead of migrating it."""
    if oracle_dir is None:
        return []
    dags = Path(oracle_dir) / _DAGS_SUBDIR
    if not dags.is_dir():
        return []
    files: list[Path] = []
    for suffix in _YAML_SUFFIXES:
        files.extend(dags.rglob(f"*{suffix}"))
    return sorted(files)


def _oracle_yaml_has_substring(oracle_dir: str | None, substrings: tuple[str, ...]) -> bool:
    """True when any substring appears in the oracle's source-tree
    YAML. Substring matching is intentional — oracle YAMLs may be
    legacy-tagged and unparseable, so we can't walk them
    structurally. The substrings are operator dotted names
    (`KubernetesPodOperator`) and structural keys
    (`timetable:`) — both are unambiguous in YAML context."""
    for path in _oracle_yaml_files(oracle_dir):
        text = read_text_capped(path)
        if text is None:
            continue
        if any(s in text for s in substrings):
            return True
    return False


def _safe_load_yaml(path: Path) -> Any | None:
    """Return parsed YAML or `None` when the file can't be parsed.
    Legacy `!and` / `!or` / `!join` tags raise yaml.YAMLError under
    SafeLoader — the structural checks fall back to text scans for
    those, but the parse-required checks (timeouts, KPO, timetable)
    can correctly report 'no shape to grade' on un-migrated YAML."""
    text = read_text_capped(path)
    if text is None:
        return None
    try:
        return yaml.safe_load(text)
    except yaml.YAMLError:
        return None


def _walk_dicts(node: Any) -> Iterator[dict]:
    """Yield every nested dict — depth-first, parents before children."""
    if isinstance(node, dict):
        yield node
        for value in node.values():
            yield from _walk_dicts(value)
    elif isinstance(node, list):
        for item in node:
            yield from _walk_dicts(item)


def _dag_blocks(parsed: Any) -> list[dict]:
    """Top-level DAG config dicts. dag-factory's YAML format is
    `<dag_name>: {<config>}` at the root with a small set of reserved
    keys (`default`, `defaults`) carrying file-wide defaults rather
    than DAGs."""
    if not isinstance(parsed, dict):
        return []
    return [
        v for k, v in parsed.items() if isinstance(v, dict) and k not in {"default", "defaults"}
    ]


@scoring_primitive("loader_uses_load_yaml_dags")
def loader_uses_load_yaml_dags(
    task_dir: str, agent_output: str, *, oracle_dir: str | None = None
) -> PrimitiveOutcome:
    """Project's loader code uses `load_yaml_dags` and contains no
    surviving `DagFactory(...)` constructor or `.generate_dags(...)`
    call. Both halves matter: an agent that adds the new symbol
    without ripping out the old one will TypeError at module load.

    Oracle-aware: when the oracle's solution uses a dag-factory
    loader (`load_yaml_dags`, or the legacy `DagFactory(...)` /
    `.generate_dags(...)` it migrated from) but the candidate has NO
    dag-factory loader at all, return `passed=False`. An agent that
    deletes the generated DAGs and the loader (stubbing in plain
    DAGs) would otherwise drop the metric to `passed=None` and dodge
    the critical gate by attrition — confirmed gameable on
    proj-7b7c4f and proj-dgfe01.

    `passed=None` only when neither the candidate nor the oracle
    references any dag-factory loader symbol — the metric does not
    apply to that task shape."""
    idioms = dag_factory_idioms()["loader"]
    forbidden_class = idioms["forbidden_class"]
    forbidden_methods = frozenset(idioms["forbidden_methods"])
    required = idioms["required_symbol"]

    files = _project_py_files(task_dir)
    saw_required = False
    saw_forbidden = False
    for path in files:
        text = read_text_capped(path)
        if text is None:
            continue
        try:
            tree = ast.parse(text)
        except (SyntaxError, ValueError):
            return PrimitiveOutcome(value=0.0, passed=False)
        verdict = _scan_loader(tree, forbidden_class, forbidden_methods, required)
        if verdict.required:
            saw_required = True
        if verdict.forbidden:
            saw_forbidden = True
    if not saw_required and not saw_forbidden:
        if _oracle_uses_loader(oracle_dir, forbidden_class, forbidden_methods, required):
            return PrimitiveOutcome(value=0.0, passed=False)
        return PrimitiveOutcome(value=0.0, passed=None)
    passed = saw_required and not saw_forbidden
    return PrimitiveOutcome(value=1.0 if passed else 0.0, passed=passed)


def _oracle_uses_loader(
    oracle_dir: str | None,
    forbidden_class: str,
    forbidden_methods: frozenset[str],
    required: str,
) -> bool:
    """True when the oracle's solution tree (or its task tree, if no
    explicit solution) references a dag-factory loader symbol — the
    migrated `load_yaml_dags` or the legacy `DagFactory` /
    `.generate_dags` it started from. Uses the same scan as the
    candidate side, so both are held to the same standard."""
    if oracle_dir is None:
        return False
    root = Path(oracle_dir)
    solution = root / "solution"
    search_root = solution if solution.is_dir() else root
    for path in _project_py_files(str(search_root)):
        text = read_text_capped(path)
        if text is None:
            continue
        try:
            tree = ast.parse(text)
        except (SyntaxError, ValueError):
            continue
        verdict = _scan_loader(tree, forbidden_class, forbidden_methods, required)
        if verdict.required or verdict.forbidden:
            return True
    return False


class _LoaderVerdict:
    __slots__ = ("forbidden", "required")

    def __init__(self) -> None:
        self.required = False
        self.forbidden = False


def _scan_loader(
    tree: ast.AST, forbidden_class: str, forbidden_methods: frozenset[str], required: str
) -> _LoaderVerdict:
    verdict = _LoaderVerdict()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module == "dagfactory":
            for alias in node.names:
                if alias.name == required:
                    verdict.required = True
                if alias.name == forbidden_class:
                    verdict.forbidden = True
        if isinstance(node, ast.Call):
            name = _call_name(node.func)
            if name == required:
                verdict.required = True
            if name == forbidden_class:
                verdict.forbidden = True
            if name in forbidden_methods:
                verdict.forbidden = True
    return verdict


def _call_name(func: ast.AST) -> str | None:
    if isinstance(func, ast.Name):
        return func.id
    if isinstance(func, ast.Attribute):
        return func.attr
    return None


@scoring_primitive("loader_kwarg_renames")
def loader_kwarg_renames(
    task_dir: str, agent_output: str, *, oracle_dir: str | None = None
) -> PrimitiveOutcome:
    """No `load_yaml_dags`/`DagFactory` call passes a deprecated
    kwarg (`default_args_config_dict`, `default_args_config_path`,
    `config`). 1.0 renamed every one of them; the old names raise
    TypeError at module load.

    `passed=None` when the project has no Python file calling either
    loader symbol."""
    renamed = dag_factory_idioms()["loader"]["renamed_kwargs"]
    deprecated = frozenset(renamed.keys())
    target_names = frozenset({dag_factory_idioms()["loader"]["required_symbol"], "DagFactory"})

    files = _project_py_files(task_dir)
    relevant = 0
    clean = 0
    for path in files:
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
            if _call_name(node.func) not in target_names:
                continue
            relevant += 1
            kw_names = {kw.arg for kw in node.keywords if kw.arg}
            if not (kw_names & deprecated):
                clean += 1
    if relevant == 0:
        return PrimitiveOutcome(value=0.0, passed=None)
    value = clean / relevant
    return PrimitiveOutcome(value=value, passed=value == 1.0)


@scoring_primitive("loader_no_clean_dags")
def loader_no_clean_dags(
    task_dir: str, agent_output: str, *, oracle_dir: str | None = None
) -> PrimitiveOutcome:
    """No surviving `clean_dags(...)` call anywhere in project code.
    The method was removed in 1.0 with no in-code replacement;
    keeping the call AttributeErrors at runtime once the factory
    object goes private.

    `passed=None` when the project has no .py files at all (no
    surface to grade); otherwise the metric grades pass-or-fail."""
    removed = frozenset(dag_factory_idioms()["loader"]["removed_methods"])
    files = _project_py_files(task_dir)
    if not files:
        return PrimitiveOutcome(value=0.0, passed=None)
    for path in files:
        text = read_text_capped(path)
        if text is None:
            continue
        try:
            tree = ast.parse(text)
        except (SyntaxError, ValueError):
            return PrimitiveOutcome(value=0.0, passed=False)
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and _call_name(node.func) in removed:
                return PrimitiveOutcome(value=0.0, passed=False)
    return PrimitiveOutcome(value=1.0, passed=True)


@scoring_primitive("yaml_uses_schedule_not_schedule_interval")
def yaml_uses_schedule_not_schedule_interval(
    task_dir: str, agent_output: str, *, oracle_dir: str | None = None
) -> PrimitiveOutcome:
    """Every dag-factory YAML DAG block uses `schedule:` (or
    `timetable:`); none use the legacy `schedule_interval:`.

    `passed=None` when the project has no YAML under `dags/`."""
    forbidden = frozenset(dag_factory_idioms()["yaml"]["forbidden_dag_keys"])
    files = _project_yaml_files(task_dir)
    if not files:
        return PrimitiveOutcome(value=0.0, passed=None)
    relevant = 0
    clean = 0
    for path in files:
        parsed = _safe_load_yaml(path)
        if parsed is None:
            # Un-parseable YAML is by definition not migrated (legacy
            # tags or actual syntax errors). Don't pretend it passed.
            return PrimitiveOutcome(value=0.0, passed=False)
        for block in _dag_blocks(parsed):
            relevant += 1
            keys_in_block = set(_collect_keys(block))
            if forbidden & keys_in_block:
                continue
            if "schedule" in block or "timetable" in block:
                clean += 1
    if relevant == 0:
        return PrimitiveOutcome(value=0.0, passed=None)
    value = clean / relevant
    return PrimitiveOutcome(value=value, passed=value == 1.0)


def _collect_keys(node: Any) -> Iterator[str]:
    """All dict keys at any depth — used to spot a legacy key
    sitting inside `default_args:` or `task_defaults:`."""
    for d in _walk_dicts(node):
        yield from d.keys()


@scoring_primitive("yaml_no_sec_shortcut_kwargs")
def yaml_no_sec_shortcut_kwargs(
    task_dir: str, agent_output: str, *, oracle_dir: str | None = None
) -> PrimitiveOutcome:
    """No YAML file uses any `*_sec` / `*_secs` shortcut key. Every
    one was removed in 1.0 (PR #512); the value must move to the
    real timeout key with a `__type__: datetime.timedelta` block.

    `passed=None` when the project has no YAML under `dags/`."""
    forbidden = frozenset(dag_factory_idioms()["yaml"]["forbidden_sec_shortcut_keys"])
    files = _project_yaml_files(task_dir)
    if not files:
        return PrimitiveOutcome(value=0.0, passed=None)
    for path in files:
        parsed = _safe_load_yaml(path)
        if parsed is None:
            # Fall back to text-scan: legacy YAML may carry custom
            # tags (`!and`) the loader rejects, but we can still
            # decide on shortcut keys against the raw text.
            text = read_text_capped(path) or ""
            if any(re.search(rf"(?m)^\s*{re.escape(k)}\s*:", text) for k in forbidden):
                return PrimitiveOutcome(value=0.0, passed=False)
            continue
        if forbidden & set(_collect_keys(parsed)):
            return PrimitiveOutcome(value=0.0, passed=False)
    return PrimitiveOutcome(value=1.0, passed=True)


@scoring_primitive("yaml_timeouts_use_timedelta")
def yaml_timeouts_use_timedelta(
    task_dir: str, agent_output: str, *, oracle_dir: str | None = None
) -> PrimitiveOutcome:
    """Every timeout-shaped key (`dagrun_timeout`, `retry_delay`,
    `sla`, `execution_delta`, `execution_timeout`) is either absent
    or expressed as a `__type__: datetime.timedelta` dict. PR #512
    rejected bare ints / floats for these keys.

    `passed=None` when no DAG block uses any of the timeout keys —
    the metric does not apply."""
    timeout_keys = frozenset(dag_factory_idioms()["yaml"]["timeout_keys"])
    files = _project_yaml_files(task_dir)
    if not files:
        return PrimitiveOutcome(value=0.0, passed=None)
    relevant = 0
    clean = 0
    for path in files:
        parsed = _safe_load_yaml(path)
        if parsed is None:
            return PrimitiveOutcome(value=0.0, passed=False)
        for d in _walk_dicts(parsed):
            for key, value in d.items():
                if key not in timeout_keys:
                    continue
                relevant += 1
                if _is_timedelta_block(value):
                    clean += 1
    if relevant == 0:
        return PrimitiveOutcome(value=0.0, passed=None)
    value = clean / relevant
    return PrimitiveOutcome(value=value, passed=value == 1.0)


_TIMEDELTA_KWARGS = frozenset(
    {"days", "seconds", "microseconds", "milliseconds", "minutes", "hours", "weeks"}
)


def _is_timedelta_block(value: Any) -> bool:
    """A `__type__: datetime.timedelta` block whose constructor
    arguments resolve to a meaningful duration. dag-factory treats
    `__args__: [60]` as a positional call (`timedelta(60)` = 60
    days), so a primitive that only checks the `__type__` marker
    accepts a 60-second-to-60-day silent regression. Require the
    block to either omit `__args__` (meaningless without it) and
    pass at least one timedelta keyword field, or pass an
    `__args__: []` together with kwargs."""
    if not isinstance(value, dict):
        return False
    type_marker = value.get("__type__")
    if not isinstance(type_marker, str) or not type_marker.endswith("timedelta"):
        return False
    has_kwarg = any(k in value for k in _TIMEDELTA_KWARGS)
    args = value.get("__args__")
    if args is None:
        return has_kwarg
    if not isinstance(args, list):
        return False
    # Empty `__args__: []` is a no-op call — the kwargs alongside
    # do the work. Non-empty positional args silently change the
    # unit (timedelta(60) = 60 days), so reject them.
    if not args:
        return has_kwarg
    return False


@scoring_primitive("yaml_logical_keys_consolidated")
def yaml_logical_keys_consolidated(
    task_dir: str, agent_output: str, *, oracle_dir: str | None = None
) -> PrimitiveOutcome:
    """No YAML file uses the legacy logical forms — neither the
    YAML tags (`!and`, `!or`, `!join`) anywhere in the file, nor
    the bare-alias keys (`and:`, `or:`, `join:`) when they appear
    *under* a `schedule:` / `schedule_interval:` mapping. PR #525
    consolidated all three onto the dunder-keyed shape.

    The context-aware bare-key check trades a tiny ambiguity (a
    dag-factory task literally named `and` under
    `tasks.<name>.schedule` — a path we don't think exists in
    practice) for closing the gap where a legacy
    `schedule: {and: [...]}` would otherwise pass. Tasks named
    `join` outside the schedule namespace are unaffected.

    Text-based tag detection — legacy YAML tags raise
    yaml.YAMLError under SafeLoader, so we scan raw bytes for
    those. The bare-key check uses safe_load since by definition
    those YAMLs parse. `passed=None` when the project has no
    YAML under `dags/`."""
    forbidden_tags = dag_factory_idioms()["yaml"]["forbidden_logical_tags"]
    bare_logical_keys = frozenset(tag.lstrip("!") for tag in forbidden_tags)
    schedule_keys = frozenset({"schedule", "schedule_interval"})
    files = _project_yaml_files(task_dir)
    if not files:
        return PrimitiveOutcome(value=0.0, passed=None)
    for path in files:
        text = read_text_capped(path)
        if text is None:
            continue
        if any(tag in text for tag in forbidden_tags):
            return PrimitiveOutcome(value=0.0, passed=False)
        parsed = _safe_load_yaml(path)
        if parsed is None:
            continue
        if _has_bare_logical_under_schedule(parsed, bare_logical_keys, schedule_keys):
            return PrimitiveOutcome(value=0.0, passed=False)
    return PrimitiveOutcome(value=1.0, passed=True)


def _has_bare_logical_under_schedule(
    node: Any, bare_logical: frozenset[str], schedule_keys: frozenset[str]
) -> bool:
    """True when any `schedule:` / `schedule_interval:` mapping in
    `node` contains a bare `and:` / `or:` / `join:` key. Walks
    every dict — the schedule key may live at the DAG level or
    inside a tasks block, both legitimate dag-factory positions."""
    for d in _walk_dicts(node):
        for key, value in d.items():
            if key not in schedule_keys:
                continue
            if isinstance(value, dict) and (set(value.keys()) & bare_logical):
                return True
    return False


@scoring_primitive("dag_factory_pin_uplifted")
def dag_factory_pin_uplifted(
    task_dir: str, agent_output: str, *, oracle_dir: str | None = None
) -> PrimitiveOutcome:
    """`requirements.txt` pins `dag-factory` at or above the 1.0
    floor *and* the pin actually installs on the task's target
    Python. Legacy 0.x pins still install but every shape under
    test fails import.

    Uses `packaging.requirements.Requirement` so a malformed line,
    a marker-disabled line (`dag-factory>=1.0; python_version <
    '3.11'` against a 3.11 target), or a contradictory specifier
    set (`>=1.0,<1.0`) all fail closed — bare regex extraction
    silently accepts these.

    `passed=None` only when no `dag-factory` line is present at
    all — the metric does not apply."""
    floor_str = dag_factory_idioms()["min_version"]
    try:
        floor = Version(floor_str)
    except InvalidVersion:
        return PrimitiveOutcome(value=0.0, passed=False)
    req_path = Path(task_dir) / _REQUIREMENTS_FILE
    text = read_text_capped(req_path) if req_path.is_file() else None
    if not text:
        return PrimitiveOutcome(value=0.0, passed=None)
    target_python = _target_python(oracle_dir)
    requirement = _find_requirement(text, _is_dag_factory_name)
    if requirement is None:
        return PrimitiveOutcome(value=0.0, passed=None)
    if not _requirement_active(requirement, target_python):
        return PrimitiveOutcome(value=0.0, passed=False)
    if not _specifier_admits_floor(requirement, floor):
        return PrimitiveOutcome(value=0.0, passed=False)
    return PrimitiveOutcome(value=1.0, passed=True)


def _is_dag_factory_name(name: str) -> bool:
    """PEP 503 canonicalised match for `dag-factory` /
    `dag_factory` / `Dag-Factory`."""
    return name.lower().replace("_", "-") == "dag-factory"


def _find_requirement(requirements_text: str, name_predicate) -> Requirement | None:
    """Walk `requirements.txt` lines, return the first parseable
    Requirement whose name matches `name_predicate`. Comments,
    blank lines, `-r`/`-c` includes, and unparseable lines are
    skipped silently — same as `pip install -r` resolves them."""
    for raw in requirements_text.splitlines():
        line = raw.split("#", 1)[0].strip()
        if not line or line.startswith("-"):
            continue
        try:
            requirement = Requirement(line)
        except InvalidRequirement:
            continue
        if name_predicate(requirement.name):
            return requirement
    return None


def _requirement_active(requirement: Requirement, target_python: str) -> bool:
    """A requirement with no marker is always active. With a
    marker, evaluate it against the target Python — a
    marker-disabled line on the target environment doesn't
    install."""
    marker = requirement.marker
    if marker is None:
        return True
    try:
        return marker.evaluate({"python_version": target_python})
    except Exception:
        # Malformed marker: don't crash the metric; fail-closed so
        # the agent doesn't gain from constructing a marker we
        # can't parse.
        return False


def _floor_probe_versions(spec: SpecifierSet, floor: Version) -> set[Version]:
    """A deterministic candidate set dense enough to pin a realistic
    semver SpecifierSet's lower and upper edges.

    The old probe set was a fixed five versions anchored on the floor
    (`floor`, `floor+micro`, `floor+minor`, `floor.major+1`, …), so a
    legitimate pin whose admitted versions fell *between* those rungs
    — `==1.14.2` with a `1.14.0` floor, `==1.16.0`, `~=1.14.2` — was
    scored as admitting nothing >= floor and failed. The fix is to
    seed the probe set with the exact versions the specifier names
    (plus their immediate neighbours) so every `==`/`~=`/`>=` edge is
    hit, and to lay a dense ladder across the realistic version range
    so a below-floor admission can't slip between rungs."""
    cands: set[Version] = set()
    # The versions the specifier itself names — closes the between-rungs
    # hole. `.*` wildcards strip to their base release.
    for clause in spec:
        base = clause.version.rstrip(".*")
        try:
            named = Version(base)
        except InvalidVersion:
            continue
        cands.add(named)
        release = [*named.release, 0, 0, 0]
        major, minor, micro = release[0], release[1], release[2]
        cands.add(Version(f"{major}.{minor}.{micro + 1}"))
        if micro > 0:
            cands.add(Version(f"{major}.{minor}.{micro - 1}"))
        cands.add(Version(f"{major}.{minor + 1}.0"))
    # Floor and its immediate predecessor — the boundary the rule turns on.
    cands.add(floor)
    if floor.micro > 0:
        cands.add(Version(f"{floor.major}.{floor.minor}.{floor.micro - 1}"))
    # Dense ladder spanning the realistic range around the floor.
    for major in range(0, floor.major + 3):
        max_minor = (floor.minor + 3) if major == floor.major else 30
        for minor in range(0, max_minor + 1):
            for micro in range(0, 4):
                cands.add(Version(f"{major}.{minor}.{micro}"))
    return cands


def _specifier_admits_floor(requirement: Requirement, floor: Version) -> bool:
    """A pin is "uplifted" iff its SpecifierSet admits at least one
    version AND admits no version below the floor — i.e. every version
    the specifier allows is >= floor.

    An empty specifier set (bare `dag-factory`) is rejected — pip would
    install the latest, which on a stale machine may be the legacy 0.x.
    A contradictory set (`>=1.0,<1.0`) admits nothing and fails. Any
    pin that admits a below-floor version (`==0.22.0`, `<1.14`, `>=1.0`,
    `!=1.14.0`) fails. `packaging` exposes no satisfiability / infimum
    API, so we decide via `SpecifierSet.contains` over a dense,
    deterministically-generated candidate ladder seeded with the
    versions the specifier names (see `_floor_probe_versions`).
    Prereleases are excluded — a default `pip install` won't resolve to
    one unless the pin explicitly targets it, and the realistic agent
    pins under test never do."""
    spec = requirement.specifier
    try:
        if not list(spec):
            return False
    except InvalidSpecifier:
        return False
    admitted = [v for v in _floor_probe_versions(spec, floor) if spec.contains(v)]
    if not admitted:
        return False
    return not any(v < floor for v in admitted)


def _target_python(oracle_dir: str | None) -> str:
    """Best-effort target Python for marker evaluation. Reads
    `target.python` (falls back to `source.python`) from the
    oracle's task.yaml. Defaults to a recent Python so an agent
    can't hide behind a marker that would disable on the latest
    interpreters."""
    if oracle_dir is None:
        return _DEFAULT_PYTHON
    task_yaml = Path(oracle_dir) / "task.yaml"
    if not task_yaml.is_file():
        return _DEFAULT_PYTHON
    try:
        data = yaml.safe_load(task_yaml.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError:
        return _DEFAULT_PYTHON
    target = (data.get("target") or {}).get("python")
    source = (data.get("source") or {}).get("python")
    chosen = target or source
    if isinstance(chosen, str):
        return chosen
    return _DEFAULT_PYTHON


@scoring_primitive("dag_factory_providers_declared")
def dag_factory_providers_declared(
    task_dir: str, agent_output: str, *, oracle_dir: str | None = None
) -> PrimitiveOutcome:
    """For every provider whose operators appear in the project's
    YAML, `requirements.txt` declares the matching provider package
    (or a `dag-factory[<extra>]` extras pin). PR #486 stopped
    pulling http / cncf-kubernetes in transitively.

    Oracle-aware: when the source fixture's YAML references a
    provider operator, the candidate must declare it — even if the
    agent deleted that operator from their working tree. A blanket
    `passed=None` would let the agent remove the one DAG that
    references the http provider and skip the metric entirely. With
    this gate, the agent only escapes the rule when the original
    fixture had no provider-bound operator at all.

    `passed=None` only when neither candidate nor oracle ships any
    YAML referencing a provider operator — the metric truly does
    not apply."""
    providers = dag_factory_idioms()["providers"]
    files = _project_yaml_files(task_dir)
    candidate_text = "\n".join(filter(None, (read_text_capped(f) for f in files)))
    candidate_needs = _providers_referenced(candidate_text, providers)
    oracle_needs: set[str] = set()
    for path in _oracle_yaml_files(oracle_dir):
        text = read_text_capped(path)
        if text is None:
            continue
        oracle_needs.update(_providers_referenced(text, providers))

    needed_ids = candidate_needs | oracle_needs
    if not needed_ids:
        return PrimitiveOutcome(value=0.0, passed=None)
    if not files:
        # Oracle expected provider operators; agent shipped no YAML.
        return PrimitiveOutcome(value=0.0, passed=False)

    req_path = Path(task_dir) / _REQUIREMENTS_FILE
    req_text = read_text_capped(req_path) if req_path.is_file() else ""
    req_text = req_text or ""
    target_python = _target_python(oracle_dir)
    pinned = _active_packages(req_text, target_python)
    extras = _active_dag_factory_extras(req_text, target_python)

    relevant = len(needed_ids)
    clean = 0
    for provider_id in sorted(needed_ids):
        spec = providers[provider_id]
        if any(_normalize(name) in pinned for name in spec["requirement_names"]):
            clean += 1
            continue
        if any(alias in extras for alias in spec["extras_aliases"]):
            clean += 1
    value = clean / relevant
    return PrimitiveOutcome(value=value, passed=value == 1.0)


def _providers_referenced(yaml_text: str, providers: dict) -> set[str]:
    """Provider IDs whose operator substrings appear in `yaml_text`."""
    if not yaml_text:
        return set()
    out: set[str] = set()
    for provider_id, spec in providers.items():
        if any(sub in yaml_text for sub in spec["operator_substrings"]):
            out.add(provider_id)
    return out


def _normalize(name: str) -> str:
    return name.lower().replace("_", "-")


def _active_packages(requirements_text: str, target_python: str) -> set[str]:
    """The set of canonicalised package names whose requirement
    is active on the target Python. Marker-disabled lines, broken
    lines, and `-r`/`-c` includes are skipped — same as `pip
    install -r` resolves."""
    out: set[str] = set()
    for raw in requirements_text.splitlines():
        line = raw.split("#", 1)[0].strip()
        if not line or line.startswith("-"):
            continue
        try:
            requirement = Requirement(line)
        except InvalidRequirement:
            continue
        if not _requirement_active(requirement, target_python):
            continue
        out.add(_normalize(requirement.name))
    return out


def _active_dag_factory_extras(requirements_text: str, target_python: str) -> set[str]:
    """Extras declared on the project's active `dag-factory[...]`
    line, lowercased. Skips marker-disabled lines."""
    requirement = _find_requirement(requirements_text, _is_dag_factory_name)
    if requirement is None:
        return set()
    if not _requirement_active(requirement, target_python):
        return set()
    return {extra.lower() for extra in requirement.extras}


@scoring_primitive("kpo_uses_type_annotations")
def kpo_uses_type_annotations(
    task_dir: str, agent_output: str, *, oracle_dir: str | None = None
) -> PrimitiveOutcome:
    """Every nested object under a KubernetesPodOperator task in
    YAML carries a `__type__: kubernetes.client.models.V1...`
    annotation whose class is a real `kubernetes.client.models.V1*`
    pod-spec class. PR #523 dropped the legacy auto-cast —
    un-annotated nested dicts no longer round-trip through KPO.

    Type validation is against the curated `kpo_v1_classes` set, not
    just the `kubernetes.client.models.V1` prefix: a prefix-only
    check accepted a fabricated `__type__: ...V1Garbage` that would
    fail to construct at runtime. The bare class name after the
    prefix must be a known pod-spec class.

    Oracle-aware identity parity: the candidate must ship every KPO
    task the source fixture had, keyed on `(dag_id, task_id)` (with a
    pod-config fingerprint fallback when those can't be resolved
    structurally). A count-only parity left the delete-one-pad-another
    dodge open — drop a real KPO DAG, author a decoy KPO block
    elsewhere, and the count is restored while a real pod silently
    disappears. Identity parity catches the missing `(dag_id,
    task_id)` regardless of decoy padding.

    `passed=None` only when neither candidate nor oracle ships a
    KPO operator — the metric truly does not apply."""
    yaml_spec = dag_factory_idioms()["yaml"]
    typed_block_keys = frozenset(yaml_spec["kpo_typed_blocks"])
    type_prefix = yaml_spec["kpo_type_prefix"]
    valid_classes = frozenset(yaml_spec["kpo_v1_classes"])
    kpo_substrings = ("KubernetesPodOperator",)

    files = _project_yaml_files(task_dir)
    oracle_identities = _kpo_task_identities(_oracle_yaml_files(oracle_dir), kpo_substrings)
    if not files:
        if oracle_identities:
            return PrimitiveOutcome(value=0.0, passed=False)
        return PrimitiveOutcome(value=0.0, passed=None)
    candidate_identities = _kpo_task_identities(files, kpo_substrings)
    if not oracle_identities.issubset(candidate_identities):
        # An oracle KPO task is missing from the candidate (deleted /
        # renamed); decoy padding can't restore the missing identity.
        return PrimitiveOutcome(value=0.0, passed=False)
    relevant = 0
    clean = 0
    for path in files:
        parsed = _safe_load_yaml(path)
        if parsed is None:
            continue
        for task_block in _kpo_task_blocks(parsed, kpo_substrings):
            for key in typed_block_keys:
                if key not in task_block:
                    continue
                relevant += 1
                if _all_dicts_typed(task_block[key], type_prefix, valid_classes):
                    clean += 1
    if relevant == 0:
        if oracle_identities:
            return PrimitiveOutcome(value=0.0, passed=False)
        return PrimitiveOutcome(value=0.0, passed=None)
    value = clean / relevant
    return PrimitiveOutcome(value=value, passed=value == 1.0)


def _kpo_task_blocks(parsed: Any, substrings: tuple[str, ...]) -> Iterator[dict]:
    """Every dict that looks like a task config and whose
    `operator:` value matches one of the substrings."""
    for d in _walk_dicts(parsed):
        operator = d.get("operator")
        if not isinstance(operator, str):
            continue
        if any(sub in operator for sub in substrings):
            yield d


def _kpo_task_identities(files: list[Path], substrings: tuple[str, ...]) -> set[str]:
    """Stable identities for every KPO task across `files`.

    Each identity is `<dag_id>::<task_id>` when both can be resolved
    structurally (dag-factory's `<dag_id>: {tasks: {<task_id>: {...}}}`
    shape). When a file doesn't `safe_load` (legacy custom tags in a
    source fixture), fall back to a per-`operator:`-block fingerprint
    (file name + the block's `name:`/`image:` lines) so each KPO task
    still contributes a distinct identity. Identities are the parity
    anchor: the candidate must reproduce every oracle identity, so the
    delete-one-pad-another-decoy dodge can't restore the set."""
    identities: set[str] = set()
    for path in files:
        parsed = _safe_load_yaml(path)
        if parsed is not None:
            identities.update(_kpo_identities_from_parsed(parsed, substrings))
            continue
        identities.update(_kpo_identities_from_text(path, substrings))
    return identities


def _kpo_identities_from_parsed(parsed: Any, substrings: tuple[str, ...]) -> set[str]:
    """`<dag_id>::<task_id>` for every KPO task in a parsed DAG tree."""
    out: set[str] = set()
    if not isinstance(parsed, dict):
        return out
    for dag_id, dag_block in parsed.items():
        if not isinstance(dag_block, dict) or dag_id in {"default", "defaults"}:
            continue
        tasks = dag_block.get("tasks")
        if not isinstance(tasks, dict):
            continue
        for task_id, task_block in tasks.items():
            if not isinstance(task_block, dict):
                continue
            operator = task_block.get("operator")
            if isinstance(operator, str) and any(sub in operator for sub in substrings):
                out.add(f"{dag_id}::{task_id}")
    return out


def _kpo_identities_from_text(path: Path, substrings: tuple[str, ...]) -> set[str]:
    """Fingerprint identities for an unparseable (legacy-tagged) YAML.
    One identity per `operator:` line whose value is a KPO, keyed on
    the file name plus the nearby `name:` / `image:` lines so two KPO
    tasks in the same file don't collapse to one identity."""
    text = read_text_capped(path) or ""
    lines = text.splitlines()
    out: set[str] = set()
    seq = 0
    for idx, raw in enumerate(lines):
        stripped = raw.lstrip()
        if not stripped.startswith("operator:"):
            continue
        if not any(sub in stripped for sub in substrings):
            continue
        # Gather a small window of following lines for a stable fingerprint.
        window = []
        for follow in lines[idx + 1 : idx + 12]:
            fs = follow.lstrip()
            if fs.startswith(("name:", "image:", "namespace:")):
                window.append(fs)
        fingerprint = "|".join(window) if window else f"seq{seq}"
        seq += 1
        out.add(f"{path.name}::{fingerprint}")
    return out


def _all_dicts_typed(node: Any, prefix: str, valid_classes: frozenset[str]) -> bool:
    """Every dict in `node` carries a `__type__: <prefix><ClassName>`
    whose `<ClassName>` is in `valid_classes`. Lists and primitives
    short-circuit to True (the rule applies to dict nodes only).

    Validating the class against the curated set — not just the
    `kubernetes.client.models.V1` prefix — rejects a fabricated
    `V1Garbage` that satisfies the prefix but would `AttributeError`
    on the real model module at runtime."""
    if isinstance(node, list):
        return all(_all_dicts_typed(item, prefix, valid_classes) for item in node)
    if not isinstance(node, dict):
        return True
    type_marker = node.get("__type__")
    if not isinstance(type_marker, str):
        return False
    if not type_marker.startswith(prefix):
        return False
    # `prefix` is `kubernetes.client.models.V1`; the real class name is
    # the trailing dotted segment, e.g. `V1ResourceRequirements`.
    class_name = type_marker.rsplit(".", 1)[-1]
    return class_name in valid_classes


@scoring_primitive("timetable_uses_type_annotation")
def timetable_uses_type_annotation(
    task_dir: str, agent_output: str, *, oracle_dir: str | None = None
) -> PrimitiveOutcome:
    """Any DAG declaring a custom `timetable:` uses the new
    `__type__/__args__` shape. PR #533 removed the legacy
    `{callable, params}` parsing.

    Oracle-aware count parity: the candidate must ship at least as
    many `timetable:` blocks as the source fixture. A blanket
    "any typed timetable passes" check accepted dropping a
    timetable in favour of a plain `schedule:`, silently changing
    the DAG's cadence — the prompt forbids that, but the metric
    has to back it up. The metric does not validate the migrated
    timetable's class or arguments; that's a separate guard worth
    layering on if the value-drift attack matters.

    `passed=None` only when neither candidate nor oracle declares
    a `timetable:`."""
    spec = dag_factory_idioms()["yaml"]
    required = frozenset(spec["timetable_required_keys"])
    forbidden = frozenset(spec["timetable_forbidden_keys"])
    files = _project_yaml_files(task_dir)
    oracle_timetables = _count_timetables(_oracle_yaml_files(oracle_dir))
    if not files:
        if oracle_timetables > 0:
            return PrimitiveOutcome(value=0.0, passed=False)
        return PrimitiveOutcome(value=0.0, passed=None)
    candidate_timetables = _count_timetables(files)
    if candidate_timetables < oracle_timetables:
        return PrimitiveOutcome(value=0.0, passed=False)
    relevant = 0
    clean = 0
    for path in files:
        parsed = _safe_load_yaml(path)
        if parsed is None:
            continue
        for block in _dag_blocks(parsed):
            tt = block.get("timetable")
            if not isinstance(tt, dict):
                continue
            relevant += 1
            keys = set(tt.keys())
            if required.issubset(keys) and not (forbidden & keys):
                clean += 1
    if relevant == 0:
        if oracle_timetables > 0:
            return PrimitiveOutcome(value=0.0, passed=False)
        return PrimitiveOutcome(value=0.0, passed=None)
    value = clean / relevant
    return PrimitiveOutcome(value=value, passed=value == 1.0)


def _count_timetables(files: list[Path]) -> int:
    """Total `timetable:` block count across `files`. Falls back to
    a key-line text scan when the YAML doesn't safe_load (legacy
    tags survive in source fixtures)."""
    total = 0
    for path in files:
        parsed = _safe_load_yaml(path)
        if parsed is not None:
            for block in _dag_blocks(parsed):
                if isinstance(block.get("timetable"), dict):
                    total += 1
            continue
        text = read_text_capped(path) or ""
        for line in text.splitlines():
            stripped = line.lstrip()
            if stripped.startswith("timetable:"):
                total += 1
    return total


@scoring_primitive("sla_miss_callback_handled")
def sla_miss_callback_handled(
    task_dir: str, agent_output: str, *, oracle_dir: str | None = None
) -> PrimitiveOutcome:
    """No YAML file declares `sla_miss_callback:` at any depth.
    AF >= 3.1 + dag-factory >= 1.0.1 silently strip the field
    (PR #586/#588) — DAGs continue to load but the callback never
    fires. The migration is to drop the key or replace with an
    AF3-native mechanism (Asset / outlet-driven callbacks).

    `passed=None` when the project has no YAML under `dags/`."""
    forbidden = frozenset(dag_factory_idioms()["yaml"]["silently_stripped_dag_kwargs"])
    files = _project_yaml_files(task_dir)
    if not files:
        return PrimitiveOutcome(value=0.0, passed=None)
    for path in files:
        parsed = _safe_load_yaml(path)
        if parsed is None:
            text = read_text_capped(path) or ""
            if any(re.search(rf"(?m)^\s*{re.escape(k)}\s*:", text) for k in forbidden):
                return PrimitiveOutcome(value=0.0, passed=False)
            continue
        if forbidden & set(_collect_keys(parsed)):
            return PrimitiveOutcome(value=0.0, passed=False)
    return PrimitiveOutcome(value=1.0, passed=True)
