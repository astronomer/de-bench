"""Scoring primitives for the Version Awareness category.

VA tasks expect the agent to emit a structured `output.json` at the
task root. Primitives load that file and compare specific keys against
the task's `expected.json`, which lives in the **oracle** tree (NOT in
the agent's working directory — the answer key is kept outside so the
agent can't just copy it). A missing or unparseable `output.json` is a
metric-level fail; primitives don't raise.
"""

from __future__ import annotations

import ast
import json
import re
from pathlib import Path

import yaml

from de_bench.upgrade_v2_scoring.data import (
    airflow_version_idioms,
    provider_compat_matrix,
    provider_idioms,
    stepping_stone_floor,
)
from de_bench.upgrade_v2_scoring.primitives._common import read_text_capped
from de_bench.upgrade_v2_scoring.registry import PrimitiveOutcome, scoring_primitive

_OUTPUT_FILE = "output.json"
_EXPECTED_FILE = "expected.json"
_REQUIREMENTS_FILE = "requirements.txt"
_DOCKERFILE = "Dockerfile"
_TASK_YAML_FILE = "task.yaml"

# Astro Runtime tag -> Airflow major.minor lookup for the AF2-era
# numbering (sequential like "11.3.0"). AF3 tags follow a different
# scheme — `<airflow_mm>-<patch>` (e.g. "3.0-14") — and are parsed
# without a lookup. Source: otto-upgrade-kb v1/runtime/runtime.json,
# verified against astronomer/astro-runtime CHANGELOG.md.
#
# Two-tier resolution (see _runtime_tag_to_airflow_mm):
#
#   1. Specific entries below — the ones airflow-bench tasks
#      actively pin in their fixtures + oracle solutions. Wins on
#      exact match.
#   2. Family-major fallback — the AF2 runtime numbering line is
#      stable: every patch under runtime 11.x targets airflow 2.9,
#      every 12.x targets 2.10, every 13.x targets 2.11. The 2026-04-26
#      smoke caught Otto picking `astro-runtime:13.4.0` (a valid 2.11
#      patch the upgrade-kb knows about) on proj-d8e7f6; without the
#      family fallback the metric failed closed. Family wins are
#      reasonable because:
#        - Astronomer publishes a public release-notes line per
#          family (https://www.astronomer.io/docs/runtime/runtime-release-notes).
#        - Off-by-major is a much louder error than off-by-patch:
#          if astro switched 13.x to airflow 3.x the eventual AF3
#          numbering scheme `<mm>-<patch>` kicks in and bypasses
#          this map entirely.
#      Add a specific entry only when a particular tag needs to
#      override the family (or when the family doesn't apply).
_AF2_RUNTIME_TO_AIRFLOW_MM: dict[str, str] = {
    "11.3.0": "2.9",
    "12.5.0": "2.10",
    "12.7.1": "2.10",
    "13.0.0": "2.11",
    "13.6.0": "2.11",
}

# Family-major fallback used when the explicit table doesn't match.
# Keyed on the runtime's MAJOR component only (e.g. "13" for
# "13.4.0"). Maintained in lockstep with public Astro Runtime
# release notes — when astronomer publishes 14.x for AF 3.x they
# stop using this scheme and switch to AF3 tags, so this map
# wouldn't grow into 14+.
_AF2_RUNTIME_FAMILY_TO_AIRFLOW_MM: dict[str, str] = {
    "11": "2.9",
    "12": "2.10",
    "13": "2.11",
}


@scoring_primitive("detected_airflow_version_match")
def detected_airflow_version_match(
    task_dir: str, agent_output: str, *, oracle_dir: str | None = None
) -> PrimitiveOutcome:
    return _match_scalar_field(task_dir, oracle_dir, "airflow")


@scoring_primitive("detected_python_version_match")
def detected_python_version_match(
    task_dir: str, agent_output: str, *, oracle_dir: str | None = None
) -> PrimitiveOutcome:
    return _match_scalar_field(task_dir, oracle_dir, "python")


_CONFLICT_STOPWORDS: frozenset[str] = frozenset(
    {"vs", "and", "or", "the", "a", "an", "with", "to", "of", "in", "between"}
)


def _normalise_conflict(s: str) -> set[str]:
    """Tokenise a conflict-name string into a lowercased token set,
    stripping punctuation and short stopwords. `"protobuf-diamond"`
    → `{"protobuf", "diamond"}`; `"google vs common-ai"` →
    `{"google", "common", "ai"}`."""
    if not isinstance(s, str):
        return set()
    tokens = re.findall(r"[a-z0-9]+", s.lower())
    return {t for t in tokens if t not in _CONFLICT_STOPWORDS and len(t) >= 2}


@scoring_primitive("compat_conflict_detected")
def compat_conflict_detected(
    task_dir: str, agent_output: str, *, oracle_dir: str | None = None
) -> PrimitiveOutcome:
    """Agent's `output.json` matches the oracle on both the
    `compatible` boolean AND, when incompatible, names a conflict
    that overlaps the oracle's `conflicts` list under
    token-set normalisation.

    Why two-part: the boolean alone saturates at 50% by coin flip
    — half of all `{compatible: true, false}` guesses match the
    oracle by chance. A `compatible: false` answer with a
    contentless conflict list (e.g. `["unknown"]`) leaks no
    diagnosis signal but would have passed under a boolean-only
    check. The `conflicts` half raises the bar: when the oracle
    expects an incompatible verdict, every expected conflict must
    have at least one agent-side conflict whose tokens overlap by
    ≥ ⌈|expected|/2⌉ — `"protobuf-diamond"` (2 tokens) requires the
    agent's conflict to share at least 1 token (`protobuf` or
    `diamond`). Agents using different shorthand (`"protobuf
    version conflict"`, `"google-common-ai diamond"`) still pass;
    agents with empty / contentless / unrelated conflict names
    fail.

    `compatible: true` cases don't grade the conflicts list (the
    field is expected to be empty); the boolean match is the only
    requirement.

    `passed=None` when the oracle is missing or the expected
    `compatible` value isn't a bool."""
    agent = _load_output(task_dir)
    expected = _load_expected(oracle_dir)
    if agent is None or expected is None:
        return PrimitiveOutcome(value=0.0, passed=False)
    agent_compat = agent.get("compatible")
    want_compat = expected.get("compatible")
    if not isinstance(want_compat, bool):
        return PrimitiveOutcome(value=0.0, passed=None)
    if agent_compat != want_compat:
        return PrimitiveOutcome(value=0.0, passed=False)
    if want_compat is True:
        return PrimitiveOutcome(value=1.0, passed=True)
    expected_conflicts = expected.get("conflicts") or []
    agent_conflicts = agent.get("conflicts") or []
    if not isinstance(agent_conflicts, list):
        return PrimitiveOutcome(value=0.0, passed=False)
    agent_token_sets = [_normalise_conflict(c) for c in agent_conflicts]
    for exp in expected_conflicts:
        exp_tokens = _normalise_conflict(exp)
        if not exp_tokens:
            continue
        threshold = max(1, len(exp_tokens) // 2 + len(exp_tokens) % 2)
        if not any(len(exp_tokens & ats) >= threshold for ats in agent_token_sets):
            return PrimitiveOutcome(value=0.0, passed=False)
    return PrimitiveOutcome(value=1.0, passed=True)


_AIRFLOW_PIN_PATTERN = re.compile(r"^\s*apache-airflow\s*==\s*([\w.+-]+)", re.IGNORECASE)


def _airflow_pin_from_requirements(task_dir: Path) -> str | None:
    """Read the agent's `requirements.txt` and return the pinned
    `apache-airflow` version, or None when the file is missing or
    no `apache-airflow==X` line is present. The first uncommented
    matching line wins."""
    req = task_dir / _REQUIREMENTS_FILE
    if not req.is_file():
        return None
    text = read_text_capped(req)
    if text is None:
        return None
    for raw in text.splitlines():
        line = raw.split("#", 1)[0]
        match = _AIRFLOW_PIN_PATTERN.match(line)
        if match:
            return match.group(1)
    return None


@scoring_primitive("stepping_stone_path_match")
def stepping_stone_path_match(
    task_dir: str, agent_output: str, *, oracle_dir: str | None = None
) -> PrimitiveOutcome:
    """The agent's `path` describes a valid upgrade route from the
    expected source pin to the expected target, AND for bridged
    routes the agent has actually pinned the project to an
    intermediate version (executable first hop).

    Two shapes are accepted, gated by what the oracle's expected
    path looks like:

    - **Bridged route** (oracle's expected path has length >= 3):
      agent's path must be source + at least one recognised LTS
      bridge from `af3_migrations.yaml::stepping_stone_floor` +
      target. Multiple defensible bridges exist (2.10 vs 2.11),
      and extra hops are OK as long as an LTS bridge sits
      somewhere in the middle. **In addition**, the agent's
      `requirements.txt` must pin `apache-airflow` to one of the
      expected intermediate versions — diagnosis-only output is
      gameable (the route 2.x → 2.10 → 3.0 is widely memorised),
      so the metric requires the agent to also *take* the first
      hop. Pinning back to source or jumping straight to target
      both fail.

    - **Direct route** (oracle's expected path has length == 2):
      agent's path must also be exactly source + target, length 2.
      A spurious bridge here would penalise the agent for
      *adding* an unnecessary hop within a major-line jump
      (2.8 → 2.11, 3.1 → 3.2) where the team can land directly.
      No requirements-pin check (no intermediate to advance to).

    The previous strict list-equality rule rejected correct
    answers; this version grades the right shape against the
    expected one."""
    agent = _load_output(task_dir)
    expected = _load_expected(oracle_dir)
    if agent is None or expected is None:
        return PrimitiveOutcome(value=0.0, passed=False)
    got = agent.get("path")
    want = expected.get("path")
    if not isinstance(want, list) or len(want) < 2:
        return PrimitiveOutcome(value=0.0, passed=None)
    if not isinstance(got, list):
        return PrimitiveOutcome(value=0.0, passed=False)
    if len(want) == 2:
        # Direct hop: agent must match endpoints exactly with no
        # interior steps. A length-3+ path adds an unwarranted
        # bridge.
        if len(got) != 2:
            return PrimitiveOutcome(value=0.0, passed=False)
        ok = _endpoint_matches(got[0], want[0]) and _endpoint_matches(got[-1], want[-1])
        return PrimitiveOutcome(value=1.0 if ok else 0.0, passed=ok)
    # Bridged route: source + at least one bridge + target.
    if len(got) < 3:
        return PrimitiveOutcome(value=0.0, passed=False)
    if not _endpoint_matches(got[0], want[0]) or not _endpoint_matches(got[-1], want[-1]):
        return PrimitiveOutcome(value=0.0, passed=False)
    bridges = stepping_stone_floor()
    middle = got[1:-1]
    has_bridge = any(
        isinstance(step, str) and any(step == b or step.startswith(f"{b}.") for b in bridges)
        for step in middle
    )
    if not has_bridge:
        return PrimitiveOutcome(value=0.0, passed=False)
    # Bridged route — verify the agent actually pinned to one of
    # the intermediates *they themselves declared*. Memorising the
    # route alone is the dodge this guard fixes; coherent agents
    # advance the pin to a bridge they recommended. We grade against
    # the agent's own declared intermediates (not the oracle's) so
    # an agent who picked an alternative valid LTS bridge (2.11
    # instead of the oracle's 2.10) still passes when they pin to
    # what they recommended.
    agent_intermediates = [v for v in middle if isinstance(v, str)]
    if not agent_intermediates:
        return PrimitiveOutcome(value=1.0, passed=True)
    actual_pin = _airflow_pin_from_requirements(Path(task_dir))
    if actual_pin is None:
        return PrimitiveOutcome(value=0.0, passed=False)
    pinned_to_intermediate = any(
        _endpoint_matches(actual_pin, declared) for declared in agent_intermediates
    )
    return PrimitiveOutcome(
        value=1.0 if pinned_to_intermediate else 0.0,
        passed=pinned_to_intermediate,
    )


def _endpoint_matches(got: object, want: object) -> bool:
    """Endpoint comparison that treats `"3.0.x"` and `"3.0.0"` as the
    same target: both satisfy the `3.0` major.minor. Exact equality
    wins (source pins like `"2.6.3"` stay strict). The `.x` convention
    is bench-internal shorthand; agents reasonably write a real patch
    or just the major.minor — accept any of them.

    Major-only wildcard `"<major>.x"` (e.g. `"3.x"`) accepts any minor
    under that major. Used when the prompt only commits to a major
    line ("land on Airflow 3") and the bench should not punish agents
    that target a more current 3.minor than the gold was written
    against — at task-author time 3.0 was current; once 3.2 ships,
    `[2.5.3, 2.10.x, 3.2.0]` is just as valid."""
    if not isinstance(got, str) or not isinstance(want, str):
        return False
    if got == want:
        return True
    want_match = re.fullmatch(r"(\d+)\.x", want)
    if want_match:
        got_major = got.split(".", 1)[0]
        return got_major == want_match.group(1)
    return _major_minor(got) == _major_minor(want)


def _major_minor(version: str) -> str | None:
    parts = version.split(".")
    if len(parts) < 2:
        return None
    return f"{parts[0]}.{parts[1]}"


@scoring_primitive("airflow_pin_target_aligned")
def airflow_pin_target_aligned(
    task_dir: str, agent_output: str, *, oracle_dir: str | None = None
) -> PrimitiveOutcome:
    """`requirements.txt` pins `apache-airflow` to the same major.minor
    as the task's `target.airflow` (read from the oracle's task.yaml),
    OR omits the pin entirely (Runtime-supplied semantics).

    Catches the LTS-bridge failure mode for the OSS airflow flavor:
    target says 2.11, agent pins 3.x because it conflated the bridge
    with a major upgrade.

    No `apache-airflow` constraint line in requirements.txt is a pass:
    the bench harness runs every task inside an Astro Runtime
    container, and Runtime supplies the apache-airflow distribution
    itself — pinning it in requirements.txt is anti-idiomatic in that
    context. The deletion-attack guard (`req.is_file()`) above stops
    an agent from dodging the metric by removing the file outright.

    Astro flavor pins via `FROM astro-runtime:<tag>` and never lists
    `apache-airflow` in requirements.txt — those tasks should use
    `astro_runtime_target_aligned` instead.

    `passed=None` when target.airflow isn't set in task.yaml — the
    metric only applies to tasks that declare a target. Missing or
    unparseable requirements.txt is a hard fail; an apache-airflow
    line whose pin parses to a different major.minor is a hard fail.
    Multiple apache-airflow lines: every pin must match (the file is
    inconsistent otherwise)."""
    target_mm = _target_major_minor(oracle_dir)
    if target_mm is None:
        return PrimitiveOutcome(value=0.0, passed=None)
    req = Path(task_dir) / _REQUIREMENTS_FILE
    if not req.is_file():
        return PrimitiveOutcome(value=0.0, passed=False)
    text = read_text_capped(req)
    if text is None:
        return PrimitiveOutcome(value=0.0, passed=False)
    pins = _airflow_pins(text)
    if not pins:
        return PrimitiveOutcome(value=1.0, passed=True)
    aligned = all(_major_minor(p) == target_mm for p in pins)
    return PrimitiveOutcome(value=1.0 if aligned else 0.0, passed=aligned)


@scoring_primitive("astro_runtime_target_aligned")
def astro_runtime_target_aligned(
    task_dir: str, agent_output: str, *, oracle_dir: str | None = None
) -> PrimitiveOutcome:
    """`Dockerfile`'s FROM line points at an Astro Runtime image whose
    Airflow major.minor matches `target.airflow` from task.yaml. The
    LTS-bridge failure mode for astro flavor: target=2.11 but the
    agent bumped to a 3.x runtime tag.

    Tag-to-airflow rules:
      - AF3 tags follow `<af_major.minor>-<patch>` (e.g. `3.0-14`)
        — major.minor is parsed directly from the tag.
      - AF2 tags use sequential numbering (`11.3.0`, `13.0.0`) and
        require `_AF2_RUNTIME_TO_AIRFLOW_MM` lookup.

    Image-name shape is also graded for AF3 targets. Astronomer
    publishes AF3 under the new image name `runtime` (typically on
    `astrocrpublic.azurecr.io/runtime`); the legacy `astro-runtime`
    image only ships AF2 builds. An agent that bumps the tag to a
    3.x value on the `astro-runtime` image is signaling unawareness
    of the AF3-era image rename. So for AF3 targets we additionally
    require the FROM line's image segment to end in `/runtime` (any
    registry) — not `/astro-runtime`. AF2 targets accept either
    image-name shape since AF2 is published on both.

    `passed=None` when target.airflow isn't set. Missing or
    unparseable Dockerfile, no FROM line for an Astro Runtime image,
    or a tag that doesn't resolve are hard fails."""
    target_mm = _target_major_minor(oracle_dir)
    if target_mm is None:
        return PrimitiveOutcome(value=0.0, passed=None)
    dockerfile = Path(task_dir) / _DOCKERFILE
    if not dockerfile.is_file():
        return PrimitiveOutcome(value=0.0, passed=False)
    text = read_text_capped(dockerfile)
    if text is None:
        return PrimitiveOutcome(value=0.0, passed=False)
    parsed = _astro_runtime_from_line(text)
    if parsed is None:
        return PrimitiveOutcome(value=0.0, passed=False)
    image_kind, tag = parsed
    pin_mm = _runtime_tag_to_airflow_mm(tag)
    if pin_mm is None:
        return PrimitiveOutcome(value=0.0, passed=False)
    if pin_mm != target_mm:
        return PrimitiveOutcome(value=0.0, passed=False)
    target_major = target_mm.split(".", 1)[0]
    if target_major == "3" and image_kind != "af3":
        return PrimitiveOutcome(value=0.0, passed=False)
    return PrimitiveOutcome(value=1.0, passed=True)


_FROM_PATTERN = re.compile(r"^\s*FROM\s+(\S+):([\w.+-]+)", re.IGNORECASE)


def _astro_runtime_from_line(dockerfile_text: str) -> tuple[str, str] | None:
    """First uncommented FROM line that references an Astro Runtime
    image, returned as `(image_kind, tag)`:
      - `"af2"` when the image-name segment ends in `astro-runtime`
        (e.g. `quay.io/astronomer/astro-runtime`).
      - `"af3"` when it ends in `runtime` but NOT `astro-runtime`
        (e.g. `astrocrpublic.azurecr.io/runtime`).
    Returns None when no matching FROM line exists."""
    for raw in dockerfile_text.splitlines():
        line = raw.split("#", 1)[0]
        match = _FROM_PATTERN.match(line)
        if not match:
            continue
        image, tag = match.group(1), match.group(2)
        if image == "astro-runtime" or image.endswith("/astro-runtime"):
            return ("af2", tag)
        if image == "runtime" or image.endswith("/runtime"):
            return ("af3", tag)
    return None


def _runtime_tag_to_airflow_mm(tag: str) -> str | None:
    af3 = re.match(r"^(\d+\.\d+)-\d+$", tag)
    if af3:
        return af3.group(1)
    if tag in _AF2_RUNTIME_TO_AIRFLOW_MM:
        return _AF2_RUNTIME_TO_AIRFLOW_MM[tag]
    af2_family = re.match(r"^(\d+)\.\d+\.\d+$", tag)
    if af2_family:
        return _AF2_RUNTIME_FAMILY_TO_AIRFLOW_MM.get(af2_family.group(1))
    return None


def _target_major_minor(oracle_dir: str | None) -> str | None:
    if oracle_dir is None:
        return None
    yaml_path = Path(oracle_dir) / _TASK_YAML_FILE
    if not yaml_path.is_file():
        return None
    try:
        payload = yaml.safe_load(yaml_path.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError:
        return None
    target = (payload.get("target") or {}).get("airflow")
    if not isinstance(target, str):
        return None
    return _major_minor(target)


def _airflow_pins(text: str) -> list[str]:
    """Pull the version literal from every uncommented
    `apache-airflow == X.Y(.Z)?` line. Lines without `==` are ignored
    — `>=` and `~=` constraints don't fix a single version, so the
    metric has nothing to align against."""
    pins: list[str] = []
    for raw in text.splitlines():
        line = raw.split("#", 1)[0].strip()
        if not line:
            continue
        match = re.match(r"^apache-airflow\s*==\s*([\w.+-]+)", line)
        if match:
            pins.append(match.group(1))
    return pins


def _airflow_pins_strict(text: str) -> list[str] | None:
    """Strict variant for `author_idiomatic_for_pin`: returns the
    list of `apache-airflow==X.Y[.Z]` pins ONLY when every
    apache-airflow constraint line is a clean exact pin with no
    PEP 508 markers, no extras, and no non-`==` operators.

    Returns `None` when any apache-airflow line has:
      - an environment marker (`apache-airflow==3.2.0; python_version<'0'`)
        — adversarial inactive pin that the marker disables but the
        loose regex would otherwise accept
      - extras (`apache-airflow[postgres]==2.9`) — bench tasks pin the
        bare package; extras add ambiguity
      - any non-`==` operator (`>=`, `<=`, `~=`, `!=`) — doesn't fix a
        single version, so the metric has nothing to grade against

    `[]` is returned (NOT None) when there are no apache-airflow
    constraint lines at all — the caller decides whether that's
    a fail or out-of-scope."""
    pins: list[str] = []
    for raw in text.splitlines():
        line = raw.split("#", 1)[0].strip()
        if not line:
            continue
        # Match any line whose top-level package name starts with
        # apache-airflow (with or without extras).
        if not re.match(r"^apache-airflow(?:\[|\s|=|<|>|!|~)", line, re.IGNORECASE):
            continue
        # Reject environment markers — even an inactive marker
        # carries information that a loose pin doesn't capture.
        if ";" in line:
            return None
        # Reject extras — `apache-airflow[postgres]==2.9` is a
        # different package shape than the bench grades against.
        if re.match(r"^apache-airflow\[", line, re.IGNORECASE):
            return None
        # Reject non-`==` constraints. `>=`, `~=`, `<=`, `!=` etc.
        # cannot identify a single major.minor.
        match = re.match(r"^apache-airflow\s*==\s*([\w.+-]+)\s*$", line, re.IGNORECASE)
        if not match:
            return None
        pins.append(match.group(1))
    return pins


@scoring_primitive("author_idiomatic_for_pin")
def author_idiomatic_for_pin(
    task_dir: str, agent_output: str, *, oracle_dir: str | None = None
) -> PrimitiveOutcome:
    """Newly-authored DAGs use the kwargs / context-vars / imports
    that are idiomatic for the Airflow major.minor pinned in
    `requirements.txt`. An agent that emits 3.x idioms against a 2.9
    pin is wrong even when the code is more modern; a 2.x agent
    emitting `schedule_interval=` against a 3.2 pin is wrong in the
    other direction.

    The idiom table lives in
    `airflow_bench.scoring.data.version_idioms.yaml` and is consulted
    via `airflow_version_idioms(major_minor)`. Adding a new pinned
    version is a YAML edit, not a Python edit. Each DAG file is
    graded against three checks:

      1. **DAG kwargs** — at least one of the version's
         `dag_kwargs_idiomatic` is present on every DAG-construction
         site.
      2. **Imports** — none of `imports_forbidden` are imported.
      3. **Context vars** — none of `context_var_forbidden` are
         referenced.

    Score is the fraction of files that pass all three checks.
    `passed=None` when `requirements.txt` doesn't pin
    `apache-airflow==X.Y` or when the pinned version isn't in the
    idiom table — the metric needs both inputs to grade."""
    # Fail closed when requirements.txt is absent or the
    # apache-airflow pin is missing — an agent that removes the pin
    # to make the metric return passed=None would otherwise dodge
    # the 0.65-weight authoring check by attrition.
    req = Path(task_dir) / _REQUIREMENTS_FILE
    if not req.is_file():
        return PrimitiveOutcome(value=0.0, passed=False)
    text = read_text_capped(req)
    if text is None:
        return PrimitiveOutcome(value=0.0, passed=False)
    raw_pins = _airflow_pins_strict(text)
    if raw_pins is None:
        # Markers, extras, or non-`==` constraints — bench can't
        # determine the effective version. An adversarial agent that
        # adds an inactive `apache-airflow==3.2.0; python_version<'0'`
        # next to a real `>=2.9,<3` would otherwise grade as 3.2.
        return PrimitiveOutcome(value=0.0, passed=False)
    pins = [_major_minor(p) for p in raw_pins]
    pins = [p for p in pins if p is not None]
    if not pins:
        return PrimitiveOutcome(value=0.0, passed=False)
    # All pins must agree — a mixed `apache-airflow==2.9` plus
    # `apache-airflow==3.0` file is incoherent regardless of which
    # one the agent meant.
    if len(set(pins)) > 1:
        return PrimitiveOutcome(value=0.0, passed=False)
    pin_mm = pins[0]
    idioms = airflow_version_idioms(pin_mm)
    if idioms is None:
        # Legitimately out of scope: pin parses but the bench has no
        # idiom table for this version. Caller can extend the YAML.
        return PrimitiveOutcome(value=0.0, passed=None)
    dags = Path(task_dir) / "dags"
    files = sorted(dags.rglob("*.py"))
    if not files:
        return PrimitiveOutcome(value=0.0, passed=False)
    clean = sum(1 for path in files if _file_idiomatic_for(path, idioms))
    value = clean / len(files)
    return PrimitiveOutcome(value=value, passed=value == 1.0)


def _file_idiomatic_for(path: Path, idioms: dict) -> bool:
    """True when the file passes all three idiom checks for the
    pinned version. Files with syntax errors fail (callers running
    parse_ok in the same suite catch those separately)."""
    text = read_text_capped(path)
    if text is None:
        return False
    try:
        tree = ast.parse(text)
    except (SyntaxError, ValueError):
        return False
    if not _dag_kwargs_idiomatic(tree, idioms):
        return False
    if not _imports_clean(tree, idioms):
        return False
    return _context_vars_clean(text, idioms)


def _dag_kwargs_idiomatic(tree: ast.AST, idioms: dict) -> bool:
    """Every DAG construction in the tree uses one of the version's
    idiomatic schedule kwargs. Files that don't construct a DAG pass
    this check trivially (the metric is "*emitted* DAGs use the
    right kwargs")."""
    accepted = set(idioms.get("dag_kwargs_idiomatic", []))
    if not accepted:
        return True
    return all(accepted & kwargs for kwargs in _collect_dag_kwargs(tree))


def _imports_clean(tree: ast.AST, idioms: dict) -> bool:
    forbidden = idioms.get("imports_forbidden") or []
    if not forbidden:
        return True
    for node in ast.walk(tree):
        modules: list[str] = []
        if isinstance(node, ast.ImportFrom) and node.module is not None:
            modules.append(node.module)
        elif isinstance(node, ast.Import):
            modules.extend(alias.name for alias in node.names)
        for module in modules:
            for forb in forbidden:
                if module == forb or module.startswith(f"{forb}."):
                    return False
    return True


def _context_vars_clean(text: str, idioms: dict) -> bool:
    forbidden = idioms.get("context_var_forbidden") or []
    if not forbidden:
        return True
    return not any(re.search(rf"\b{re.escape(var)}\b", text) for var in forbidden)


def _collect_dag_kwargs(tree: ast.AST) -> list[set[str]]:
    """Local copy of `_collect_dag_kwargs` shape from upgrades.py —
    duplicated rather than imported to keep the VA module's import
    graph independent. Recognises `DAG(...)` calls and `@dag(...)`
    decorators."""
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


@scoring_primitive("provider_incompatibility_flagged")
def provider_incompatibility_flagged(
    task_dir: str, agent_output: str, *, oracle_dir: str | None = None
) -> PrimitiveOutcome:
    """The agent's `output.json` flags every provider/symbol or
    provider/kwarg incompatibility listed in `expected.json`. Used
    on tasks that ask "does this DAG work with the pinned provider
    versions?" — the agent is expected to surface specific known
    removals (e.g. `SnowflakeOperator` removed in
    snowflake>=6.0, `is_delete_operator_pod` removed in
    cncf-kubernetes>=10).

    `expected.json` shape:

        {
          "removed_symbols": ["SnowflakeOperator"],
          "removed_kwargs": {"KubernetesPodOperator": ["is_delete_operator_pod"]}
        }

    The agent's `output.json` may use the same shape OR a flat
    `flags: ["SnowflakeOperator", "is_delete_operator_pod"]` list —
    both register a hit.

    Score = matched / (expected + spurious_agent), where spurious is
    the agent's flagged items NOT present in expected. This closes
    the canned-output attack across the corpus: an agent that emits
    every-known-removed-symbol on every fixture would otherwise score
    1.0 everywhere (matched=|expected|, denominator=|expected|), but
    each unrelated extra flag now subtracts. Mirrors the spurious-
    field penalty in `provider_compatibility_check` (#106 hardening
    pattern)."""
    expected = _load_expected(oracle_dir)
    agent = _load_output(task_dir)
    if expected is None:
        return PrimitiveOutcome(value=0.0, passed=None)
    if agent is None:
        return PrimitiveOutcome(value=0.0, passed=False)
    expected_items = _flatten_provider_expected(expected)
    if not expected_items:
        return PrimitiveOutcome(value=0.0, passed=None)
    agent_items = _flatten_agent_flags(agent)
    matched = expected_items & agent_items
    spurious = agent_items - expected_items
    denominator = len(expected_items) + len(spurious)
    value = len(matched) / denominator
    return PrimitiveOutcome(value=value, passed=value == 1.0)


def _canonical_class_name(name: str) -> str:
    """Bare class name from a possibly-fully-qualified path.
    `airflow.providers.cncf.kubernetes.operators.pod.KubernetesPodOperator`
    → `KubernetesPodOperator`. Python class names can't contain
    dots, so `rpartition('.')` is unambiguous on the operator
    boundary. Returns the input unchanged when it already has no
    dots."""
    return name.rpartition(".")[2] or name


def _flatten_provider_expected(expected: dict) -> set[tuple[str, ...]]:
    """Normalise expected items to structured tuples that include the
    operator identity for kwargs. Operator names are canonicalised
    to bare class names so a fully-qualified import path on either
    side still matches its bare-name counterpart."""
    items: set[tuple[str, ...]] = set()
    for sym in expected.get("removed_symbols") or []:
        items.add(("symbol", _canonical_class_name(str(sym))))
    rm_kwargs = expected.get("removed_kwargs") or {}
    if isinstance(rm_kwargs, dict):
        for op, kwargs in rm_kwargs.items():
            if not isinstance(kwargs, list):
                continue
            for kw in kwargs:
                items.add(("kwarg", _canonical_class_name(str(op)), str(kw)))
    return items


def _flatten_agent_flags(agent: dict) -> set[tuple[str, ...]]:
    """Normalise agent items to the same tuple shape as expected.

    Accepts the structured shape directly. The flat `flags` list is
    accepted for symbols (bare class name) and for `Operator.kwarg`
    dotted shorthand. The shorthand splits at the **last** dot
    because kwargs cannot contain dots — that lets agents include
    fully-qualified import paths on the operator side
    (`airflow.providers.cncf.kubernetes.operators.pod.KubernetesPodOperator.is_delete_operator_pod`)
    without the parser misreading the leading prefix as the
    operator name. Operator names are then canonicalised to the
    bare class for matching against the expected tuples."""
    items: set[tuple[str, ...]] = set()
    for sym in agent.get("removed_symbols") or []:
        items.add(("symbol", _canonical_class_name(str(sym))))
    rm_kwargs = agent.get("removed_kwargs") or {}
    if isinstance(rm_kwargs, dict):
        for op, kwargs in rm_kwargs.items():
            if not isinstance(kwargs, list):
                continue
            for kw in kwargs:
                items.add(("kwarg", _canonical_class_name(str(op)), str(kw)))
    for flag in agent.get("flags") or []:
        flag_s = str(flag)
        if "." in flag_s:
            op, _, kw = flag_s.rpartition(".")
            items.add(("kwarg", _canonical_class_name(op), kw))
        else:
            items.add(("symbol", _canonical_class_name(flag_s)))
    return items


# Public helper used by tests for the YAML accessor.
__all_provider_idioms__ = provider_idioms  # re-export for downstream access


@scoring_primitive("provider_compatibility_check")
def provider_compatibility_check(
    task_dir: str, agent_output: str, *, oracle_dir: str | None = None
) -> PrimitiveOutcome:
    """The agent's `output.json` matches the oracle's `expected.json`
    on each declared field of a provider/Airflow compatibility
    diagnosis. Used on tasks that ask "given this Airflow + this
    provider pin, what should we recommend?" — the agent emits a
    JSON document and the primitive grades each expected field
    independently, so a partial answer earns partial credit AND
    gratuitous extra fields cost the agent.

    Recognised expected fields:

      - `airflow`: target Airflow version. Matched on major.minor
        (`"3.0.0"` ≡ `"3.0"` ≡ `"3.0.5"`). Used when the bench
        expects the agent to recommend an Airflow bump alongside
        provider work.
      - `providers`: dict of provider-name → version (string OR
        list of strings). Each entry is one scoring item: hit when
        the agent's `providers[name]` exists and its **major**
        matches the expected major. A list value (`{"google":
        ["10", "11"]}`) admits any of the listed majors as
        correct — used when the compat matrix permits more than
        one acceptable bump (e.g. AF 3.0 admits both google 10
        and google 11). The bench grades majors because most
        provider-side breaking changes actually land on major
        boundaries (snowflake 4→5→6, google 8→10→11) and pinning
        a specific patch would punish equally-correct answers.
      - `incompatible`: bool. Hit only when expected and agent
        agree (typically both `true` for the deliberate-incompat
        fixtures).
      - `acceptable_resolutions`: list of resolution strings
        (`"airflow_bump"`, `"provider_downgrade"`). The expected
        list says which paths the bench is willing to accept; the
        agent's `resolution` (single string) earns the point if
        it appears in the set.

    Score: ``matched / (expected_items + spurious_agent_items)``.
    A spurious agent item is any field the agent populated that
    expected.json did not declare — extra `airflow` value on a
    provider-only fixture, an `incompatible: true` claim on a
    fixture that doesn't ask, or an extra provider key beyond what
    expected enumerated. Penalising spurious fields closes the
    cross-task constant-output attack: a single canned JSON that
    sets every possible field can no longer score 0.75+ across the
    corpus by accident, because each fixture's expected.json only
    declares the fields it actually expects.

    `passed=None` when expected.json declares no scoring fields the
    primitive knows how to grade — out-of-scope rather than fail.
    Missing output.json with non-empty expected items is a hard
    fail."""
    expected = _load_expected(oracle_dir)
    if expected is None:
        return PrimitiveOutcome(value=0.0, passed=None)
    expected_items = _flatten_compat_expected(expected)
    if not expected_items:
        return PrimitiveOutcome(value=0.0, passed=None)
    agent = _load_output(task_dir)
    if agent is None:
        return PrimitiveOutcome(value=0.0, passed=False)
    matched = sum(1 for item in expected_items if _compat_item_satisfied(item, agent))
    spurious = _count_spurious_compat_fields(expected, agent)
    denominator = len(expected_items) + spurious
    if denominator == 0:
        return PrimitiveOutcome(value=0.0, passed=None)
    value = matched / denominator
    return PrimitiveOutcome(value=value, passed=value == 1.0)


def _flatten_compat_expected(expected: dict) -> list[tuple]:
    """Expected fields → list of scoring items. Each item is a
    `(kind, ...)` tuple consumed by `_compat_item_satisfied`. The
    list shape (rather than set) keeps duplicate names reflective
    in malformed input — `providers: {"snowflake": "5", ...}` is
    a dict so duplicates can't survive yaml/json parsing anyway."""
    items: list[tuple] = []
    airflow = expected.get("airflow")
    if isinstance(airflow, str) and _major_minor(airflow):
        items.append(("airflow", _major_minor(airflow)))
    providers = expected.get("providers")
    if isinstance(providers, dict):
        for name, version in providers.items():
            if not isinstance(name, str):
                continue
            accepted = _expected_provider_majors(version)
            if accepted:
                items.append(("provider", name, accepted))
    incompat = expected.get("incompatible")
    if isinstance(incompat, bool):
        items.append(("incompatible", incompat))
    resolutions = expected.get("acceptable_resolutions")
    if isinstance(resolutions, list):
        cleaned = tuple(r for r in resolutions if isinstance(r, str))
        if cleaned:
            items.append(("resolution", cleaned))
    return items


def _expected_provider_majors(version: object) -> tuple[str, ...]:
    """Normalise an expected provider version field to a tuple of
    acceptable majors. Accepts a single version string or a list of
    versions (any-of match). Returns the empty tuple on malformed
    input so the caller can skip the entry without crashing."""
    if isinstance(version, str):
        major = _provider_major(version)
        return (major,) if major else ()
    if isinstance(version, list):
        majors: list[str] = []
        for v in version:
            if not isinstance(v, str):
                continue
            major = _provider_major(v)
            if major and major not in majors:
                majors.append(major)
        return tuple(majors)
    return ()


def _compat_item_satisfied(item: tuple, agent: dict) -> bool:
    kind = item[0]
    if kind == "airflow":
        want_mm = item[1]
        got = agent.get("airflow")
        if not isinstance(got, str):
            return False
        return _major_minor(got) == want_mm
    if kind == "provider":
        _, name, accepted_majors = item
        providers = agent.get("providers")
        if not isinstance(providers, dict):
            return False
        # Match on the normalised provider name so the canonical PyPI
        # package (`apache-airflow-providers-google`) the agent naturally
        # writes still matches the oracle's short key (`google`). Without
        # this, every provider entry missed AND counted as spurious — a
        # double penalty that made these fixtures unwinnable for both
        # agents regardless of the major they chose.
        #
        # Collect ALL majors the agent declared for this provider across
        # alias forms. An unambiguous answer that lands in the accepted
        # set passes; an agent that hedges by listing the same provider
        # twice under different name forms with conflicting majors
        # (`{"google": "22", "apache-airflow-providers-google": "15"}`)
        # does NOT get credit for the cherry-picked match. The duplicate
        # alias is also charged as spurious in `_count_spurious_compat_fields`.
        want = _normalise_provider_name(name)
        majors = {
            _provider_major(v)
            for k, v in providers.items()
            if isinstance(k, str) and _normalise_provider_name(k) == want and isinstance(v, str)
        }
        majors.discard(None)
        return len(majors) == 1 and next(iter(majors)) in accepted_majors
    if kind == "incompatible":
        return agent.get("incompatible") is item[1]
    if kind == "resolution":
        got = agent.get("resolution")
        if not isinstance(got, str):
            return False
        return got in item[1]
    return False


def _count_spurious_compat_fields(expected: dict, agent: dict) -> int:
    """Count agent-output fields that expected.json never declared.
    Each one is a scoring penalty. The four scalar slots (airflow,
    incompatible, resolution) contribute at most 1 each; provider
    keys contribute one per extra name."""
    spurious = 0
    if "airflow" not in expected and isinstance(agent.get("airflow"), str):
        spurious += 1
    if "incompatible" not in expected and isinstance(agent.get("incompatible"), bool):
        spurious += 1
    if "acceptable_resolutions" not in expected and isinstance(agent.get("resolution"), str):
        spurious += 1
    expected_provider_names = _expected_provider_names(expected)
    agent_providers = agent.get("providers")
    if isinstance(agent_providers, dict):
        seen_canonical: set[str] = set()
        for name in agent_providers:
            if not isinstance(name, str):
                continue
            canon = _normalise_provider_name(name)
            # Count value-malformed entries (e.g. `{"snowflake": 5}`)
            # only when the name was not expected — otherwise the
            # missed provider already costs via expected_items.
            # Compare on the normalised name so the canonical PyPI
            # package isn't punished as "extra" against a short key.
            if canon not in expected_provider_names:
                spurious += 1
            elif canon in seen_canonical:
                # A second key that normalises to an already-counted
                # provider is alias padding — the agent listed the same
                # provider twice (e.g. short + canonical form) to hedge
                # two different majors. Charge the duplicate as spurious
                # so the hedge can't score full credit.
                spurious += 1
            else:
                seen_canonical.add(canon)
    return spurious


def _expected_provider_names(expected: dict) -> set[str]:
    providers = expected.get("providers")
    if not isinstance(providers, dict):
        return set()
    return {_normalise_provider_name(name) for name in providers if isinstance(name, str)}


def _normalise_provider_name(name: str) -> str:
    """Canonicalise a provider name so the oracle's short key
    (`google`, `cncf-kubernetes`) matches whatever form the agent
    wrote: the full PyPI package (`apache-airflow-providers-google`),
    a dotted path (`apache.airflow.providers.cncf.kubernetes`), or the
    short name itself. Lowercases, unifies `.`/`_` to `-`, and strips
    the provider package prefix."""
    n = name.strip().lower().replace("_", "-").replace(".", "-")
    for prefix in ("apache-airflow-providers-", "airflow-providers-", "providers-"):
        if n.startswith(prefix):
            return n[len(prefix) :]
    return n


def _provider_major(version: str) -> str | None:
    """Extract the major component from a provider version string.
    Accepts bare majors (`"5"`), `major.minor` (`"5.8"`), and full
    `major.minor.patch` (`"5.8.0"`). Returns None when the leading
    component isn't a non-negative integer."""
    head = version.split(".", 1)[0].strip()
    if not head.isdigit():
        return None
    return head


# Public helper for downstream access to the matrix from tests.
__all_provider_compat_matrix__ = provider_compat_matrix


@scoring_primitive("python_drop_detected")
def python_drop_detected(
    task_dir: str, agent_output: str, *, oracle_dir: str | None = None
) -> PrimitiveOutcome:
    """Agent must flag `python_bump_required=true` and the correct
    from/to pair."""
    agent = _load_output(task_dir)
    expected = _load_expected(oracle_dir)
    if agent is None or expected is None:
        return PrimitiveOutcome(value=0.0, passed=False)
    if expected.get("python_bump_required") is not True:
        return PrimitiveOutcome(value=0.0, passed=None)
    for key in ("python_bump_required", "from", "to"):
        if agent.get(key) != expected.get(key):
            return PrimitiveOutcome(value=0.0, passed=False)
    return PrimitiveOutcome(value=1.0, passed=True)


def _match_scalar_field(task_dir: str, oracle_dir: str | None, key: str) -> PrimitiveOutcome:
    agent = _load_output(task_dir)
    expected = _load_expected(oracle_dir)
    if agent is None or expected is None:
        return PrimitiveOutcome(value=0.0, passed=False)
    want = expected.get(key)
    if want is None:
        return PrimitiveOutcome(value=0.0, passed=None)
    passed = agent.get(key) == want
    return PrimitiveOutcome(value=1.0 if passed else 0.0, passed=passed)


def _load_output(task_dir: str) -> dict | None:
    return _load_json(Path(task_dir) / _OUTPUT_FILE)


def _load_expected(oracle_dir: str | None) -> dict | None:
    if oracle_dir is None:
        return None
    return _load_json(Path(oracle_dir) / _EXPECTED_FILE)


def _load_json(path: Path) -> dict | None:
    if not path.is_file():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None
    return data if isinstance(data, dict) else None
