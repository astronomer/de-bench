"""Data-driven constants consumed by scoring primitives.

`af3_migrations.yaml` is the single source of truth for the sets of
removed imports, deprecated kwargs, context variables, etc. Primitives
load it lazily via the helpers below so `@cache` can memoise the
per-call lookup — we never want to re-parse YAML on every primitive
invocation.

Adding a new AF2->AF3 break is usually one-line YAML + a task fixture.
A new *detection shape* (not just a new symbol) still needs a new
primitive in `scoring/primitives/`."""

from __future__ import annotations

import re
from functools import cache
from pathlib import Path

import yaml

_DATA_DIR = Path(__file__).parent
_MIGRATIONS_FILE = _DATA_DIR / "af3_migrations.yaml"
_VERSION_IDIOMS_FILE = _DATA_DIR / "version_idioms.yaml"
_DAG_FACTORY_IDIOMS_FILE = _DATA_DIR / "dag_factory_idioms.yaml"
_COSMOS_IDIOMS_FILE = _DATA_DIR / "cosmos_idioms.yaml"
_PROVIDER_COMPAT_MATRIX_FILE = _DATA_DIR / "provider_compat_matrix.yaml"
_PROVIDER_REMOVALS_FILE = _DATA_DIR / "provider_removals.yaml"


@cache
def _migrations() -> dict:
    return yaml.safe_load(_MIGRATIONS_FILE.read_text(encoding="utf-8"))


@cache
def _provider_removals() -> dict:
    """Provider-package side of the AF3-era removals — module
    relocations, kwarg drops, symbol removals shipped by the
    cncf-kubernetes / snowflake / google provider releases that
    customers usually adopt alongside AF3. The accessor functions
    below union these lists with the matching `_migrations()` keys,
    so consumer primitives see a single combined view (`removed_modules`,
    `deprecated_kwargs`, `removed_symbols`)."""
    return yaml.safe_load(_PROVIDER_REMOVALS_FILE.read_text(encoding="utf-8"))


@cache
def _version_idioms() -> dict:
    return yaml.safe_load(_VERSION_IDIOMS_FILE.read_text(encoding="utf-8"))


@cache
def dag_factory_idioms() -> dict:
    """Data table for the dag-factory 0.x → 1.0 migration primitives.
    Keys: `min_version`, `loader`, `yaml`, `providers`. See
    `dag_factory_idioms.yaml` for the schema and per-key rationale."""
    return yaml.safe_load(_DAG_FACTORY_IDIOMS_FILE.read_text(encoding="utf-8"))


@cache
def cosmos_idioms() -> dict:
    """Data table for the astronomer-cosmos 0.x → 1.x migration
    primitives. Keys: `min_version`, `imports`, `constructors`,
    `render_config`, `datasets`, `project_config`. See
    `cosmos_idioms.yaml` for the schema and per-key rationale."""
    return yaml.safe_load(_COSMOS_IDIOMS_FILE.read_text(encoding="utf-8"))


@cache
def provider_compat_matrix() -> dict:
    """Provider/Airflow compatibility matrix for the diagnosis
    primitive `provider_compatibility_check`. Top-level key
    `providers`; each entry maps a provider name to a dict of
    major-version → {min_airflow, max_airflow_major_minor,
    max_known_release}. See `provider_compat_matrix.yaml` for
    schema and sources."""
    return yaml.safe_load(_PROVIDER_COMPAT_MATRIX_FILE.read_text(encoding="utf-8"))


@cache
def airflow_version_idioms(version: str) -> dict | None:
    """Idiom table for a given Airflow major.minor pin (e.g. "2.9").
    Returns None when the version is unknown (caller should treat the
    metric as N/A rather than fail)."""
    return _version_idioms().get("airflow", {}).get(version)


@cache
def provider_idioms(name: str, major: str) -> dict | None:
    """Idiom table for `<provider>` at major version `<major>`
    (e.g. ("snowflake", "6")). None when not in the table."""
    return _version_idioms().get("provider", {}).get(name, {}).get(major)


@cache
def removed_modules() -> frozenset[str]:
    """Union of airflow-core module relocations (from
    `af3_migrations.yaml`) and provider-package module relocations
    (from `provider_removals.yaml`). Consumers don't care which
    file an entry came from; the split is for authoring
    ergonomics."""
    core = _migrations().get("removed_modules") or []
    provider = _provider_removals().get("removed_modules") or []
    return frozenset([*core, *provider])


@cache
def af3_only_modules() -> frozenset[str]:
    return frozenset(_migrations()["af3_only_modules"])


@cache
def deprecated_context_vars() -> tuple[str, ...]:
    return tuple(_migrations()["deprecated_context_vars"])


@cache
def deprecated_kwargs() -> tuple[str, ...]:
    """Union of airflow-core kwarg removals and provider-package
    kwarg removals. Order is core-first, then provider, with
    duplicates dropped — primitives only care about set membership
    but a stable order keeps cache keys deterministic in tests."""
    core = _migrations().get("deprecated_kwargs") or []
    provider = _provider_removals().get("deprecated_kwargs") or []
    return _ordered_unique(core, provider)


@cache
def removed_symbols() -> tuple[str, ...]:
    """Union of airflow-core symbol removals and provider-package
    symbol removals. Same shape as `deprecated_kwargs`."""
    core = _migrations().get("removed_symbols") or []
    provider = _provider_removals().get("removed_symbols") or []
    return _ordered_unique(core, provider)


def _ordered_unique(*lists: list[str]) -> tuple[str, ...]:
    seen: set[str] = set()
    out: list[str] = []
    for items in lists:
        for item in items:
            if item not in seen:
                seen.add(item)
                out.append(item)
    return tuple(out)


@cache
def stepping_stone_floor() -> tuple[str, ...]:
    return tuple(_migrations()["stepping_stone_floor"])


@cache
def in_bundle_forbidden_pattern() -> re.Pattern[str]:
    return re.compile(_migrations()["in_bundle_forbidden_pattern"])


@cache
def sibling_skip_dirs() -> frozenset[str]:
    return frozenset(_migrations()["sibling_skip_dirs"])


@cache
def forbidden_orm_patterns() -> tuple[re.Pattern[str], ...]:
    return tuple(re.compile(p) for p in _migrations()["forbidden_orm_patterns"])


@cache
def forbidden_ti_attr_patterns() -> tuple[re.Pattern[str], ...]:
    return tuple(re.compile(p) for p in _migrations()["forbidden_ti_attr_patterns"])


@cache
def task_sdk_ipc_methods() -> tuple[str, ...]:
    """The set of IPC method names, independent of version — useful
    for the presence-only `uses_task_sdk_ipc` check."""
    return tuple(_migrations()["task_sdk_ipc_methods"])


@cache
def task_sdk_ipc_method_versions() -> dict[str, str]:
    """Map of IPC method name -> the earliest Airflow version that
    ships the method. Used by `task_sdk_ipc_version_aware` to flag
    calls that won't resolve at the project's pinned Airflow."""
    raw = _migrations()["task_sdk_ipc_methods"]
    return dict(raw)
