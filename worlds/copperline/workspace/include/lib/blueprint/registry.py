"""The blueprint registry: a name, and the builder it stands for.

Five kinds ship in `kinds/` and register themselves when this package is
imported. A team that needs a sixth registers its own from
`projects/<team>/dags/kinds.py`, which the loader imports when the file
exists. That is the documented way to extend the factory, and it is the only
one: `include/lib/blueprint/` is shared code, and a kind that only one team
uses does not belong in it.

    # projects/supply/dags/kinds.py
    from include.lib.blueprint import registry

    def sftp_drop(step, ctx):
        \"\"\"Write a rendered file to a partner's SFTP endpoint.\"\"\"
        return SFTPPutOperator(
            task_id=step.name,
            local_path=ctx.render(step["local_path"]),
            remote_path=ctx.render(step["remote_path"]),
            ssh_conn_id=step["conn_id"],
        )

    registry.register("sftp_drop", sftp_drop)

A builder takes the step and a render context and returns ONE operator. It
does not open the warehouse, build a path or write a partition itself — that
is `include/lib/`'s job, and the builder calls into it.
"""

from __future__ import annotations

from typing import Callable

__all__ = ["BlueprintError", "MissingStepKey", "register", "get", "names", "registered"]

Builder = Callable[..., object]

_BUILDERS: dict[str, Builder] = {}


class BlueprintError(Exception):
    """A blueprint file or a builder that cannot be turned into a DAG."""


class MissingStepKey(BlueprintError, KeyError):
    """A blueprint needs a key the step does not carry.

    It is a `KeyError` as well, so `"mode" in step` and `step.get("mode")` do
    the ordinary thing and only `step["mode"]` raises.
    """

    def __str__(self) -> str:
        return str(self.args[0]) if self.args else ""


def register(name: str, builder: Builder) -> Builder:
    """Add a kind under `name`, and return the builder so this can be used as
    a decorator.

    Registering a name twice with the same builder is a no-op, which is what
    makes importing a team's `kinds.py` twice harmless. Registering a name
    twice with two different builders raises: two teams that both call their
    kind `export` would otherwise get whichever module imported last, and the
    DAG that broke would be in the team that did nothing wrong.
    """
    existing = _BUILDERS.get(name)
    if existing is not None and existing is not builder:
        raise BlueprintError(
            f"blueprint {name!r} is already registered to "
            f"{getattr(existing, '__module__', '?')}.{getattr(existing, '__name__', '?')}"
        )
    _BUILDERS[name] = builder
    return builder


def get(name: str) -> Builder:
    """The builder for a kind. Raises `BlueprintError` when nothing claims the
    name, and says what is registered, because the usual cause is a team kind
    whose `kinds.py` was never imported."""
    try:
        return _BUILDERS[name]
    except KeyError:
        raise BlueprintError(
            f"no blueprint named {name!r}; registered: {', '.join(names())}"
        ) from None


def names() -> list[str]:
    """Every registered kind, sorted."""
    return sorted(_BUILDERS)


def registered(name: str) -> bool:
    """Whether a kind is registered."""
    return name in _BUILDERS
