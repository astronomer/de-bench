"""Registry for scoring primitives. Tasks declare `scoring.metrics: [name, ...]`
in `task.yaml`; the runner looks each up here and invokes it."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True, slots=True)
class PrimitiveOutcome:
    """What a scoring primitive returns.

    `passed=None` means the metric does not apply to the given task and
    should be dropped from the weighted total via `renormalised_weights`.
    Weights come from the task's `scoring.weights`, not from the primitive.
    """

    value: float
    passed: bool | None = None


class ScoringPrimitive(Protocol):
    """Primitive signature.

    `oracle_dir` is the path to the unmodified *source* task directory
    — the one that has `solution/`, `expected.json`, and every other
    "answer-key" file. It is kept **outside** `task_dir` (the agent's
    working copy) so the agent cannot discover it by `ls`. Only
    oracle-aware primitives (e.g. `structural_match_taskids`,
    `detected_airflow_version_match`) consume it. Primitives that
    don't need the oracle ignore the argument."""

    def __call__(
        self,
        task_dir: str,
        agent_output: str,
        *,
        oracle_dir: str | None = None,
    ) -> PrimitiveOutcome: ...


_REGISTRY: dict[str, ScoringPrimitive] = {}


def scoring_primitive(name: str) -> Callable[[ScoringPrimitive], ScoringPrimitive]:
    """Decorator. Register a function as a scoring primitive under `name`."""

    def _wrap(fn: ScoringPrimitive) -> ScoringPrimitive:
        if name in _REGISTRY:
            raise ValueError(f"scoring primitive already registered: {name!r}")
        _REGISTRY[name] = fn
        return fn

    return _wrap


def get_primitive(name: str) -> ScoringPrimitive:
    if name not in _REGISTRY:
        raise KeyError(f"unknown scoring primitive: {name!r}")
    return _REGISTRY[name]


def registered_primitives() -> tuple[str, ...]:
    return tuple(_REGISTRY)
