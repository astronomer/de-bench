"""Turning a `*.dag.yaml` into a DAG.

`plan` reads the file and says what the DAG will be — id, schedule, steps, and
the order they depend in. It needs no Airflow, so it is what a check or a
review tool asks. `render_file` takes that plan and builds the DAG object.

`depends_on` is the only wiring. There is no way to express a dependency
across two YAML files; a DAG that needs one takes an asset or a sensor.
"""

from __future__ import annotations

import importlib.util
import sys
from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from string import Template
from typing import Any

from .. import workspace_root
from ..notify import TEAMS as NOTIFY_TEAMS
from ..notify import notify as build_notifier
from .registry import BlueprintError, MissingStepKey, get

__all__ = ["Step", "Plan", "RenderContext", "plan", "render_file", "render_all",
           "START_DATE", "TOP_LEVEL_KEYS"]

#: Where a blueprint DAG starts when the file names no `start_date`. It is the
#: first day the warehouse holds, so a rendered DAG has history behind it.
START_DATE = (2024, 2, 4)

#: The keys the top level of a blueprint file may carry. `dag_id`, `schedule`
#: and `steps` are required; the rest have defaults.
TOP_LEVEL_KEYS = ("dag_id", "schedule", "description", "default_args",
                  "start_date", "tags", "notify", "steps")


class Step(Mapping):
    """One entry under `steps`.

    It is a mapping over the step's own keys, so a builder reads
    `step["table"]` and `step.get("mode", "append")`. `name` is the step's name
    in the file, and it becomes the task id. `blueprint` is the kind.
    `depends_on` is the step names this one waits for.
    """

    def __init__(self, name: str, config: Mapping[str, Any]):
        self.name = name
        self.blueprint = config.get("blueprint")
        depends = config.get("depends_on") or []
        self.depends_on: list[str] = [depends] if isinstance(depends, str) else list(depends)
        self._config = {k: v for k, v in config.items()
                        if k not in ("blueprint", "depends_on")}

    def __getitem__(self, key: str) -> Any:
        try:
            return self._config[key]
        except KeyError:
            raise MissingStepKey(
                f"step {self.name!r} ({self.blueprint}) needs a {key!r} key"
            ) from None

    def __iter__(self) -> Iterator[str]:
        return iter(self._config)

    def __len__(self) -> int:
        return len(self._config)

    def __repr__(self) -> str:
        return f"<Step {self.name} blueprint={self.blueprint}>"


@dataclass(frozen=True)
class Plan:
    """What a blueprint file says, before any of it is built."""

    path: Path
    dag_id: str
    schedule: Any
    steps: list[Step]
    description: str = ""
    default_args: dict[str, Any] = field(default_factory=dict)
    start_date: tuple[int, int, int] = START_DATE
    tags: list[str] = field(default_factory=list)
    team: str = ""
    notify: dict[str, Any] | None = None

    @property
    def task_ids(self) -> list[str]:
        """The task ids this file renders, in dependency order."""
        return [step.name for step in self.steps]

    @property
    def edges(self) -> list[tuple[str, str]]:
        """Every (upstream, downstream) pair `depends_on` asks for."""
        return [(up, step.name) for step in self.steps for up in step.depends_on]


class RenderContext:
    """What a builder is handed beside its step.

    `dag_id`, `team`, `path` and `default_args` say which file is being built.
    `workspace` is the repository root, for a builder that has to name a path.
    """

    def __init__(self, plan: Plan, step: Step):
        self.plan = plan
        self.step = step
        self.dag_id = plan.dag_id
        self.team = plan.team
        self.path = plan.path
        self.default_args = dict(plan.default_args)
        self.workspace = workspace_root()

    def render(self, value: Any) -> Any:
        """One value from the step, as the operator should receive it.

        Airflow's `{{ ... }}` templates are LEFT ALONE. The operator renders
        those at run time against the run's own context, which is why a
        blueprint never has to know the interval and why a step may write
        `{{ ds }}` in any templated field.

        What this does fill in is the two things the file cannot know about
        itself, written `$dag_id` and `$step`. So a step may say
        `local_path: "include/data/marts/${step}_{{ ds }}.csv"` and get its own
        name in the path. Lists and mappings are rendered through, element by
        element. Anything that is not a string comes back as it is.
        """
        if isinstance(value, str):
            return Template(value).safe_substitute(dag_id=self.dag_id, step=self.step.name)
        if isinstance(value, Mapping):
            return {k: self.render(v) for k, v in value.items()}
        if isinstance(value, (list, tuple)):
            return [self.render(v) for v in value]
        return value


def plan(path: str | Path) -> Plan:
    """Read a blueprint file and order its steps.

    Raises `BlueprintError` for the four things a file can get wrong that are
    worth naming: a missing `dag_id`, `schedule` or `steps`; a step with no
    `blueprint`; a `depends_on` that names a step the file does not have; and a
    cycle. Everything else is accepted, including a key no blueprint knows —
    a misspelled key is ignored rather than rejected, so check the spelling
    when a step quietly does the default thing.
    """
    import yaml

    path = Path(path)
    document = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    for key in ("dag_id", "schedule", "steps"):
        if key not in document:
            raise BlueprintError(f"{path.name}: no {key}")

    steps = {name: Step(name, config or {}) for name, config in document["steps"].items()}
    for step in steps.values():
        if not step.blueprint:
            raise BlueprintError(f"{path.name}: step {step.name!r} names no blueprint")
        for upstream in step.depends_on:
            if upstream not in steps:
                raise BlueprintError(
                    f"{path.name}: step {step.name!r} depends on {upstream!r}, "
                    "which this file does not have"
                )

    team = _team(path)
    tags = document.get("tags") or ([team, "blueprint"] if team else ["blueprint"])
    start = document.get("start_date")
    return Plan(
        path=path,
        dag_id=document["dag_id"],
        schedule=document["schedule"],
        steps=_ordered(path.name, steps),
        description=document.get("description") or "",
        default_args=dict(document.get("default_args") or {}),
        start_date=_start_date(start),
        tags=list(tags),
        team=team,
        notify=_notify_spec(document.get("notify", None), team),
    )


def render_file(path: str | Path) -> Any:
    """Build the DAG one blueprint file describes, and return it.

    The DAG is `catchup=False` and `max_active_runs=1`: a rendered DAG writes
    the warehouse, and two runs of one writing DAG race for the same partition.
    Its `doc_md` is the file's `description` and the file's own path, so
    whoever opens the DAG in the UI knows which YAML to edit.

    IT ALSO PAGES. A blueprint file cannot carry a callable, so the factory
    builds the failure callback from the file's `notify` key — or, when the
    file says nothing, from the team that owns the directory. The callback goes
    on the DAG and into `default_args`, so every task a builder makes inherits
    it. A team that renders its DAGs does NOT have to attach a notifier
    afterwards, and should not: assigning `on_failure_callback` over a rendered
    DAG replaces this one rather than adding to it.
    """
    import pendulum
    from airflow.sdk import DAG

    from . import kinds  # noqa: F401 - registers the five shipped kinds

    blueprint = plan(path)
    _import_team_kinds(blueprint.path)
    year, month, day = blueprint.start_date
    default_args = dict(blueprint.default_args)
    on_failure = build_notifier(**blueprint.notify) if blueprint.notify else None
    if on_failure is not None:
        default_args.setdefault("on_failure_callback", on_failure)

    with DAG(
        dag_id=blueprint.dag_id,
        schedule=blueprint.schedule,
        start_date=pendulum.datetime(year, month, day, tz="UTC"),
        catchup=False,
        max_active_runs=1,
        on_failure_callback=on_failure,
        default_args=default_args,
        tags=blueprint.tags,
        description=blueprint.description,
        doc_md=f"{blueprint.description}\n\nRendered from `{_relative(blueprint.path)}`.",
    ) as dag:
        tasks = {}
        for step in blueprint.steps:
            builder = get(step.blueprint)
            task = builder(step, RenderContext(blueprint, step))
            if task is None:
                raise BlueprintError(
                    f"{blueprint.dag_id}: the {step.blueprint} builder returned "
                    "nothing, and a builder returns one operator"
                )
            tasks[step.name] = task
        for upstream, downstream in blueprint.edges:
            tasks[upstream] >> tasks[downstream]
    return dag


def render_all(directory: str | Path) -> dict[str, Any]:
    """Every `*.dag.yaml` in a directory, rendered, as `{dag_id: DAG}`.

    A team's `dags/` folder renders its blueprints in two lines:

        from include.lib.blueprint import render_all

        globals().update(render_all(Path(__file__).parent))
    """
    return {
        dag.dag_id: dag
        for dag in (render_file(p) for p in sorted(Path(directory).glob("*.dag.yaml")))
    }


# --- the parts plan and render use -----------------------------------------

def _ordered(label: str, steps: dict[str, Step]) -> list[Step]:
    """Steps in dependency order. Raises on a cycle and names the steps in it."""
    order: list[Step] = []
    done: set[str] = set()
    remaining = dict(steps)
    while remaining:
        ready = [name for name, step in remaining.items()
                 if all(up in done for up in step.depends_on)]
        if not ready:
            raise BlueprintError(
                f"{label}: {', '.join(sorted(remaining))} depend on each other"
            )
        for name in ready:
            order.append(remaining.pop(name))
            done.add(name)
    return order


def _notify_spec(declared: Any, team: str) -> dict[str, Any] | None:
    """Who a rendered DAG pages, as plain data.

    A blueprint file cannot carry a callable — YAML holds strings and numbers —
    so the file names the team and `render_file` builds the callback. Four
    shapes are accepted, and the first is the one almost every file uses:

        (the key is absent)     page the team that owns the directory
        notify: false           page nobody, deliberately
        notify: finance         page that team
        notify:                 page that team, with the rest of it
          team: finance
          summary: ledger tie broke
          runbook: ops/runbooks/alerting.md
          page: false

    Returns None when nothing is to be paged. The default is the owning team
    rather than nobody, because a rendered DAG that pages nobody is a DAG whose
    failures are silent, and the file that would have to remember to say so is
    the file nobody edits.
    """
    if declared is False:
        return None
    if declared is None:
        return {"team": team} if team in NOTIFY_TEAMS else None
    if isinstance(declared, str):
        return {"team": declared}
    if isinstance(declared, Mapping):
        spec = dict(declared)
        spec.setdefault("team", team)
        return spec if spec.get("team") else None
    raise BlueprintError(
        f"notify is a team name, a mapping or false, not {declared!r}"
    )


def _team(path: Path) -> str:
    """The owning team, from `projects/<team>/dags/<file>`."""
    parts = path.resolve().parts
    if "projects" in parts:
        after = parts[parts.index("projects") + 1:]
        if after:
            return after[0]
    return ""


def _start_date(value: Any) -> tuple[int, int, int]:
    import datetime as dt

    if value is None:
        return START_DATE
    if isinstance(value, dt.date):
        return (value.year, value.month, value.day)
    parsed = dt.date.fromisoformat(str(value)[:10])
    return (parsed.year, parsed.month, parsed.day)


def _relative(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(workspace_root()))
    except ValueError:
        return str(path)


def _import_team_kinds(path: Path) -> None:
    """Import `projects/<team>/dags/kinds.py` when the team ships one.

    Imported once per process and by its dotted name, so a second render of
    the same team reuses it and `register` never sees the same builder twice.
    """
    kinds_file = path.resolve().parent / "kinds.py"
    if not kinds_file.exists():
        return
    team = _team(path)
    module_name = f"projects.{team}.dags.kinds" if team else f"blueprint_kinds_{path.stem}"
    if module_name in sys.modules:
        return
    spec = importlib.util.spec_from_file_location(module_name, kinds_file)
    if spec is None or spec.loader is None:  # pragma: no cover - unreadable file
        raise BlueprintError(f"cannot import {kinds_file}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    try:
        spec.loader.exec_module(module)
    except Exception:
        del sys.modules[module_name]
        raise
