"""`dbt_select` — run one dbt selection."""

from __future__ import annotations

import json
import shlex
from typing import Any

from airflow.providers.standard.operators.bash import BashOperator

from ... import workspace_root
from ..registry import BlueprintError, register

__all__ = ["dbt_select", "COMMANDS", "DEFAULT_PROJECT"]

#: The pinned dbt, the way every hand-written DAG calls it. A bare `dbt` is
#: either not on the PATH or is not the pinned one — `dbt/README.md` says so.
DBT = "/opt/dbt-venv/bin/dbt"

#: The dbt subcommands a blueprint may run. Nothing here drops or seeds.
COMMANDS = ("run", "build", "test", "snapshot", "compile")

#: The project a step means when it names none.
DEFAULT_PROJECT = "copperline_analytics"


def dbt_select(step: Any, ctx: Any) -> Any:
    """Build the dbt run.

    Keys:
        select        the selection, as dbt spells it: `tag:finance`,
                      `marts.daily_revenue+`, `state:modified`. Required.
        exclude       a selection to leave out.
        command       run (default), build, test, snapshot or compile.
        project       the dbt project under `dbt/`. `copperline_analytics`
                      by default.
        target        the profile target.
        vars          a mapping passed as `--vars`, templated, so
                      `{run_date: "{{ ds }}"}` reaches the models.
        full_refresh  true to add `--full-refresh`.

    ONE TASK, ONE SELECTION. This kind is for the case where one opaque dbt
    invocation is what you want. When you want a task per model, with a retry
    per model, that is what the cosmos-rendered DAG is for, and a blueprint is
    the wrong tool.

    `--full-refresh` on the shared warehouse rebuilds every model the
    selection matches and holds the single writer for as long as it takes.
    `projects/platform/README.md` says when that is allowed.
    """
    command = step.get("command", "run")
    if command not in COMMANDS:
        raise BlueprintError(
            f"step {step.name!r}: command is one of {', '.join(COMMANDS)}, "
            f"not {command!r}"
        )
    project = step.get("project", DEFAULT_PROJECT)
    parts = [DBT, command, "--select", shlex.quote(ctx.render(step["select"]))]
    if step.get("exclude"):
        parts += ["--exclude", shlex.quote(ctx.render(step["exclude"]))]
    if step.get("target"):
        parts += ["--target", shlex.quote(str(step["target"]))]
    if step.get("vars"):
        parts += ["--vars", shlex.quote(json.dumps(ctx.render(step["vars"])))]
    if step.get("full_refresh"):
        parts.append("--full-refresh")
    return BashOperator(
        task_id=step.name,
        bash_command=" ".join(parts),
        cwd=str(workspace_root() / "dbt" / project),
    )


register("dbt_select", dbt_select)
