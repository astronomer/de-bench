"""One dbt selection, as an operator.

The blueprint factory's `dbt_select` kind does this for the YAML DAGs. The
hand-written ones need the same thing and there is no reason for six copies of
the same `BashOperator`, so it is here.

`dbt run --select <model>` is one opaque invocation and that is the point: a
task per model is what the cosmos-rendered `plat_dbt_analytics_daily` is for.
A growth DAG that wants one model rebuilt after it has landed something wants
this.

The warehouse takes one writer, so a selection started while the nightly
build is running fails with a lock error rather than waiting. That is why
these DAGs run in the morning, after the 04:00 build, and why anything that
only reads takes `warehouse.connect(read_only=True)` instead.
"""

from __future__ import annotations

import json
import shlex
from typing import Any, Mapping

from airflow.providers.standard.operators.bash import BashOperator

from include.lib import workspace_root

__all__ = ["PROJECT", "selection"]

#: The dbt project every growth model lives in.
PROJECT = "copperline_analytics"

#: The pinned dbt, the way every hand-written DAG calls it. A bare `dbt` is
#: either not on the PATH or is not the pinned one — `dbt/README.md` says so.
DBT = "/opt/dbt-venv/bin/dbt"


def selection(
    task_id: str,
    select: str,
    *,
    command: str = "run",
    exclude: str | None = None,
    variables: Mapping[str, Any] | None = None,
    **kwargs: Any,
) -> BashOperator:
    """A `BashOperator` running one dbt selection in the analytics project.

    Args:
        task_id: the task's id.
        select: the selection, as dbt spells it — `funnel_daily`,
            `int_touch_ordered+`, `tag:growth`.
        command: `run` by default; `build` when the selection includes a seed
            or its tests, `test` for tests alone.
        exclude: a selection to leave out.
        variables: passed as `--vars`, templated, so
            `{"run_date": "{{ ds }}"}` reaches the models.
        kwargs: anything else the operator takes — `retries`, `pool`.
    """
    parts = [DBT, command, "--select", shlex.quote(select)]
    if exclude:
        parts += ["--exclude", shlex.quote(exclude)]
    if variables:
        parts += ["--vars", shlex.quote(json.dumps(dict(variables)))]
    return BashOperator(
        task_id=task_id,
        bash_command=" ".join(parts),
        cwd=str(workspace_root() / "dbt" / PROJECT),
        **kwargs,
    )
