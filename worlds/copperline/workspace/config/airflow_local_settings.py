"""Cluster policies for the Copperline deployment.

Two rules the platform team got tired of writing on review, applied to every
task in every project:

1. Every task retries at least twice. Two is the house number and the review
   comment that asked for it was written 140 times before this file existed.
2. Every task carries an owner, and the owner is a team, not a person. A task
   with no owner is a task nobody is told about when it fails.

A policy raises `AirflowClusterPolicyViolation` to reject a DAG at parse and
mutates the object to fix one quietly. These mutate: a rejected DAG is a broken
DAG in the list, and a missing owner is not worth breaking a deployment over.

`docs/change-management.md` governs a change here. The policies are platform
infrastructure and they apply across every team's folder, so a team that wants
an exception raises it rather than working around it.
"""

from __future__ import annotations

#: The house retry count. `docs/` says nothing about it; the review comments do.
MINIMUM_RETRIES = 2

#: Where a task with no owner is addressed. It should never be reached: every
#: DAG in the tree sets an owner in `default_args`.
DEFAULT_OWNER = "data-platform"

#: The teams a task may be owned by. A DAG under `projects/<team>/` uses that
#: team's name.
TEAMS = ("platform", "finance", "commerce", "supply", "growth", "customer",
         "data-platform")

#: Tasks that are allowed no retry, because a retry of one is wrong rather than
#: slow. The legacy history bracket is the example: running it twice marks a
#: night's history complete while the second half is still loading.
NO_RETRY_TASKS = ("history_complete", "close_bracket")


def task_policy(task) -> None:
    """Applied to every task, at parse, before the DAG is registered.

    Raises the retry count to the house minimum and fills in a missing owner.
    Neither one overrides a task that has said something louder: a task that
    asks for more than two retries keeps its number, and a task in
    `NO_RETRY_TASKS` keeps its zero.
    """
    if task.task_id in NO_RETRY_TASKS:
        return
    if getattr(task, "retries", None) is None or task.retries < MINIMUM_RETRIES:
        task.retries = MINIMUM_RETRIES
    owner = getattr(task, "owner", None)
    if not owner or owner in ("airflow", "Airflow"):
        task.owner = DEFAULT_OWNER


def dag_policy(dag) -> None:
    """Applied to every DAG, at parse.

    Fills in the tag that says which team's folder the DAG came from, so the UI
    filters by team whether or not the author remembered. It reads the DAG's
    file path and nothing else, and it does no work of its own.
    """
    path = str(getattr(dag, "fileloc", "") or "")
    parts = path.split("/")
    if "projects" in parts:
        after = parts[parts.index("projects") + 1:]
        team = after[0] if after else ""
        if team and team not in (dag.tags or []):
            dag.tags = sorted({*(dag.tags or []), team})
