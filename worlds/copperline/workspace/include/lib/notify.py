"""Who gets woken when a DAG fails, and what the record says afterwards.

`notify` builds the failure callback every DAG hangs on `on_failure_callback`.
It pages the OWNING TEAM, never a person: a rotation is where an alert lands,
and an alert addressed to somebody who moved teams is an alert nobody answers.
`contracts/alerting.md` AL-1 states the rule and this is the code half of it.

    from include.lib.notify import notify

    DEFAULT_ARGS = {
        "owner": "supply",
        "retries": 2,
        "on_failure_callback": notify("supply", "carrier file missing"),
    }

`notify(...)` is a FACTORY. It returns the callback; it is not the callback
itself. Calling it takes no lock, opens no file and reads no clock, so it is
safe in `default_args` at parse time — which is where it belongs, because a
callback set per task is a callback somebody forgets on the one task that
matters.

**A notifier never raises.** Anything that goes wrong inside it is logged and
swallowed. A callback that throws replaces the real failure in the log with
its own, and then the on-call reads a traceback about the pager instead of a
traceback about the data.

**A rendered DAG does not write this line at all.** A blueprint file is YAML,
and YAML holds strings and numbers and never a callable, so `default_args` in
a `*.dag.yaml` cannot carry a callback. The factory builds one instead, from
the file's `notify` key or from the team that owns the directory — see
`include/lib/blueprint/`. So a rendered DAG pages its owning team with nothing
written down, and a team that renders its DAGs must not attach a notifier
after `render_all`: assigning over the factory's replaces it, and appending
pages the rotation twice.

**Every page leaves a record**, one JSON object a line, at
`include/data/_notifications/<dag_id>.jsonl`. It sits beside the run manifest
in `include/data/_manifest/` and for the same reason: the question "what woke
us last week, and how often" outlives the scheduler that could answer it. A
line rather than a document because an alert log is only ever appended to,
and an append needs no read, no lock and no rewrite of what is already there
— which matters in a callback, where the run is already going badly.
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any, Callable, Mapping

from . import data_dir

__all__ = ["notify", "message", "record", "notifications_path"]

log = logging.getLogger("copperline.notify")

#: The teams that own something. An alert addressed anywhere else has no
#: rotation behind it.
TEAMS = ("platform", "finance", "commerce", "supply", "growth", "customer")


def notify(
    team: str,
    summary: str | None = None,
    *,
    runbook: str | None = None,
    page: bool = True,
) -> Callable[[Mapping[str, Any]], None]:
    """Build the failure callback for a DAG or a task.

    Args:
        team: the owning team — one of platform, finance, commerce, supply,
            growth, customer. It is a TEAM AND NOT A PERSON, per
            `contracts/alerting.md` AL-1: an alert nobody owns wakes the wrong
            people and gets muted, which is worse than not alerting at all.
            The team's rotation is where it lands.
        summary: what the on-call needs to know before they open anything —
            "carrier file missing", "ledger tie broke". One short phrase. It
            is what the alert leads with, so write it for somebody reading a
            phone at 03:00.
        runbook: the runbook that says what to do about it, as a repository
            path: `ops/runbooks/alerting.md`. Worth setting —
            `docs/change-management.md` CM-4 asks the reader to check a
            runbook's review date, which they cannot do if the alert does not
            name one.
        page: True wakes the rotation now; False records the failure without
            paging, for a DAG whose failure can wait for the morning. Either
            way the record is written.

    Returns a callable taking the Airflow context, which is what
    `on_failure_callback` wants. THE SAME SHAPE WORKS AT BOTH LEVELS: hang it
    on the DAG for run failure, or on a task, or on both. A DAG-level callback
    gets a context with no `task_instance` in it, so this reads what is there
    and does not assume either shape.

    The callback is not a retry hook. It fires when the task has run out of
    retries and failed for good, not on each attempt — which is why the record
    carries the try number: a task that failed on try 1 and a task that failed
    on try 3 are two different problems.

        on_failure_callback=notify("supply", "carrier file missing")
        on_failure_callback=notify("finance", "ledger tie broke",
                                   runbook="ops/runbooks/alerting.md")
        on_failure_callback=notify("growth", "funnel late", page=False)
    """
    if team not in TEAMS:
        raise ValueError(
            f"{team!r} is not a team that owns anything; one of {', '.join(TEAMS)}. "
            "An alert names a team and never a person."
        )

    def on_failure(context: Mapping[str, Any]) -> None:
        try:
            entry = record(context, team=team, summary=summary,
                           runbook=runbook, page=page)
            log.error("%s", message(entry))
            _append(entry)
        except Exception:  # noqa: BLE001 - a notifier never masks the real failure
            log.exception("notify(%r) could not record the failure", team)

    on_failure.__name__ = f"notify_{team}"
    on_failure.__doc__ = f"Page the {team} rotation when this fails."
    return on_failure


def notifications_path(dag_id: str) -> Path:
    """`include/data/_notifications/<dag_id>.jsonl` — the DAG's alert log."""
    return data_dir() / "_notifications" / f"{dag_id}.jsonl"


def record(
    context: Mapping[str, Any],
    *,
    team: str,
    summary: str | None = None,
    runbook: str | None = None,
    page: bool = True,
) -> dict[str, Any]:
    """One failure, as the line that gets written.

    Eight fields carry the alert: `team`, `summary`, `dag_id`, `task_id`,
    `try_number`, `run_id`, `logical_date` and `exception`. `runbook` and
    `page` come along when they are set.

    NOTHING HERE IS STAMPED WITH THE WALL CLOCK. The run says which interval
    it owns and the run id says which attempt it was, and those are the two
    facts a person actually reconciles an alert against. A wall-clock stamp
    would also make a replayed interval look like a new incident.
    """
    task = context.get("task_instance")
    dag = context.get("dag")
    exception = context.get("exception") or context.get("reason")
    entry: dict[str, Any] = {
        "team": team,
        "summary": summary,
        "dag_id": getattr(task, "dag_id", None) or getattr(dag, "dag_id", None),
        "task_id": getattr(task, "task_id", None),
        "try_number": getattr(task, "try_number", None),
        "run_id": context.get("run_id") or getattr(task, "run_id", None),
        "logical_date": _text(context.get("logical_date")
                              or context.get("data_interval_start")),
        "exception": _text(exception),
    }
    if runbook:
        entry["runbook"] = runbook
    if not page:
        entry["page"] = False
    return entry


def message(entry: Mapping[str, Any]) -> str:
    """The one line the alert carries.

    The summary leads, then the team, then what broke and for which day. That
    is the order the on-call needs it in: what happened, whether it is theirs,
    and where to look.
    """
    parts = [entry.get("summary") or "failed",
             f"team={entry.get('team')}",
             f"dag={entry.get('dag_id') or '?'}",
             f"task={entry.get('task_id') or '-'}",
             f"try={entry.get('try_number') or '-'}",
             f"logical_date={entry.get('logical_date') or '-'}",
             f"run={entry.get('run_id') or '-'}"]
    if entry.get("page") is False:
        parts.append("no-page")
    if entry.get("runbook"):
        parts.append(f"runbook={entry['runbook']}")
    if entry.get("exception"):
        parts.append(f"exception={entry['exception']}")
    return " ".join(parts)


def _append(entry: Mapping[str, Any]) -> None:
    """Add one line to the DAG's alert log.

    One `O_APPEND` write of one line, so two tasks of the same run failing at
    once cannot interleave or lose each other's record. Nothing is read and
    nothing already written is touched.
    """
    path = notifications_path(entry.get("dag_id") or "unknown")
    path.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps(entry, default=str, sort_keys=True) + "\n"
    handle = os.open(path, os.O_CREAT | os.O_WRONLY | os.O_APPEND, 0o644)
    try:
        os.write(handle, line.encode("utf-8"))
    finally:
        os.close(handle)


def _text(value: Any) -> str | None:
    """A context value as one short line, or None. An exception's first line
    is the part that says what happened; the traceback is already in the log."""
    if value is None:
        return None
    return str(value).splitlines()[0][:300] or None
