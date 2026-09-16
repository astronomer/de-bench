"""Task-instance listeners: the run ledger the ops dashboard reads.

Every task instance that finishes writes one row to `ops/runs/<dag_id>.jsonl`:
what ran, which interval it ran for, how it ended, and which try it was. The
dashboard reads those files, and so does whoever is asked why last Tuesday's
figures moved.

It is a listener rather than a callback on purpose. A callback is per DAG and
somebody has to remember it; a listener covers the deployment, including the
DAGs the factory renders and the ones a team adds next week.

Written when the deployment moved off the old scheduler. The ledger has been
running since and nobody has had to touch it.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

from airflow.listeners import hookimpl
from airflow.plugins_manager import AirflowPlugin

#: Where the ledger goes. One newline-delimited JSON file per DAG.
RUN_LEDGER = Path("include/data/_runs")


def _write(state: str, task_instance: Any, session: Any = None) -> None:
    """Append one row to the DAG's ledger.

    `session` is the scheduler's database session. It is not used to write the
    ledger — the ledger is a file, so a slow write cannot hold a scheduler
    transaction open — but it is what the hookspec hands us, and a listener
    that does not take it is not called.
    """
    row = {
        "dag_id": task_instance.dag_id,
        "task_id": task_instance.task_id,
        "run_id": getattr(task_instance, "run_id", None),
        "map_index": getattr(task_instance, "map_index", -1),
        "try_number": getattr(task_instance, "try_number", None),
        "state": state,
        "start_date": _text(getattr(task_instance, "start_date", None)),
        "end_date": _text(getattr(task_instance, "end_date", None)),
        "duration": getattr(task_instance, "duration", None),
        "operator": getattr(task_instance, "operator", None),
        "pool": getattr(task_instance, "pool", None),
    }
    path = RUN_LEDGER / f"{task_instance.dag_id}.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(row, default=str) + "\n")


def _text(value: Any) -> str | None:
    return None if value is None else str(value)


@hookimpl
def on_task_instance_success(previous_state, task_instance, session):
    """A task finished. Record it."""
    _write("success", task_instance, session)


@hookimpl
def on_task_instance_failed(previous_state, task_instance, error, session):
    """A task failed. Record it, with the error's text.

    The notification is somebody else's job — `include/lib/notify.py` does that
    from the DAG's own callback. This is the ledger, and a ledger records
    everything or it is not one.
    """
    row_error = None if error is None else str(error)
    _write(f"failed: {row_error}" if row_error else "failed", task_instance, session)


@hookimpl
def on_task_instance_running(previous_state, task_instance, session):
    """A task started. Recorded so a run that never finishes is visible as a
    row with no partner, which is how the dashboard finds a hung task."""
    _write("running", task_instance, session)


class CopperlineListenerPlugin(AirflowPlugin):
    """Registers the module above as a listener."""

    name = "copperline_run_ledger"
    listeners = [sys.modules[__name__]]
