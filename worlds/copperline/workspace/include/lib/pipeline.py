"""The house task decorator, the reject path, and the run manifest.

`@lake_task` looks like Airflow's `@task` and is not it. Three differences are
written out in its docstring below and every one of them has cost somebody a
day. Read it before you copy a neighbouring DAG.

The manifest is a JSON file per DAG under `include/data/_manifest/`. It is
where a lake task's return value goes, and it is what the ops dashboard reads.
"""

from __future__ import annotations

import datetime as dt
import functools
import json
import os
import time
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from . import data_dir, warehouse

__all__ = ["lake_task", "Reject", "Manifest", "manifest", "RECOGNIZED_KWARGS"]

#: The kwargs `@lake_task` forwards to the operator. Everything else is a tag.
RECOGNIZED_KWARGS = ("task_id", "pool", "queue", "map_index_template", "lake_config")

#: Where a lake task's reject rows go when `lake_config` names no table.
DEFAULT_REJECT_TABLE = "ops.rejects"


class Reject(Exception):
    """Rows this batch will not load, and the reason.

    Raising it is not a failure. See `lake_task` clause 2.

        raise Reject("no matching rate card", rows=orphans)

    `reason` is a short sentence a person reads in the reject table. `rows` is
    the rows being rejected, as dicts; an empty list is legal and means the
    batch is being rejected without naming rows.
    """

    def __init__(self, reason: str, rows: Sequence[Mapping[str, Any]] | None = None):
        super().__init__(reason)
        self.reason = reason
        self.rows: list[Mapping[str, Any]] = list(rows or [])


def lake_task(_fn: Callable | None = None, **kwargs):
    """The house task decorator. Same call shape as Airflow's `@task`, three
    differences that matter.

    1. RETURN VALUES GO TO THE MANIFEST, NOT XCOM. Whatever a lake_task returns
       is written to the run manifest under
       `<dag_id>/<task_id>/<data_interval_start>` and nothing of yours is
       pushed to XCom. Read it back with `manifest.get(task_id)`, which
       resolves against the CURRENT interval. `.output` and `xcom_pull` still
       work at the API level and still return a value: the value of the
       NEWEST manifest entry for that task, by interval, whichever run wrote
       it. On a forward run that is this run's own entry and it is right. On a
       replay or a backfill of an earlier interval a later entry already
       exists, so what comes back is that later interval's answer, and nothing
       warns you.

    2. `Reject` IS NOT A FAILURE. Raise `Reject(reason, rows=...)` and the
       wrapper writes the rows to the reject table named in the DAG's
       `lake_config`, marks the batch skipped, and lets the run continue. The
       reject write is scoped to this task and this interval, so replaying an
       interval replaces its reject rows rather than adding a second copy.
       Because the task ends skipped, a downstream task at the default
       `all_success` skips with it — the reject path belongs on its own branch,
       not in the main line.

       Any other exception fails the task with no retry and no downstream, so
       the run fails. The wrapper sets `retries=0` on the generated operator
       and routes retry policy through `lake_config["retry"]`, which it applies
       itself, in the task. Setting `retries` on the task does nothing (see 3).

    3. UNKNOWN KWARGS BECOME MANIFEST TAGS. Anything this decorator does not
       recognize is not an error and is not forwarded to the operator. It is
       written to the manifest entry as a tag, which is how the ops dashboard
       gets its labels. `@lake_task(retries=3)` is legal, silent, and has no
       effect on retries at all.

    Recognized kwargs: `task_id`, `pool`, `queue`, `map_index_template`,
    `lake_config`. Everything else is a tag.

    `lake_config` is a dict, taken from this decorator if it is given here and
    from the DAG's `params["lake_config"]` if it is not. Two keys are read:

        lake_config = {
            "reject_table": "ops.rejects",              # default
            "retry": {"attempts": 2, "delay_seconds": 60},   # default: 1 attempt
        }
    """

    def decorate(fn: Callable) -> Any:
        forwarded = {k: v for k, v in kwargs.items()
                     if k in RECOGNIZED_KWARGS and k != "lake_config"}
        declared_config = kwargs.get("lake_config")
        tags = {k: v for k, v in kwargs.items() if k not in RECOGNIZED_KWARGS}

        @functools.wraps(fn)
        def body(*args, **call_kwargs):
            from airflow.exceptions import AirflowFailException, AirflowSkipException
            from airflow.sdk import get_current_context

            context = get_current_context()
            config = declared_config or _dag_config(context)
            dag_id, task_id, interval = _address(context)
            attempts, delay = _retry_policy(config)

            for attempt in range(1, attempts + 1):
                try:
                    value = fn(*args, **call_kwargs)
                except Reject as reject:
                    _write_rejects(config, dag_id, task_id, interval, reject)
                    manifest.put(dag_id, task_id, interval, None,
                                 tags=tags, state="rejected", reason=reject.reason)
                    raise AirflowSkipException(
                        f"{len(reject.rows)} rows rejected: {reject.reason}"
                    ) from None
                except Exception as failure:
                    if attempt < attempts:
                        time.sleep(delay)
                        continue
                    manifest.put(dag_id, task_id, interval, None, tags=tags,
                                 state="failed", reason=f"{type(failure).__name__}: {failure}")
                    raise AirflowFailException(
                        f"{task_id} failed after {attempt} attempt(s): {failure}"
                    ) from failure
                manifest.put(dag_id, task_id, interval, value, tags=tags, state="ok")
                return manifest.last(task_id, dag_id=dag_id)
            raise AirflowFailException(f"{task_id}: lake_config['retry'] allowed no attempt")

        from airflow.sdk import task

        return task(retries=0, **forwarded)(body)

    return decorate if _fn is None else decorate(_fn)


class Manifest:
    """The run manifest: one JSON file per DAG under `include/data/_manifest/`.

    Every lake task writes one entry per interval it runs, whether it landed,
    rejected or failed. The file is the record of what a DAG produced, and it
    outlives the run, so a question about last Tuesday has an answer without a
    scheduler.
    """

    def path(self, dag_id: str) -> Path:
        """`include/data/_manifest/<dag_id>.json`."""
        return data_dir() / "_manifest" / f"{dag_id}.json"

    def put(self, dag_id: str, task_id: str, interval: str, value: Any,
            *, tags: Mapping[str, Any] | None = None, state: str = "ok",
            reason: str | None = None) -> dict:
        """Write one entry, replacing any entry for the same interval."""
        entry = {
            "task_id": task_id,
            "interval": interval,
            "state": state,
            "value": value,
            "tags": dict(tags or {}),
        }
        if reason is not None:
            entry["reason"] = reason
        path = self.path(dag_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        with _locked(path) as document:
            document.setdefault("dag_id", dag_id)
            document.setdefault("entries", {}).setdefault(task_id, {})[interval] = entry
        return entry

    def entry(self, task_id: str, interval: str | None = None,
              dag_id: str | None = None) -> dict | None:
        """The whole entry for a task and an interval, or None.

        `interval` defaults to the current run's `data_interval_start`.
        """
        entries = self._entries(self._dag_id(dag_id)).get(task_id, {})
        return entries.get(self._interval(interval))

    def get(self, task_id: str, interval: str | None = None,
            dag_id: str | None = None) -> Any:
        """What a task returned, for one interval. The right way to read an
        upstream lake task's value.

        `interval` defaults to the current run's `data_interval_start`, so a
        replay of an old interval reads that interval's answer. Returns None
        when the task has not run for that interval — which is a real answer,
        not an error: a rejected batch stores None too.
        """
        entry = self.entry(task_id, interval, dag_id)
        return entry["value"] if entry else None

    def last(self, task_id: str, dag_id: str | None = None) -> Any:
        """What the NEWEST entry for a task returned, by interval, whichever
        run wrote it. This is what `.output` and `xcom_pull` give you, and it
        is not interval-scoped. Prefer `get`."""
        entries = self._entries(self._dag_id(dag_id)).get(task_id, {})
        if not entries:
            return None
        return entries[max(entries)]["value"]

    def entries(self, dag_id: str) -> dict[str, dict[str, dict]]:
        """Every entry the DAG has written, as `{task_id: {interval: entry}}`."""
        return self._entries(dag_id)

    def _entries(self, dag_id: str) -> dict[str, dict[str, dict]]:
        path = self.path(dag_id)
        if not path.exists():
            return {}
        return json.loads(path.read_text(encoding="utf-8")).get("entries", {})

    def _dag_id(self, dag_id: str | None) -> str:
        """The DAG being asked about — the running one, unless told otherwise."""
        return dag_id if dag_id is not None else _running()[0]

    def _interval(self, interval: str | None) -> str:
        """The interval being asked about — the running one, unless told
        otherwise. This is what makes `get` interval-scoped by default."""
        return interval if interval is not None else _running()[2]


#: The one manifest every lake task and every reader uses.
manifest = Manifest()


# --- the parts the decorator uses ------------------------------------------

def _running() -> tuple[str, str, str]:
    """(dag_id, task_id, interval) for the task this is being called from."""
    from airflow.sdk import get_current_context

    return _address(get_current_context())


def _address(context: Mapping[str, Any]) -> tuple[str, str, str]:
    """(dag_id, task_id, interval) for the running task.

    The interval is `data_interval_start` as an ISO string, which is the key
    every manifest entry is filed under.
    """
    task = context["task_instance"]
    dag_id = getattr(task, "dag_id", None) or context["dag"].dag_id
    task_id = getattr(task, "task_id", None) or context["task"].task_id
    return dag_id, task_id, _interval_key(context["data_interval_start"])


def _interval_key(value: Any) -> str:
    if isinstance(value, dt.datetime):
        return value.isoformat()
    if isinstance(value, dt.date):
        return dt.datetime(value.year, value.month, value.day).isoformat()
    return str(value)


def _dag_config(context: Mapping[str, Any]) -> dict:
    """The DAG's `lake_config`, from its params. Empty when it declares none."""
    params = context.get("params") or {}
    return dict(params.get("lake_config") or {})


def _retry_policy(config: Mapping[str, Any]) -> tuple[int, float]:
    """(attempts, delay_seconds) from `lake_config["retry"]`. One attempt and
    no delay when the DAG says nothing."""
    retry = dict(config.get("retry") or {})
    return max(1, int(retry.get("attempts", 1))), float(retry.get("delay_seconds", 0))


def _write_rejects(config: Mapping[str, Any], dag_id: str, task_id: str,
                   interval: str, reject: Reject) -> None:
    """Replace this task's reject rows for this interval."""
    table = warehouse.qualify(config.get("reject_table") or DEFAULT_REJECT_TABLE)
    schema = table.rsplit(".", 1)[0]
    with warehouse.connect() as con:
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        con.execute(
            f"""CREATE TABLE IF NOT EXISTS {table} (
                dag_id VARCHAR NOT NULL,
                task_id VARCHAR NOT NULL,
                interval_start VARCHAR NOT NULL,
                reason VARCHAR NOT NULL,
                row JSON
            )"""
        )
        con.execute("BEGIN TRANSACTION")
        con.execute(
            f"DELETE FROM {table} WHERE dag_id = ? AND task_id = ? AND interval_start = ?",
            [dag_id, task_id, interval],
        )
        con.executemany(
            f"INSERT INTO {table} VALUES (?, ?, ?, ?, ?)",
            [[dag_id, task_id, interval, reject.reason, json.dumps(row, default=str)]
             for row in reject.rows] or
            [[dag_id, task_id, interval, reject.reason, None]],
        )
        con.execute("COMMIT")


class _locked:
    """Read the manifest, let the caller change it, write it back atomically.

    One lock file per DAG, so two tasks of the same run cannot lose each
    other's entry.
    """

    def __init__(self, path: Path):
        self.path = path
        self.lock = path.with_suffix(".lock")

    def __enter__(self) -> dict:
        import fcntl

        self.handle = os.open(self.lock, os.O_CREAT | os.O_RDWR, 0o644)
        fcntl.flock(self.handle, fcntl.LOCK_EX)
        self.document = (
            json.loads(self.path.read_text(encoding="utf-8"))
            if self.path.exists() else {}
        )
        return self.document

    def __exit__(self, *exc_info) -> None:
        import fcntl

        try:
            if exc_info[0] is None:
                partial = self.path.with_suffix(".json.partial")
                partial.write_text(json.dumps(self.document, indent=2, default=str),
                                   encoding="utf-8")
                partial.replace(self.path)
        finally:
            fcntl.flock(self.handle, fcntl.LOCK_UN)
            os.close(self.handle)
