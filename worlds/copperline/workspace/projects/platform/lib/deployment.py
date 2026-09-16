"""Talking to the deployment itself: the API server, and the secret store.

Airflow 3 tasks talk to the API server rather than to the metadata database, so
anything that reads or changes deployment state — a connection, a pool, a DAG
run — goes through `/api/v2/`. This is the small wrapper the two platform DAGs
that need it share.

It is here rather than in `include/lib/` on purpose. The house library is what
every team imports and none of them has any business changing the deployment;
this is the platform team's own code, in the platform team's own `lib/`.

Nothing here works at import. The base URL and the token are read when a call
is made, so a DAG that imports this and never calls it needs no credentials.
"""

from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

__all__ = ["BASE_URL_ENV", "TOKEN_ENV", "SECRET_ROOT_ENV", "api", "secret",
           "trigger_dag_run", "wait_for_dag_run"]

BASE_URL_ENV = "AIRFLOW__API__BASE_URL"
TOKEN_ENV = "AIRFLOW_API_TOKEN"
SECRET_ROOT_ENV = "COPPERLINE_SECRET_ROOT"

#: Terminal run states. Anything else means the run is still going.
FINISHED = ("success", "failed")


def api(method: str, path: str, body: dict | None = None) -> dict:
    """One call against `/api/v2/`. Returns the decoded body, or `{}`.

    Args:
        method: GET, PUT, POST, PATCH or DELETE.
        path: the path under `/api/v2/`, with no leading slash.
        body: the request body, as a dict. Omitted for a GET.

    Raises `urllib.error.HTTPError` with the server's own status, because the
    status is the answer: a 404 from a DELETE is a key that was already gone,
    and the caller is the one that knows whether that matters.
    """
    base = os.environ[BASE_URL_ENV].rstrip("/")
    request = urllib.request.Request(
        f"{base}/api/v2/{path.lstrip('/')}",
        method=method,
        data=json.dumps(body).encode("utf-8") if body is not None else None,
        headers={
            "Authorization": f"Bearer {os.environ[TOKEN_ENV]}",
            "Content-Type": "application/json",
        },
    )
    with urllib.request.urlopen(request) as response:  # noqa: S310 - our own API server
        payload = response.read()
    return json.loads(payload) if payload else {}


def secret(reference: str) -> str:
    """Resolve `secret://<path>` against the secret store the platform mounts.

    The store is a directory of files, one per path, readable by the worker and
    by nothing else. A reference that resolves to nothing raises here: a
    connection stored with an empty password fails on its next open, which is
    hours later and somewhere else.
    """
    if not reference.startswith("secret://"):
        raise ValueError(f"not a secret reference: {reference!r}")
    root = Path(os.environ.get(SECRET_ROOT_ENV, "/run/secrets"))
    path = root / reference[len("secret://"):]
    if not path.exists():
        raise FileNotFoundError(f"{reference} resolves to nothing at {path}")
    return path.read_text(encoding="utf-8").strip()


def trigger_dag_run(dag_id: str, logical_date: str, *, run_id: str | None = None,
                    conf: dict | None = None) -> dict:
    """Start one run of another DAG, and return what the server made.

    `logical_date` is a date or a timestamp, as a string. A run that already
    exists for it comes back as a 409 and the caller decides — this does not
    clear a run, because clearing one re-executes the code that run originally
    used and that is rarely what a replay wants.
    """
    body: dict[str, Any] = {"logical_date": logical_date, "conf": conf or {}}
    if run_id:
        body["dag_run_id"] = run_id
    return api("POST", f"dags/{dag_id}/dagRuns", body)


def wait_for_dag_run(dag_id: str, run_id: str, *, poke_seconds: int = 30,
                     timeout_seconds: int = 7200) -> str:
    """Poll a run until it finishes, and return its state.

    Raises when the timeout is reached rather than returning "still going". A
    caller that wanted to fire and forget did not need this function.
    """
    waited, state = 0, None
    while waited < timeout_seconds:
        state = api("GET", f"dags/{dag_id}/dagRuns/{run_id}").get("state")
        if state in FINISHED:
            return str(state)
        time.sleep(poke_seconds)
        waited += poke_seconds
    raise TimeoutError(
        f"{dag_id} run {run_id} was still {state!r} after {timeout_seconds}s"
    )
