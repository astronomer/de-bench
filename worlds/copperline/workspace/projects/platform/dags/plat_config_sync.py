"""Apply `config/iac/` to the deployment: variables, connections and pools.

Runs at 04:00 every morning. The three files under `config/iac/` are the
declared set, and this DAG makes the deployment match them.

**Replace, not merge.** Each apply step PUTs the full declared set and removes
anything the deployment holds that the files do not declare. That is what makes
the files the truth rather than a suggestion, and it is why a variable added by
hand in the UI is gone by the next morning. Make the change in the files.

Nothing here is destructive to data. It is destructive to state, which is worse
in one way — a removed connection takes down the tasks that used it, and the
tasks fail on the next open rather than at sync time. `config/iac/README.md`
is the place to start; the removals this DAG made are in `ops.config_sync_log`.

Owned by data-platform.
"""

from __future__ import annotations

import pendulum
from airflow.sdk import dag

from include.lib import warehouse, workspace_root
from include.lib.notify import notify
from include.lib.pipeline import lake_task
from projects.platform.lib.deployment import api, secret

IAC = workspace_root() / "config" / "iac"

#: What this DAG did, one row per key it added, changed or removed.
LOG_TABLE = "ops.config_sync_log"

DEFAULT_ARGS = {
    "owner": "platform",
    "retries": 1,
    "retry_delay": pendulum.duration(minutes=10),
    "on_failure_callback": notify(
        "platform", "the deployment no longer matches config/iac/"
    ),
}


@dag(
    dag_id="plat_config_sync",
    schedule="0 4 * * *",
    start_date=pendulum.datetime(2024, 2, 4, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["platform", "config", "deployment"],
    doc_md=__doc__,
)
def plat_config_sync():
    @lake_task
    def read_declared() -> dict[str, dict]:
        """Read the three files. A missing file is a failure, not an empty
        declaration: an empty declaration would remove everything."""
        import yaml

        declared = {}
        for kind, filename, key in (
            ("variables", "variables.yaml", "variables"),
            ("connections", "connections.yaml", "connections"),
            ("pools", "pools.yaml", "pools"),
        ):
            path = IAC / filename
            if not path.exists():
                raise FileNotFoundError(f"config/iac/{filename} is missing")
            document = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
            if key not in document:
                raise ValueError(f"config/iac/{filename} declares no {key!r} block")
            declared[kind] = document[key] or {}
        return declared

    @lake_task
    def apply_variables(declared: dict[str, dict]) -> dict[str, list[str]]:
        """Apply the declared variables. The files are the whole truth:
        anything not declared here is removed on this pass."""
        from airflow.sdk import Variable

        desired = {str(k): str(v) for k, v in declared["variables"].items()}
        live = _live_variable_keys()
        for key, value in desired.items():
            Variable.set(key, value)
        removed = sorted(set(live) - set(desired))
        for key in removed:
            Variable.delete(key)
        return {"applied": sorted(desired), "removed": removed}

    @lake_task
    def apply_connections(declared: dict[str, dict]) -> dict[str, list[str]]:
        """Apply the declared connections, and remove the rest.

        A password written `secret://<path>` is resolved against the secret
        store as the connection is set. A literal secret in the file is
        rejected here rather than stored, because a secret that reaches the
        metadata database has to be rotated rather than deleted.
        """
        desired = declared["connections"]
        for conn_id, spec in desired.items():
            password = str(spec.get("password") or "")
            if password and not password.startswith("secret://"):
                raise ValueError(
                    f"{conn_id}: a literal password in config/iac/connections.yaml"
                )
        live = _live_connection_ids()
        _put_connections(desired)
        removed = sorted(set(live) - set(desired))
        _remove_connections(removed)
        return {"applied": sorted(desired), "removed": removed}

    @lake_task
    def apply_pools(declared: dict[str, dict]) -> dict[str, list[str]]:
        """Apply the declared pools, and remove the rest.

        `default_pool` is never removed. Airflow owns it and a deployment
        without it cannot schedule anything.
        """
        desired = declared["pools"]
        live = [name for name in _live_pool_names() if name != "default_pool"]
        _put_pools(desired)
        removed = sorted(set(live) - set(desired))
        _remove_pools(removed)
        return {"applied": sorted(desired), "removed": removed}

    @lake_task
    def verify(results: list[dict[str, list[str]]]) -> dict[str, int]:
        """Every declared key is live and no undeclared key survived."""
        applied = sum(len(result["applied"]) for result in results)
        removed = sum(len(result["removed"]) for result in results)
        if not applied:
            raise RuntimeError("the sync applied nothing, which means it read nothing")
        return {"applied": applied, "removed": removed}

    @lake_task
    def record(results: list[dict[str, list[str]]], target_ds: str) -> int:
        """Write what this pass did to `ops.config_sync_log`.

        The removals are the half worth keeping. A connection that vanished
        three weeks ago is the first thing to look for when a task that has run
        for a year starts failing to open one.
        """
        kinds = ("variables", "connections", "pools")
        rows = [
            {"ds": target_ds, "kind": kind, "action": action, "name": name}
            for kind, result in zip(kinds, results)
            for action in ("applied", "removed")
            for name in result[action]
        ]
        return warehouse.delete_insert(
            LOG_TABLE, "ds", target_ds, rows,
            columns=["ds", "kind", "action", "name"],
        )

    declared = read_declared()
    results = [apply_variables(declared), apply_connections(declared),
               apply_pools(declared)]
    verify(results)
    record(results, target_ds="{{ ds }}")


# --- the deployment's own state --------------------------------------------
#
# Connections and pools are deployment state and there is no task-side write
# for them, so this goes through the API server the same way a person with a
# terminal would. `projects/platform/lib/deployment.py` is the wrapper.


def _live_variable_keys() -> list[str]:
    """Every variable key the deployment holds now."""
    return sorted(item["key"] for item in api("GET", "variables").get("variables", []))


def _live_connection_ids() -> list[str]:
    return sorted(item["connection_id"]
                  for item in api("GET", "connections").get("connections", []))


def _put_connections(desired: dict[str, dict]) -> None:
    """Create or replace each declared connection, secret references resolved."""
    for conn_id, spec in desired.items():
        body = {k: v for k, v in spec.items() if k != "password"}
        body["connection_id"] = conn_id
        if spec.get("password"):
            body["password"] = secret(str(spec["password"]))
        api("PUT", f"connections/{conn_id}", body)


def _remove_connections(conn_ids: list[str]) -> None:
    for conn_id in conn_ids:
        api("DELETE", f"connections/{conn_id}")


def _live_pool_names() -> list[str]:
    return sorted(item["name"] for item in api("GET", "pools").get("pools", []))


def _put_pools(desired: dict[str, dict]) -> None:
    for name, spec in desired.items():
        api("PUT", f"pools/{name}", {
            "name": name,
            "slots": int(spec["slots"]),
            "description": spec.get("description", ""),
        })


def _remove_pools(names: list[str]) -> None:
    for name in names:
        api("DELETE", f"pools/{name}")


plat_config_sync()
