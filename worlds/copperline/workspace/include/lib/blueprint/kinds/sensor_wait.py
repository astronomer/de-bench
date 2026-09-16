"""`sensor_wait` — wait for a file or an asset before the steps that follow."""

from __future__ import annotations

from typing import Any

from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.providers.standard.sensors.python import PythonSensor

from ... import warehouse, workspace_root
from ..registry import BlueprintError, register

__all__ = ["sensor_wait", "table_has_partition"]


def sensor_wait(step: Any, ctx: Any) -> Any:
    """Build the wait.

    Keys:
        file          a path or glob under the repository root, templated.
                      One of `file` or `asset`.
        asset         a warehouse asset, as a URI or as `schema.table`:
                      `duckdb://warehouse/marts.daily_revenue`. One of `file`
                      or `asset`.
        partition_col with `asset`, the column that scopes the wait. `ds` by
                      default. Omit it to wait for the table to exist at all.
        partition_value  the partition being waited for. `{{ ds }}` by default.
        poke_interval seconds between checks. 60 by default, which is a
                      default and not a decision — match it to how often the
                      thing you wait for actually changes.
        timeout       seconds before the wait gives up.
        mode          "reschedule" by default, so a long wait releases its
                      worker slot. "poke" for a wait under five minutes.

    THERE IS NO HOUSE TIMEOUT DEFAULT. A step that names no `timeout` gets
    Airflow's own default of seven days, which is not a timeout, it is a leak:
    the run sits in `running` and nothing is alerted. Three teams have been
    bitten by it and nobody has agreed what the default should be, so set one
    on every step until somebody does.
    """
    if bool(step.get("file")) == bool(step.get("asset")):
        raise BlueprintError(
            f"step {step.name!r}: give exactly one of `file` or `asset`"
        )
    common: dict[str, Any] = {
        "task_id": step.name,
        "poke_interval": step.get("poke_interval", 60),
        "mode": step.get("mode", "reschedule"),
    }
    if "timeout" in step:
        common["timeout"] = step["timeout"]

    if step.get("file"):
        return FileSensor(filepath=str(workspace_root() / ctx.render(step["file"])),
                          **common)
    return PythonSensor(
        python_callable=table_has_partition,
        op_kwargs={
            "table": _table(ctx.render(step["asset"])),
            "partition_col": step.get("partition_col", "ds"),
            "partition_value": ctx.render(step.get("partition_value", "{{ ds }}")),
        },
        **common,
    )


def table_has_partition(*, table: str, partition_col: str | None,
                        partition_value: str) -> bool:
    """Whether the table holds rows for the partition being waited for.

    Reads the snapshot, so the wait never queues behind the run it is waiting
    for. That means the wait clears one snapshot refresh after the rows land,
    not the instant they do.
    """
    schema, _, bare = table.rpartition(".")
    with warehouse.connect(read_only=True) as con:
        exists = con.execute(
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_schema = ? AND table_name = ?",
            [schema, bare],
        ).fetchone()
        if not exists:
            return False
        if not partition_col:
            return True
        found = con.execute(
            f"SELECT 1 FROM {warehouse.qualify(table)} WHERE {partition_col} = ? LIMIT 1",
            [partition_value],
        ).fetchone()
    return bool(found)


def _table(asset: str) -> str:
    """`schema.table` from an asset URI or from a bare name."""
    return asset.rsplit("/", 1)[-1] if "://" in asset else asset


register("sensor_wait", sensor_wait)
