"""This team's blueprint kinds.

`include/lib/blueprint/` ships five kinds and is shared code. A kind only one
team uses is registered from here, which is the documented entry point — the
loader imports this file when it renders a `*.dag.yaml` in this folder. See
`docs/blueprints.md`.

One kind so far.
"""

from __future__ import annotations

from typing import Any

from airflow.providers.standard.operators.python import PythonOperator

from include.lib import warehouse
from include.lib.blueprint import registry

__all__ = ["table_profile", "run_profile"]


def table_profile(step: Any, ctx: Any) -> Any:
    """Count a list of tables and write one row each into a profile table.

    Keys:
        tables        the tables to count, `schema.table`, as a list. Required.
        target        where the profile lands. `ops.profile_daily` by default.
        partition_col the target's partition column. `ds` by default.
        partition_value  the partition this run owns. `{{ ds }}` by default.

    ONE STEP COUNTS THE WHOLE LIST, and that is not tidiness. The write is a
    `delete_insert` on the target's partition, which replaces the day rather
    than adding to it, so two steps writing the same day would each delete what
    the other had just put there. A list is what the write can be made safe
    for.

    The read is read-only, so a profile never queues behind the build it is
    profiling. It reads the snapshot, which is at most one run behind.
    """
    return PythonOperator(
        task_id=step.name,
        python_callable=run_profile,
        op_kwargs={
            "tables": [ctx.render(table) for table in step["tables"]],
            "target": ctx.render(step.get("target", "ops.profile_daily")),
            "partition_col": step.get("partition_col", "ds"),
            "partition_value": ctx.render(step.get("partition_value", "{{ ds }}")),
        },
    )


def run_profile(*, tables: list[str], target: str, partition_col: str,
                partition_value: str) -> int:
    """Count each table and replace the target's partition. Returns the rows
    written, which is one per table asked for.

    A table that does not exist is counted as NULL rather than as zero. Zero is
    a real answer and means the table is there and empty; a missing table is a
    different problem and the profile should not hide it as a very quiet day.
    """
    rows = []
    with warehouse.connect(read_only=True) as con:
        for table in tables:
            schema, _, bare = table.rpartition(".")
            exists = con.execute(
                "SELECT 1 FROM information_schema.tables "
                "WHERE table_schema = ? AND table_name = ?",
                [schema, bare],
            ).fetchone()
            count = con.execute(
                f"SELECT count(*) FROM {warehouse.qualify(table)}"
            ).fetchone()[0] if exists else None
            rows.append({partition_col: partition_value, "table_name": table,
                         "row_count": count})
    return warehouse.delete_insert(target, partition_col, partition_value, rows)


registry.register("table_profile", table_profile)
