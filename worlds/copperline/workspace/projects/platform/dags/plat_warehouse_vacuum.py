"""The Saturday warehouse window: measure, checkpoint, measure, record.

DuckDB does not return space to the file system when rows are deleted. A
`CHECKPOINT` is what actually reclaims it, and this DAG is the only scheduled
thing that runs one. It takes the size before and after so the platform channel
gets a number rather than an impression.

It runs on Saturday at 08:00, when nothing else writes. It takes the single
writer for as long as the checkpoint takes, which has been under two minutes
every week since the file passed 30GB.

This DAG does the metadata half. `ops/runbooks/warehouse-cleanup.md` is the
half somebody does by hand, and it has an open item asking for the two to be
folded together.

Owned by data-platform.
"""

from __future__ import annotations

import pendulum
from airflow.sdk import dag

from include.lib import warehouse
from include.lib.notify import notify
from include.lib.pipeline import lake_task

#: One row a week: the size before, the size after, and what that reclaimed.
TABLE = "ops.vacuum_log"

DEFAULT_ARGS = {
    "owner": "platform",
    "retries": 1,
    "retry_delay": pendulum.duration(minutes=15),
    "on_failure_callback": notify("platform", "the weekly checkpoint did not run"),
}


@dag(
    dag_id="plat_warehouse_vacuum",
    schedule="0 8 * * 6",
    start_date=pendulum.datetime(2024, 2, 4, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["platform", "warehouse", "ops"],
    doc_md=__doc__,
)
def plat_warehouse_vacuum():
    @lake_task(pool="warehouse_write")
    def size_before() -> int:
        """The file's size on disk, in bytes, before anything is reclaimed."""
        return warehouse.warehouse_path().stat().st_size

    @lake_task(pool="warehouse_write")
    def checkpoint() -> str:
        """Flush the write-ahead log and reclaim what the deletes freed.

        `CHECKPOINT` is the whole of it. Nothing here deletes a row: the
        retention DAG owns what goes, and this one owns what the file does
        about it afterwards.
        """
        with warehouse.connect() as con:
            con.execute("CHECKPOINT")
        return "checkpointed"

    @lake_task(pool="warehouse_write")
    def size_after() -> int:
        """The file's size once the checkpoint has returned."""
        return warehouse.warehouse_path().stat().st_size

    @lake_task
    def record(before: int, after: int, target_ds: str) -> dict[str, int]:
        """One row a week. The trend is the useful part, not the week."""
        row = {"ds": target_ds, "bytes_before": before, "bytes_after": after,
               "bytes_reclaimed": before - after}
        warehouse.delete_insert(
            TABLE, "ds", target_ds, [row],
            columns=["ds", "bytes_before", "bytes_after", "bytes_reclaimed"],
        )
        return row

    before = size_before()
    done = checkpoint()
    after = size_after()
    before >> done >> after
    record(before, after, target_ds="{{ ds }}")


plat_warehouse_vacuum()
