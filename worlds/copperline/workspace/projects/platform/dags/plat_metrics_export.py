"""Model run times and row counts, from dbt's own run results, into `ops/metrics/`.

dbt writes `target/run_results.json` on every invocation: one entry per node,
with the status, the wall time and the adapter's row count. This reads the
night's file, keeps the numbers worth trending, and writes one CSV a day.

It is what somebody opens when the build starts taking twenty minutes longer
than it did and nobody can say which model. The slow ones have been the same
three for a year, which is itself the useful fact.

Owned by data-platform.
"""

from __future__ import annotations

import json

import pendulum
from airflow.sdk import dag

from include.lib import warehouse, workspace_root
from include.lib.notify import notify
from include.lib.pipeline import lake_task

RUN_RESULTS = workspace_root() / "dbt" / "copperline_analytics" / "target" / "run_results.json"

#: Where the daily file lands, and the table behind it.
LAYER = "ops"
NAME = "model_metrics"
TABLE = "ops.model_metrics"

#: The day the build owned, which is the day before the run that reads it.
TARGET_DS = "{{ macros.ds_add(ds, -1) }}"

DEFAULT_ARGS = {
    "owner": "platform",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
    "on_failure_callback": notify("platform", "no build metrics for last night"),
}


@dag(
    dag_id="plat_metrics_export",
    schedule="0 10 * * *",
    start_date=pendulum.datetime(2024, 2, 4, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["platform", "dbt", "cost"],
    doc_md=__doc__,
)
def plat_metrics_export():
    @lake_task
    def read_run_results() -> dict:
        """Read last night's `run_results.json`.

        It is the file the build wrote, so this DAG's answer is only as recent
        as the last build. A morning with no build has no metrics, and that is
        the honest answer rather than a repeat of yesterday's.
        """
        if not RUN_RESULTS.exists():
            raise FileNotFoundError(f"no run results at {RUN_RESULTS}")
        return json.loads(RUN_RESULTS.read_text(encoding="utf-8"))

    @lake_task
    def parse(results: dict, target_ds: str) -> list[dict]:
        """One row per node: status, seconds, rows.

        `execution_time` is dbt's own wall time for the node, in seconds, and
        it includes the time the node spent waiting for the warehouse's single
        writer. That is a feature here: the queue is the cost.
        """
        rows = []
        for result in results.get("results", []):
            unique_id = result.get("unique_id", "")
            adapter = result.get("adapter_response") or {}
            rows.append({
                "ds": target_ds,
                "node": unique_id,
                "name": unique_id.rsplit(".", 1)[-1],
                "status": result.get("status"),
                "seconds": float(result.get("execution_time") or 0.0),
                "rows_affected": adapter.get("rows_affected"),
            })
        return sorted(rows, key=lambda row: -row["seconds"])

    @lake_task
    def record(rows: list[dict], target_ds: str) -> int:
        """Replace the day's partition of `ops.model_metrics`."""
        return warehouse.delete_insert(
            TABLE, "ds", target_ds, rows,
            columns=["ds", "node", "name", "status", "seconds", "rows_affected"],
        )

    @lake_task
    def publish(rows: list[dict], target_ds: str) -> str:
        """Write the day's file to `include/data/ops/model_metrics_<ds>.csv`."""
        header = ["ds", "node", "name", "status", "seconds", "rows_affected"]
        path = warehouse.write_partition(
            LAYER, NAME, target_ds, header,
            [[row[column] for column in header] for row in rows],
        )
        return str(path)

    parsed = parse(read_run_results(), target_ds=TARGET_DS)
    record(parsed, target_ds=TARGET_DS)
    publish(parsed, target_ds=TARGET_DS)


plat_metrics_export()
