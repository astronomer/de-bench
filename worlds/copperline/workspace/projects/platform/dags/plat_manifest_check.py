"""Does the committed manifest still describe the models on disk?

The 04:00 build renders from `dbt/copperline_analytics/manifest/manifest.json`
rather than from the project, which is what keeps the parse fast. The cost is
that the graph is whatever the manifest last held: a model added since is not
built, and a model removed since is built from a stale definition.

This compares the two at 03:45, fifteen minutes before the build, and **warns**.
It does not fail. Regenerating the manifest is a decision — it changes what the
next build runs — and it is not a decision to take at quarter to four in the
morning without anybody looking. `docs/change-management.md` says who takes it.

Owned by data-platform.
"""

from __future__ import annotations

import json

import pendulum
from airflow.sdk import dag

from include.lib import workspace_root
from include.lib.notify import notify
from include.lib.pipeline import lake_task

DBT_ROOT = workspace_root() / "dbt" / "copperline_analytics"
MANIFEST = DBT_ROOT / "manifest" / "manifest.json"
MODELS = DBT_ROOT / "models"

DEFAULT_ARGS = {
    "owner": "platform",
    "retries": 1,
    "retry_delay": pendulum.duration(minutes=2),
    "on_failure_callback": notify("platform", "the manifest check did not run"),
}


@dag(
    dag_id="plat_manifest_check",
    schedule="45 3 * * *",
    start_date=pendulum.datetime(2024, 2, 4, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["platform", "dbt", "ops"],
    doc_md=__doc__,
)
def plat_manifest_check():
    @lake_task
    def compare() -> dict[str, list[str]]:
        """Model names in the manifest against `.sql` files under `models/`.

        By name, not by path: a model that moved between folders is the same
        model and dbt treats it as one, so a move is not drift.
        """
        manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
        in_manifest = {
            node["name"]
            for node in (manifest.get("nodes") or {}).values()
            if node.get("resource_type") == "model"
        }
        on_disk = {path.stem for path in MODELS.rglob("*.sql")}
        return {
            "missing_from_manifest": sorted(on_disk - in_manifest),
            "missing_from_disk": sorted(in_manifest - on_disk),
        }

    @lake_task
    def warn(drift: dict[str, list[str]]) -> str:
        """Say what has drifted, and succeed either way.

        A model on disk and not in the manifest is not built tonight. A model
        in the manifest and not on disk is built from a definition nobody can
        read. Both are worth a line in the log and neither is worth stopping
        the build over.
        """
        new = drift["missing_from_manifest"]
        gone = drift["missing_from_disk"]
        if not new and not gone:
            return "manifest matches the models on disk"
        parts = []
        if new:
            parts.append(f"{len(new)} model(s) on disk are not in the manifest "
                         f"and will not be built: {', '.join(new)}")
        if gone:
            parts.append(f"{len(gone)} model(s) in the manifest are not on disk: "
                         f"{', '.join(gone)}")
        return "; ".join(parts)

    warn(compare())


plat_manifest_check()
