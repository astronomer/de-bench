"""Regenerate `ops/lineage.json` from the dbt manifest, every morning.

One node per model, one edge per `ref`. The ops dashboard draws it and the
review tooling reads it.

**It holds the `ref` rows and nothing else.** Six of the twelve consumers in
`docs/report-registry.md` are dbt references and appear here; the other six
read a mart through a config string, a filename pattern, a contract list,
literal SQL in a dashboard file, or a script in another team's project, and
none of those is a reference. `docs/lineage.md` is the list that has all
twelve, it is maintained by hand, and it is the one to work before a mart
changes shape. This file is useful and it is not that list.

Owned by data-platform.
"""

from __future__ import annotations

import json

import pendulum
from airflow.sdk import dag

from include.lib import workspace_root
from include.lib.notify import notify
from include.lib.pipeline import lake_task

MANIFEST = workspace_root() / "dbt" / "copperline_analytics" / "manifest" / "manifest.json"
OUTPUT = workspace_root() / "ops" / "lineage.json"

DEFAULT_ARGS = {
    "owner": "platform",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
    "on_failure_callback": notify("platform", "ops/lineage.json is stale"),
}


@dag(
    dag_id="plat_lineage_publish",
    schedule="0 9 * * *",
    start_date=pendulum.datetime(2024, 2, 4, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["platform", "lineage", "ops"],
    doc_md=__doc__,
)
def plat_lineage_publish():
    @lake_task
    def read_manifest() -> dict:
        """Load the committed manifest. It is a file, so this is cheap, and it
        is the same file the build renders from."""
        if not MANIFEST.exists():
            raise FileNotFoundError(f"no manifest at {MANIFEST}")
        return json.loads(MANIFEST.read_text(encoding="utf-8"))

    @lake_task
    def build_graph(manifest: dict) -> dict:
        """Nodes and edges, from the manifest's own `depends_on` blocks.

        A node carries its schema, its materialisation and the team folder it
        lives under, which is what makes the drawing readable at 146 models.
        """
        nodes, edges = [], []
        for unique_id, node in (manifest.get("nodes") or {}).items():
            if node.get("resource_type") != "model":
                continue
            path = node.get("path", "")
            nodes.append({
                "id": unique_id,
                "name": node.get("name"),
                "schema": node.get("schema"),
                "materialized": (node.get("config") or {}).get("materialized"),
                "team": path.split("/")[0] if "/" in path else "shared",
            })
            for parent in (node.get("depends_on") or {}).get("nodes", []):
                edges.append({"from": parent, "to": unique_id})
        return {
            "generated_from": str(MANIFEST.relative_to(workspace_root())),
            "manifest_generated_at": (manifest.get("metadata") or {}).get("generated_at"),
            "nodes": sorted(nodes, key=lambda n: n["id"]),
            "edges": sorted(edges, key=lambda e: (e["from"], e["to"])),
        }

    @lake_task
    def write(graph: dict) -> str:
        """Write `ops/lineage.json`, whole, in one go.

        The write is atomic, because the dashboard polls this file and a half
        written graph draws as a warehouse with no edges in it.
        """
        OUTPUT.parent.mkdir(parents=True, exist_ok=True)
        partial = OUTPUT.with_suffix(".json.partial")
        partial.write_text(json.dumps(graph, indent=2, sort_keys=True), encoding="utf-8")
        partial.replace(OUTPUT)
        return str(OUTPUT)

    write(build_graph(read_manifest()))


plat_lineage_publish()
