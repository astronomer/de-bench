"""Write `ops/report_registry.yml` from `config/reports.yml`.

The ops tooling wants the report list as machine-readable YAML with a checksum
on it, so this renders one from the config the finance publisher runs off.

**It is not `docs/report-registry.md`.** That document is the company's list —
every published output, its owner and its refresh commitment — and it is
maintained by hand because most of what is in it has no config file behind it.
This covers the four rendered finance reports and nothing else. The two do not
match and the registry document has an open item saying so.

Owned by data-platform.
"""

from __future__ import annotations

import hashlib

import pendulum
from airflow.sdk import dag

from include.lib import workspace_root
from include.lib.notify import notify
from include.lib.pipeline import lake_task

SOURCE = workspace_root() / "config" / "reports.yml"
OUTPUT = workspace_root() / "ops" / "report_registry.yml"

DEFAULT_ARGS = {
    "owner": "platform",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
    "on_failure_callback": notify("platform", "ops/report_registry.yml is stale"),
}


@dag(
    dag_id="plat_report_registry_sync",
    schedule="0 11 * * *",
    start_date=pendulum.datetime(2024, 2, 4, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["platform", "reports", "ops"],
    doc_md=__doc__,
)
def plat_report_registry_sync():
    @lake_task
    def read_config() -> dict:
        """Read `config/reports.yml`. A committed file, so this is a file read
        and nothing more."""
        import yaml

        if not SOURCE.exists():
            raise FileNotFoundError(f"no report config at {SOURCE}")
        document = yaml.safe_load(SOURCE.read_text(encoding="utf-8")) or {}
        if not document.get("reports"):
            raise ValueError(f"{SOURCE} declares no reports")
        return document

    @lake_task
    def render(document: dict) -> dict:
        """One entry per report: what it reads, where it lands, who owns it.

        The checksum is over the source file rather than over this rendering,
        so a tool can tell whether the config moved without diffing the output.
        """
        digest = hashlib.sha256(SOURCE.read_bytes()).hexdigest()
        entries = {
            name: {
                "title": report.get("title"),
                "reads": report.get("reads"),
                "grain": list(report.get("grain") or []),
                "lands_at": report.get("lands_at"),
                "owner": report.get("owner"),
                "publisher": "fin_report_publish",
            }
            for name, report in sorted(document["reports"].items())
        }
        return {
            "source": str(SOURCE.relative_to(workspace_root())),
            "source_sha256": digest,
            "covers": "the rendered finance reports only; "
                      "docs/report-registry.md is the company list",
            "reports": entries,
        }

    @lake_task
    def write(registry: dict) -> str:
        """Write the file, atomically, so a reader never sees half a list."""
        import yaml

        OUTPUT.parent.mkdir(parents=True, exist_ok=True)
        partial = OUTPUT.with_suffix(".yml.partial")
        partial.write_text(
            yaml.safe_dump(registry, sort_keys=True, default_flow_style=False),
            encoding="utf-8",
        )
        partial.replace(OUTPUT)
        return str(OUTPUT)

    write(render(read_config()))


plat_report_registry_sync()
