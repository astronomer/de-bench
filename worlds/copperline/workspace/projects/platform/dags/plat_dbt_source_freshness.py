"""Source freshness over the fourteen landed sources, hourly.

Runs `dbt source freshness`, reads what it wrote, and records one row per
source in `ops.source_freshness`. It does not page: `config/alerts.yml` and
`gro_alerting_daily` own that, and they watch a shorter list than this one.

Hourly on the quarter hour, because half the feeds are hourly and the other
half arrive at their own times through the night. A source that has not
arrived is not a failure of this DAG; the record is the point.

Owned by data-platform.
"""

from __future__ import annotations

import json

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag

from include.lib import warehouse, workspace_root
from include.lib.notify import notify
from include.lib.pipeline import lake_task

DBT_ROOT = workspace_root() / "dbt" / "copperline_analytics"

#: dbt writes the freshness result here, whatever the exit code was.
SOURCES_JSON = DBT_ROOT / "target" / "sources.json"

#: Where the record goes. One row per source per run.
TABLE = "ops.source_freshness"

DEFAULT_ARGS = {
    "owner": "platform",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=2),
    "on_failure_callback": notify("platform", "source freshness did not record"),
}


@dag(
    dag_id="plat_dbt_source_freshness",
    schedule="15 * * * *",
    start_date=pendulum.datetime(2024, 2, 4, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["platform", "dbt", "quality"],
    doc_md=__doc__,
)
def plat_dbt_source_freshness():
    # A non-zero exit means a source is stale, which is a result and not a
    # failure of the run. `|| true` keeps the parse step downstream of it.
    measure = BashOperator(
        task_id="measure",
        bash_command=(
            "/opt/dbt-venv/bin/dbt source freshness "
            "--target prod --profiles-dir . || true"
        ),
        cwd=str(DBT_ROOT),
        pool="warehouse_read",
    )

    @lake_task
    def parse() -> list[dict]:
        """Read `target/sources.json` into one row per source.

        The states dbt uses are `pass`, `warn`, `error` and `runtime error`.
        The last one means the freshness query itself failed — a source that
        has no `loaded_at_field`, usually — and it is recorded rather than
        dropped, because a source nobody can measure is worth seeing.
        """
        if not SOURCES_JSON.exists():
            raise FileNotFoundError(
                f"no freshness result at {SOURCES_JSON}; the measure step wrote nothing"
            )
        document = json.loads(SOURCES_JSON.read_text(encoding="utf-8"))
        rows = []
        for result in document.get("results", []):
            # `source.<package>.<source_name>.<table>` is dbt's own id; the
            # last two parts are the name a person uses.
            node = str(result.get("unique_id", ""))
            rows.append({
                "source": ".".join(node.split(".")[-2:]),
                "state": result.get("status"),
                "max_loaded_at": result.get("max_loaded_at"),
                "age_seconds": result.get("max_loaded_at_time_ago_in_s"),
                "measured_at": document.get("metadata", {}).get("generated_at"),
            })
        return rows

    @lake_task
    def record(rows: list[dict]) -> int:
        """Append the run's rows to `ops.source_freshness`.

        This one appends on purpose. The table is a history of measurements and
        every run is a new measurement, so there is no partition to replace —
        the run id is what makes a row unique.
        """
        with warehouse.connect() as con:
            con.execute(f"CREATE SCHEMA IF NOT EXISTS {warehouse.qualify('ops')}")
            con.execute(
                f"""CREATE TABLE IF NOT EXISTS {warehouse.qualify(TABLE)} (
                    source VARCHAR, state VARCHAR, max_loaded_at VARCHAR,
                    age_seconds BIGINT, measured_at VARCHAR)"""
            )
            con.executemany(
                f"INSERT INTO {warehouse.qualify(TABLE)} VALUES (?, ?, ?, ?, ?)",
                [[r["source"], r["state"], r["max_loaded_at"], r["age_seconds"],
                  r["measured_at"]] for r in rows],
            )
        return len(rows)

    @lake_task
    def summarise(rows: list[dict]) -> dict[str, list[str]]:
        """Group the run by state, so the log line says which sources are late
        rather than how many."""
        summary: dict[str, list[str]] = {}
        for row in rows:
            summary.setdefault(str(row["state"]), []).append(str(row["source"]))
        return {state: sorted(sources) for state, sources in sorted(summary.items())}

    parsed = parse()
    measure >> parsed
    record(parsed)
    summarise(parsed)


plat_dbt_source_freshness()
