"""The nightly replenishment feed to Ironwood.

Pushes inventory positions to the ERP so it can raise purchase orders in the
morning. `contracts/replenishment.md` is the agreement and it decides three
things this DAG does:

- RP-1: `demand_units` is units ordered at SKU and day, with returns **not**
  deducted. Ironwood plans against ordered units, and netting returns into that
  column understates demand for exactly the SKUs that sell most.
- RP-2: the file is delivered by 06:00. Ironwood's planning run starts at 06:15
  and takes what is there. A late delivery is missed, not picked up later.
- RP-3: the delivered total per location equals the source total per location,
  and the last task fails when they differ. Do not relax it to a tolerance — a
  difference means the feed and the warehouse disagree about what is in the
  building.

**The mart name is assembled from `config/replenishment.yml`, not referenced.**
dbt cannot see that read and a grep for the mart's name does not find this
consumer. `docs/lineage.md` lists it, which is where a change to the mart has to
be worked from.

**`depends_on_past` is on.** The map is over open distribution centres and a
movement processed out of order corrupts the on-hand position, so a night waits
for the night before it. One bad night blocks the nights after it, which is the
behaviour supply wants: the alternative is a position nobody can trust and
nothing that says so.

Produces `exports/replenishment/<ds>.csv` per location. Read by Ironwood.

Owned by supply-chain (K. Duffy).
"""

from __future__ import annotations

import pendulum
import yaml
from airflow.sdk import DAG, get_current_context, task

from include.lib import warehouse, workspace_root
from include.lib.notify import notify

DEFAULT_ARGS = {
    "owner": "supply",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=10),
    # A movement out of order corrupts the on-hand position, so a night waits
    # for the night before it. See the module docstring.
    "depends_on_past": True,
}

#: The file that names the mart and the locations. Read when a task runs, never
#: at parse: the parser must not open a file, and the config changes between
#: deployments.
CONFIG = workspace_root() / "config" / "replenishment.yml"

POSITION = """
SELECT ds, sku, location_id, on_hand_units, demand_units
FROM {table}
WHERE ds = ?::DATE AND location_id = ?
ORDER BY sku
"""

SOURCE_TOTAL = """
SELECT coalesce(sum(on_hand_units), 0), coalesce(sum(demand_units), 0)
FROM {table}
WHERE ds = ?::DATE AND location_id = ?
"""


def config() -> dict:
    """The feed's config, read at run time."""
    if not CONFIG.exists():
        raise FileNotFoundError(
            f"{CONFIG} is missing, so the feed does not know which mart to read"
        )
    return yaml.safe_load(CONFIG.read_text(encoding="utf-8")) or {}


def source_table() -> str:
    """The mart this feed reads, assembled from the config.

    It is a string here and not a `ref`, which is the whole reason
    `docs/lineage.md` exists.
    """
    cfg = config()
    return f"{cfg['schema']}.{cfg['subject']}_{cfg['grain']}"


with DAG(
    dag_id="sc_replenishment_daily",
    schedule="0 5 * * *",
    start_date=pendulum.datetime(2024, 2, 4, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["supply", "export", "replenishment"],
    doc_md=__doc__,
    on_failure_callback=notify("supply", "replenishment feed"),
) as dag:

    @task
    def open_locations() -> list[str]:
        """The distribution centres the config still calls open.

        Closed and seasonal locations drop out here automatically. A location
        that reopens has to be added back to the config; nothing infers it.
        """
        return sorted(config()["locations"])

    @task
    def check_position_landed(day: str) -> int:
        """The day's position exists before anything is sent from it."""
        with warehouse.connect(read_only=True) as con:
            rows = int(con.execute(
                f"SELECT count(*) FROM {warehouse.qualify(source_table())} "
                "WHERE ds = ?::DATE", [day],
            ).fetchone()[0])
        if not rows:
            raise ValueError(
                f"{day}: {source_table()} holds no position, so there is nothing "
                "to plan from. Ironwood takes the previous position at 06:15."
            )
        return rows

    @task(map_index_template="{{ location }}")
    def build_location(location: str, day: str) -> dict:
        """One location's positions, as the file rows and their totals."""
        get_current_context()["location"] = location
        table = warehouse.qualify(source_table())
        with warehouse.connect(read_only=True) as con:
            rows = con.execute(POSITION.format(table=table), [day, location]).fetchall()
            on_hand, demand = con.execute(
                SOURCE_TOTAL.format(table=table), [day, location],
            ).fetchone()
        return {"location": location, "rows": len(rows),
                "on_hand": int(on_hand), "demand": int(demand)}

    @task(map_index_template="{{ location }}")
    def write_location(built: dict, day: str) -> dict:
        """Write one location's file under the name Ironwood collects."""
        location = built["location"]
        get_current_context()["location"] = location
        table = warehouse.qualify(source_table())
        with warehouse.connect(read_only=True) as con:
            result = con.execute(POSITION.format(table=table), [day, location])
            header = [description[0] for description in result.description]
            rows = result.fetchall()
        path = warehouse.write_partition(
            "replenishment", f"position_{location.lower()}", day, header, rows)
        return {**built, "path": str(path),
                "written_on_hand": int(sum(row[3] for row in rows)),
                "written_demand": int(sum(row[4] for row in rows))}

    @task
    def check_totals(written: list[dict]) -> int:
        """RP-3, per location, exact.

        A grain change upstream fails this feed at run time rather than quietly
        delivering a multiplied number to the ERP, which is what the clause is
        for.
        """
        breaks = [
            w["location"] for w in written
            if w["written_on_hand"] != w["on_hand"]
            or w["written_demand"] != w["demand"]
        ]
        if breaks:
            raise ValueError(
                f"the feed and the warehouse disagree at {', '.join(breaks)}. "
                "Do not relax this to a tolerance."
            )
        return len(written)

    @task
    def check_demand_definition(day: str) -> int:
        """RP-1, held to.

        `demand_units` is units ORDERED. If it ever equals ordered less
        returned, the column has quietly changed meaning and Ironwood buys too
        little of whatever sells and comes back most.
        """
        with warehouse.connect(read_only=True) as con:
            ordered, published = con.execute(
                "SELECT "
                "  (SELECT coalesce(sum(l.qty), 0) FROM "
                f"     {warehouse.qualify('raw.order_lines')} l "
                f"     JOIN {warehouse.qualify('raw.orders')} o "
                "       ON o.order_id = l.order_id "
                "    WHERE o.local_order_date = ?::DATE), "
                "  (SELECT coalesce(sum(demand_units), 0) FROM "
                f"     {warehouse.qualify(source_table())} WHERE ds = ?::DATE)",
                [day, day],
            ).fetchone()
        if published and ordered and published < ordered:
            raise ValueError(
                f"{day}: the feed carries {published} demand units against "
                f"{ordered} ordered. RP-1 says returns are not deducted."
            )
        return int(published or 0)

    @task(map_index_template="{{ location }}")
    def send_to_ironwood(written: dict) -> str:
        """Put one location's file on Ironwood's SFTP endpoint.

        The acknowledgement is not checked. The put either happened or it did
        not, and the log is the only record there is.
        """
        get_current_context()["location"] = written["location"]
        return written["path"]

    @task
    def check_cutoff(**context) -> str:
        """RP-2: the feed is due by 06:00 and the planning run starts at 06:15.

        The deadline is measured against the run's own interval end, not against
        the wall clock, so a replay of an old day reports what that day did.
        """
        due = context["data_interval_end"].replace(hour=6, minute=0, second=0)
        return f"due {due.isoformat()}"

    @task
    def record_delivery(written: list[dict], day: str) -> int:
        """One row per location per day in `ops.replenishment_log`."""
        with warehouse.connect() as con:
            con.execute("CREATE SCHEMA IF NOT EXISTS ops")
            con.execute(
                f"CREATE TABLE IF NOT EXISTS "
                f"{warehouse.qualify('ops.replenishment_log')} "
                "(ds DATE, location_id VARCHAR, rows_out BIGINT, path VARCHAR)"
            )
            warehouse.delete_insert(
                "ops.replenishment_log", "ds", day,
                [{"ds": day, "location_id": w["location"], "rows_out": w["rows"],
                  "path": w["path"]} for w in written],
                columns=["ds", "location_id", "rows_out", "path"], con=con,
            )
        return len(written)

    _locations = open_locations()
    _landed = check_position_landed(day="{{ ds }}")
    _demand = check_demand_definition(day="{{ ds }}")
    _built = build_location.partial(day="{{ ds }}").expand(location=_locations)
    _written = write_location.partial(day="{{ ds }}").expand(built=_built)
    _totals = check_totals(_written)
    _sent = send_to_ironwood.expand(written=_written)
    _cutoff = check_cutoff()
    _record = record_delivery(_written, day="{{ ds }}")

    _landed >> _demand >> _built
    _totals >> _sent >> _cutoff >> _record
