"""Hourly staleness check on the three source connections that go quiet.

Three feeds have stopped arriving without failing, at least once each: the
Meridian settlements, the carrier scans and the clickstream. In every case a
task succeeded, a table was written, and the rows were the same rows as the
hour before. Nothing raised.

This is the cheap check that would have caught them: the newest row's timestamp
against the age the feed is allowed to reach. It records every hour and raises
only when a feed is past its limit, so the record is a history of how close to
the edge each feed usually runs.

It does not page. `config/alerts.yml` and `gro_alerting_daily` own paging, they
watch a longer list, and they run once a day. This runs every hour and is what
somebody looks at when the question is "since when".

Owned by data-platform.
"""

from __future__ import annotations

import pendulum
from airflow.sdk import dag

from include.lib import warehouse
from include.lib.notify import notify
from include.lib.pipeline import lake_task

#: The feed, the table behind it, the column that carries our load time, and
#: how old the newest row may be before somebody has to look. The hours are
#: each feed's own cadence plus a margin, not one number applied to three
#: different things.
FEEDS = {
    "meridian": {"table": "raw.pay_meridian_settlements", "column": "loaded_at",
                 "max_age_hours": 26},
    "carrier_scans": {"table": "raw.carrier_scans", "column": "loaded_at",
                      "max_age_hours": 6},
    "clickstream": {"table": "raw.web_events", "column": "loaded_at",
                    "max_age_hours": 3},
}

#: One row per feed per run.
TABLE = "ops.connection_health"

DEFAULT_ARGS = {
    "owner": "platform",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
    "on_failure_callback": notify("platform", "a source feed has gone quiet"),
}


@dag(
    dag_id="plat_conn_healthcheck",
    schedule="0 * * * *",
    start_date=pendulum.datetime(2024, 2, 4, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["platform", "sources", "ops"],
    doc_md=__doc__,
)
def plat_conn_healthcheck():
    @lake_task(map_index_template="{{ task.op_kwargs['feed'] }}")
    def measure(feed: str, data_interval_end) -> dict:
        """The newest row in one feed's table, and how old it is.

        Age is measured against the run's own interval end, not against the
        clock: a run replayed tomorrow then reports the age the feed had at
        the hour it is replaying, which is the only answer that means
        anything afterwards.
        """
        spec = FEEDS[feed]
        with warehouse.connect(read_only=True) as con:
            newest = con.execute(
                f"SELECT max({spec['column']}) FROM {warehouse.qualify(spec['table'])}"
            ).fetchone()[0]
        if newest is None:
            return {"feed": feed, "newest": None, "age_hours": None, "stale": True}
        age = (data_interval_end.replace(tzinfo=None) - newest).total_seconds() / 3600
        return {
            "feed": feed,
            "newest": newest.isoformat(),
            "age_hours": round(age, 2),
            "stale": age > spec["max_age_hours"],
        }

    @lake_task
    def record(measurements: list[dict], run_id: str) -> int:
        """Append the hour's measurements. A history, so it appends."""
        with warehouse.connect() as con:
            con.execute(f"CREATE SCHEMA IF NOT EXISTS {warehouse.qualify('ops')}")
            con.execute(
                f"""CREATE TABLE IF NOT EXISTS {warehouse.qualify(TABLE)} (
                    run_id VARCHAR, feed VARCHAR, newest VARCHAR,
                    age_hours DOUBLE, stale BOOLEAN)"""
            )
            con.executemany(
                f"INSERT INTO {warehouse.qualify(TABLE)} VALUES (?, ?, ?, ?, ?)",
                [[run_id, m["feed"], m["newest"], m["age_hours"], m["stale"]]
                 for m in measurements],
            )
        return len(measurements)

    @lake_task
    def report_stale(measurements: list[dict]) -> list[str]:
        """Fail when a feed is past its limit, and name every one that is.

        After the record, so the evidence is written whichever way this goes.
        """
        stale = sorted(m["feed"] for m in measurements if m["stale"])
        if stale:
            ages = {m["feed"]: m["age_hours"] for m in measurements if m["stale"]}
            raise RuntimeError(
                "stale feeds: " + ", ".join(f"{feed} ({ages[feed]}h)" for feed in stale)
            )
        return stale

    measured = measure.expand(feed=sorted(FEEDS))
    record(measured, run_id="{{ run_id }}") >> report_stale(measured)


plat_conn_healthcheck()
