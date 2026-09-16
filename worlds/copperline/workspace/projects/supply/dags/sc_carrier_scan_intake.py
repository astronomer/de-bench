"""Carrier scan events into `raw.carrier_scans`, hourly.

Every parcel is scanned several times on its way — picked up, in transit, out
for delivery, delivered — and the three carriers each spell those events their
own way. The feed is a listing on the carrier API and it pages: fifty rows and a
`next_page_token`, with `total` always null, so the only way to know the answer
is whole is to follow the token to the end.

**The scan codes are the carriers' vocabulary, not ours.** `GATE` is one of
them, and which milestone it means is not the same on every carrier or in every
era. `int_scan_normalized` is where that is resolved; nothing is translated on
the way in, which is what keeps the raw feed a record of what the carrier said.

A scan whose lane is not in `raw.lanes` is parked rather than dropped. A parked
scan is a lane somebody opened without telling us, and it comes back when the
lane is added.

Produces `raw.carrier_scans`. Read by `int_shipment_milestones`,
`int_scan_normalized` and `sc_lane_performance_daily`.

Owned by supply-chain (K. Duffy).
"""

from __future__ import annotations

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from include.lib import warehouse, watermark
from include.lib.loaders import JsonApiToWarehouseOperator
from include.lib.notify import notify
from include.lib.pipeline import Reject, lake_task

DEFAULT_ARGS = {
    "owner": "supply",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
}

STREAM = "carrier_scans"

LAKE_CONFIG = {
    "reject_table": "ops.scan_rejects",
    "retry": {"attempts": 2, "delay_seconds": 30},
}

SCAN_COLUMNS = {
    "scan_id": "VARCHAR",
    "package_id": "VARCHAR",
    "carrier_code": "VARCHAR",
    "scan_code": "VARCHAR",
    "scan_date": "DATE",
    "scanned_at": "TIMESTAMP",
    "facility_code": "VARCHAR",
    "lane_id": "VARCHAR",
    "loaded_at": "TIMESTAMP",
}

ORPHAN_LANES = """
SELECT s.scan_id, s.carrier_code, s.lane_id, s.scan_code, s.scan_date
FROM copperline.raw.carrier_scans s
LEFT JOIN copperline.raw.lanes l ON l.lane_id = s.lane_id
WHERE s.scan_date = ?::DATE AND l.lane_id IS NULL
ORDER BY s.scan_id
"""

DUPLICATES = """
-- A scan id is the carrier's own idempotency key. A repeat means a page was
-- taken twice, which is what an append load of the same page does.
SELECT scan_id, count(*) AS copies
FROM copperline.raw.carrier_scans
WHERE scan_date = ?::DATE
GROUP BY scan_id
HAVING count(*) > 1
ORDER BY copies DESC, scan_id
"""


def check_duplicates(ds: str) -> int:
    """One row per scan id for the day."""
    with warehouse.connect(read_only=True) as con:
        repeats = con.execute(DUPLICATES, [ds]).fetchall()
    if repeats:
        names = ", ".join(str(row[0]) for row in repeats[:5])
        raise ValueError(f"{ds}: scan ids repeat: {names}")
    return 0


@lake_task(task_id="park_orphan_lanes", lake_config=LAKE_CONFIG, subject="scans")
def park_orphan_lanes(ds: str) -> int:
    """Park scans on a lane `raw.lanes` does not carry.

    A branch of its own: a rejected batch ends skipped, and nothing downstream
    of this waits on it. The scans that do have a lane still publish, and the
    parked ones sit in the reject table with the lane on them.
    """
    with warehouse.connect(read_only=True) as con:
        orphans = con.execute(ORPHAN_LANES, [ds]).fetchall()
    if orphans:
        columns = ["scan_id", "carrier_code", "lane_id", "scan_code", "scan_date"]
        raise Reject(
            f"{len(orphans)} scans on {ds} name a lane raw.lanes does not have",
            rows=[dict(zip(columns, row)) for row in orphans],
        )
    return 0


def report_scan_codes(ds: str) -> list[str]:
    """The codes the day carried, per carrier.

    Reported and not failed: a new code is the carriers' business and finding
    out what it means takes a phone call, not a red run. `int_scan_normalized`
    is where an unmapped code becomes a problem.
    """
    with warehouse.connect(read_only=True) as con:
        rows = con.execute(
            f"SELECT carrier_code, scan_code, count(*) "
            f"FROM {warehouse.qualify('raw.carrier_scans')} "
            "WHERE scan_date = ?::DATE GROUP BY carrier_code, scan_code "
            "ORDER BY carrier_code, scan_code",
            [ds],
        ).fetchall()
    return [f"{carrier} {code}: {count}" for carrier, code, count in rows]


def advance_mark(**context) -> str:
    """Move the mark to the end of this run's window. Last, and on success."""
    return watermark.advance(STREAM, context).isoformat()


with DAG(
    dag_id="sc_carrier_scan_intake",
    schedule="25 * * * *",
    start_date=pendulum.datetime(2024, 2, 4, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params={"lake_config": LAKE_CONFIG},
    tags=["supply", "raw", "intake", "carriers"],
    doc_md=__doc__,
    on_failure_callback=notify("supply", "carrier scan intake"),
) as dag:
    load = JsonApiToWarehouseOperator(
        task_id="load_scans",
        endpoint="carriers/scans",
        params={"since": "{{ ds }}"},
        table="raw.carrier_scans",
        mode="replace",
        partition_col="scan_date",
        columns=SCAN_COLUMNS,
    )

    duplicates = PythonOperator(
        task_id="check_duplicates", python_callable=check_duplicates,
        op_kwargs={"ds": "{{ ds }}"})
    parked = park_orphan_lanes(ds="{{ ds }}")
    codes = PythonOperator(
        task_id="report_scan_codes", python_callable=report_scan_codes,
        op_kwargs={"ds": "{{ ds }}"})
    mark = PythonOperator(task_id="advance_watermark", python_callable=advance_mark)

    load >> duplicates >> [parked, codes]
    codes >> mark
