"""Poll the carrier tracking API for the packages still in flight.

The scan feed says what happened; this says where a package is now. It pages the
tracking listing once an hour and refreshes the milestone table from it.

**The listing pages and it does not say how far.** Fifty rows and a
`next_page_token` per page, `total` always null, and the token absent only on
the last page. `JsonApiToWarehouseOperator` follows the token to the end. This
DAG and `gro_seo_rank_intake` are the two places in the world where that is
written out in full, and both are the same three lines: give the endpoint, give
the parameters, let the operator page.

**A first-page answer looks exactly like a complete one.** Fifty rows come back,
the load succeeds, the counts are plausible, and the packages beyond the first
page simply are not there. Nothing says so.

Produces `raw.package_tracking`. Read by `int_shipment_milestones` and the
customer team's delivery promise.

Owned by supply-chain (K. Duffy).
"""

from __future__ import annotations

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from include.lib import warehouse, watermark
from include.lib.loaders import JsonApiToWarehouseOperator
from include.lib.notify import notify

DEFAULT_ARGS = {
    "owner": "supply",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
}

STREAM = "package_tracking"

TRACKING_COLUMNS = {
    "package_id": "VARCHAR",
    "carrier_code": "VARCHAR",
    "tracking_number": "VARCHAR",
    "milestone": "VARCHAR",
    "milestone_at": "TIMESTAMP",
    "status_date": "DATE",
    "facility_code": "VARCHAR",
    "loaded_at": "TIMESTAMP",
}

IN_FLIGHT = """
-- Packages dispatched and not yet delivered. This is the denominator the poll
-- is measured against: every one of them should come back from the listing.
SELECT count(*)
FROM copperline.raw.shipment_packages
WHERE ship_date >= ?::DATE - 30 AND delivered_at IS NULL
"""

POLLED = """
SELECT count(DISTINCT package_id)
FROM copperline.raw.package_tracking
WHERE status_date = ?::DATE
"""


def check_coverage(ds: str) -> int:
    """The packages the poll returned against the packages in flight.

    A poll that stopped at the first page comes back with fifty and the
    difference is the whole of the rest. There is no `total` on the wire, so
    this is the only place the shortfall can be seen.
    """
    with warehouse.connect(read_only=True) as con:
        in_flight = int(con.execute(IN_FLIGHT, [ds]).fetchone()[0])
        polled = int(con.execute(POLLED, [ds]).fetchone()[0])
    if in_flight and polled < in_flight // 2:
        raise ValueError(
            f"{ds}: {polled} packages came back from the tracking listing and "
            f"{in_flight} are in flight. Follow next_page_token to the last page."
        )
    return polled


def check_milestone_order(ds: str) -> int:
    """A package's milestones do not go backwards.

    The carriers re-send a milestone when a scan is corrected, and a correction
    that arrives with the wrong timestamp moves a package back to in-transit
    after it was delivered.
    """
    with warehouse.connect(read_only=True) as con:
        backwards = int(con.execute(
            "SELECT count(*) FROM ("
            "  SELECT package_id, milestone_at,"
            "         lag(milestone_at) OVER (PARTITION BY package_id"
            "                                 ORDER BY loaded_at) AS previous"
            f"  FROM {warehouse.qualify('raw.package_tracking')}"
            "   WHERE status_date = ?::DATE)"
            " WHERE previous IS NOT NULL AND milestone_at < previous",
            [ds],
        ).fetchone()[0])
    if backwards:
        raise ValueError(f"{ds}: {backwards} packages have milestones out of order")
    return backwards


def report_in_flight(ds: str) -> int:
    """How many packages are still out, for the morning note."""
    with warehouse.connect(read_only=True) as con:
        return int(con.execute(IN_FLIGHT, [ds]).fetchone()[0])


def advance_mark(**context) -> str:
    """Move the mark to the end of this run's window. Last, and on success."""
    return watermark.advance(STREAM, context).isoformat()


with DAG(
    dag_id="sc_carrier_api_poll",
    schedule="45 * * * *",
    start_date=pendulum.datetime(2024, 2, 4, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["supply", "raw", "intake", "carriers"],
    doc_md=__doc__,
    on_failure_callback=notify("supply", "carrier tracking poll"),
) as dag:
    poll = JsonApiToWarehouseOperator(
        task_id="poll_tracking",
        endpoint="carriers/tracking",
        params={"since": "{{ ds }}", "status": "in_flight"},
        table="raw.package_tracking",
        mode="replace",
        partition_col="status_date",
        columns=TRACKING_COLUMNS,
    )

    coverage = PythonOperator(
        task_id="check_coverage", python_callable=check_coverage,
        op_kwargs={"ds": "{{ ds }}"})
    order = PythonOperator(
        task_id="check_milestone_order", python_callable=check_milestone_order,
        op_kwargs={"ds": "{{ ds }}"})
    in_flight = PythonOperator(
        task_id="report_in_flight", python_callable=report_in_flight,
        op_kwargs={"ds": "{{ ds }}"})
    mark = PythonOperator(task_id="advance_watermark", python_callable=advance_mark)

    poll >> coverage >> order >> in_flight >> mark
