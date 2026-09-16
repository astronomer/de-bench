"""Refunds for the day, and which side of the fourteen-day line each falls.

`docs/finance-policy.md` REV-6 is the whole of this DAG. A refund requested
within fourteen days of the invoice date voids the schedule — everything
recognised so far reverses and the line recognises nothing. A refund after day
fourteen cancels forward: what recognised stands, and nothing more will.

The test is by calendar date in America/Los_Angeles and day fourteen is inside
the window, so a refund on day fourteen voids and one on day fifteen cancels.
It never uses elapsed hours and it never uses the refund's own load time. This
DAG stamps the side; finance's recognition build reads the stamp.

Produces `marts.refunds_daily`. Read by `fin_close_monthly`, the recognition
build and `marts.dispute_daily`.

Owned by commerce (J. Mwangi).
"""

from __future__ import annotations

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from include.lib import warehouse
from include.lib.notify import notify
from projects.commerce.lib import sql

DEFAULT_ARGS = {
    "owner": "commerce",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=10),
}

#: REV-6's clock. The boundary is a calendar date in this zone and not a count
#: of hours, so a refund at 23:00 in Los Angeles and one at 08:00 in Berlin the
#: next morning can be the same day.
BOUNDARY_ZONE = "America/Los_Angeles"

#: Day fourteen is inside the window: `<= 14` voids, `> 14` cancels.
VOID_DAYS = 14


def collect_refunds(ds: str) -> int:
    """The day's refunds, with the invoice date each one refunds against."""
    with warehouse.connect() as con:
        return int(con.execute(sql.read("refunds_collect"), [ds]).fetchone()[0])


def stamp_boundary(ds: str) -> int:
    """Mark each refund `void` or `cancel` on the REV-6 test.

    Both dates are taken in `America/Los_Angeles` before the difference is
    computed, which is why the zone is a parameter of the statement rather than
    something the database decides.
    """
    with warehouse.connect() as con:
        return int(con.execute(
            sql.read("refunds_stamp_boundary"), [ds, BOUNDARY_ZONE, VOID_DAYS],
        ).fetchone()[0])


def publish(ds: str) -> int:
    """Replace the day's partition of `marts.refunds_daily`."""
    with warehouse.connect() as con:
        return int(con.execute(sql.read("refunds_publish"), [ds, ds]).fetchone()[0])


def check_every_refund_stamped(ds: str) -> int:
    """No refund leaves without a side.

    An unstamped refund is one whose invoice date could not be found, and
    finance's recognition build would silently treat it as a cancel — the
    cheaper of the two answers and the wrong one about half the time.
    """
    with warehouse.connect(read_only=True) as con:
        unstamped = int(con.execute(sql.read("refunds_unstamped"), [ds]).fetchone()[0])
    if unstamped:
        raise ValueError(
            f"{ds}: {unstamped} refunds carry no REV-6 side. Find the invoice "
            "date rather than letting them default."
        )
    return unstamped


with DAG(
    dag_id="refunds_daily",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2024, 2, 4, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["commerce", "mart", "refunds"],
    doc_md=__doc__,
    on_failure_callback=notify("commerce", "refunds"),
) as dag:
    collect = PythonOperator(
        task_id="collect_refunds", python_callable=collect_refunds,
        op_kwargs={"ds": "{{ ds }}"})
    stamp = PythonOperator(
        task_id="stamp_boundary", python_callable=stamp_boundary,
        op_kwargs={"ds": "{{ ds }}"})
    write = PythonOperator(
        task_id="publish_refunds", python_callable=publish,
        op_kwargs={"ds": "{{ ds }}"})
    check = PythonOperator(
        task_id="check_every_refund_stamped",
        python_callable=check_every_refund_stamped,
        op_kwargs={"ds": "{{ ds }}"})

    collect >> stamp >> write >> check
