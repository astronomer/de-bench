"""Orders against the processor, row by row, into `marts.recon_exceptions`.

`docs/reconciliation-policy.md` is the ladder and this DAG is the ladder in
order: R-1 takes the processor's word on money, R-2 takes ours on goods, R-3
lets an open dispute override both, R-4 ages the disagreement on the processor's
own clock, and R-5 sends a correction that cannot go back to the earliest open
month. One task per clause, so a wrong number tells you which rule produced it.

Runs at 05:00 and takes about nine minutes. The payments squad works the output
before the flash goes out at 05:45.

Produces `marts.recon_exceptions`. Read by the payments squad's morning list,
finance's close totals and `marts.dispute_daily`.

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

#: The three finders, and the exception kinds each writes.
#:
#: R-6's `deleted_at_source` is not one of them: a soft-deleted row keeps its
#: last known amounts and stays in the match, so it is settled where the match
#: is made rather than found afterwards. R-7's two directions are one finder
#: because they are one clause and share a denominator.
FINDERS = (
    ("amount_mismatch", "recon_amount_mismatch"),
    ("state_mismatch", "recon_state_mismatch"),
    ("unmatched", "recon_unmatched"),
)


def match_payments(ds: str) -> int:
    """Pair the day's orders with the day's processor events.

    The pairing goes through `raw.payment_intents.order_id`, which carries the
    OMS's own order key. `docs/billing-integration.md` B-6 says why the text
    reference on the settlement row is not the key to join on.
    """
    with warehouse.connect() as con:
        return int(con.execute(sql.read("recon_match"), [ds]).fetchone()[0])


def apply_dispute_override(ds: str) -> int:
    """R-3: while a dispute is open its state wins over both sides."""
    with warehouse.connect() as con:
        return int(con.execute(sql.read("recon_dispute_override"), [ds]).fetchone()[0])


def find_kind(kind: str, statement: str, ds: str) -> int:
    """One exception kind for the day, into the working table."""
    with warehouse.connect() as con:
        return int(con.execute(sql.read(statement), [ds, kind]).fetchone()[0])


def age_disagreements(ds: str) -> int:
    """R-4: age every disagreement on the processor's `event_time`.

    Under 48 hours is `pending_sync` and is not an exception, however long ago
    we loaded it. The load time is the wrong column for every question about a
    payment, and it is the column the first version of this used.
    """
    with warehouse.connect() as con:
        return int(con.execute(sql.read("recon_age"), [ds]).fetchone()[0])


def book_closed_period_adjustments(ds: str) -> int:
    """R-5: a correction whose `ds` sits in a closed month books one adjustment
    row on the 1st of the earliest open month instead of restating."""
    with warehouse.connect() as con:
        return int(con.execute(sql.read("recon_closed_period"), [ds]).fetchone()[0])


def publish_exceptions(ds: str) -> int:
    """Replace the day's partition of `marts.recon_exceptions`."""
    with warehouse.connect() as con:
        return int(con.execute(sql.read("recon_publish"), [ds]).fetchone()[0])


def check_totals(ds: str) -> int:
    """Every processor event and every order is accounted for exactly once.

    An event that is both matched and reported unmatched, or neither, means the
    ladder was worked out of order. Returns the exception count, excluding
    `pending_sync`, which is the number the squad reads.
    """
    with warehouse.connect(read_only=True) as con:
        unaccounted, exceptions = con.execute(
            sql.read("recon_totals_check"), [ds, ds],
        ).fetchone()
    if unaccounted:
        raise ValueError(
            f"{ds}: {unaccounted} rows are neither matched nor on the exception "
            "list. Work docs/reconciliation-policy.md in order."
        )
    return int(exceptions or 0)


with DAG(
    dag_id="payments_recon_daily",
    schedule="0 5 * * *",
    start_date=pendulum.datetime(2024, 2, 4, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["commerce", "mart", "payments"],
    doc_md=__doc__,
    on_failure_callback=notify("commerce", "payments reconciliation"),
) as dag:
    match = PythonOperator(
        task_id="match_payments",
        python_callable=match_payments,
        op_kwargs={"ds": "{{ ds }}"},
    )

    disputes = PythonOperator(
        task_id="apply_dispute_override",
        python_callable=apply_dispute_override,
        op_kwargs={"ds": "{{ ds }}"},
    )

    finders = []
    for _kind, _statement in FINDERS:
        _task = PythonOperator(
            task_id=f"find_{_kind}",
            python_callable=find_kind,
            op_kwargs={"kind": _kind, "statement": _statement, "ds": "{{ ds }}"},
        )
        finders.append(_task)

    age = PythonOperator(
        task_id="age_disagreements",
        python_callable=age_disagreements,
        op_kwargs={"ds": "{{ ds }}"},
    )

    closed = PythonOperator(
        task_id="book_closed_period_adjustments",
        python_callable=book_closed_period_adjustments,
        op_kwargs={"ds": "{{ ds }}"},
    )

    publish = PythonOperator(
        task_id="publish_exceptions",
        python_callable=publish_exceptions,
        op_kwargs={"ds": "{{ ds }}"},
    )

    totals = PythonOperator(
        task_id="check_totals",
        python_callable=check_totals,
        op_kwargs={"ds": "{{ ds }}"},
    )

    match >> disputes >> finders >> age >> closed >> publish >> totals
