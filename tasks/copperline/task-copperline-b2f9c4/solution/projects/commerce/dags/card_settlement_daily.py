"""The card settlement figure of every day the night's delivery moved.

`marts.card_settlement_daily`, one row per settlement day per currency: the
events, the captures, the refunds and the chargebacks. Finance's cash sheet
reads it.

The run owns a night of delivery rather than a day of settlement. Meridian
sends the tail of a day for five days afterwards, so a night rebuilds the six
settlement days it carried rows for and leaves every other day alone.
`projects/commerce/lib/card_settlement.py` says why, and
`docs/late-data-policy.md` LD-1 and LD-2 are the policy behind it.

Runs at 05:00, after the last hour of the night is in. Owned by commerce
(J. Mwangi).
"""

from __future__ import annotations

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from include.lib.notify import notify
from projects.commerce.lib import card_settlement

DEFAULT_ARGS = {
    "owner": "commerce",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=10),
}

#: The night this run owns. `CONVENTIONS.md`: the run on the 15th owns the
#: delivery of the 14th, which is the last night that is complete.
TARGET_DS = "{{ macros.ds_add(ds, -1) }}"

with DAG(
    dag_id="card_settlement_daily",
    schedule="0 5 * * *",
    start_date=pendulum.datetime(2025, 7, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["commerce", "mart", "payments"],
    doc_md=__doc__,
    on_failure_callback=notify("commerce", "card settlement daily"),
) as dag:
    load = PythonOperator(
        task_id="load_delivery",
        python_callable=card_settlement.load_delivery,
        op_kwargs={"ds": TARGET_DS},
    )

    check = PythonOperator(
        task_id="check_days",
        python_callable=card_settlement.check_days,
        op_kwargs={"ds": TARGET_DS},
    )

    load >> check
