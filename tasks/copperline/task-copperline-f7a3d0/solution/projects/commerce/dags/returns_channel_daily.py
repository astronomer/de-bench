"""Returns split by the channel that sold them, one order day a run.

`marts.returns_by_channel_day`, one row per order day and channel: the
authorisations raised against that day's orders in that channel, the money
going back and which book it goes back into, and the store credit handed over a
counter. Store finance and merchandising argue from it.

The channel is the ORDER's — the channel that took the sale. The returns feed's
own `return_channel` is the door the goods came back through, and it appears
here once, in `counter_credit_cents`.
`projects/commerce/lib/returns_channel.py` says why.

A run owns one order day and rebuilds it whole, from every authorisation the
feed carries against it. A return comes in for two months after a sale, so the
day is rebuilt many times before it stops moving; `docs/late-data-policy.md`
LD-2 is the rule. Runs at 07:00, after `returns_daily` has landed the night.
Owned by commerce (J. Mwangi).
"""

from __future__ import annotations

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from include.lib.notify import notify
from projects.commerce.lib import returns_channel

DEFAULT_ARGS = {
    "owner": "commerce",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=10),
}

#: The order day this run owns. `CONVENTIONS.md`: the run on the 15th owns the
#: 14th, which is the last day that is complete.
TARGET_DS = "{{ macros.ds_add(ds, -1) }}"

with DAG(
    dag_id="returns_channel_daily",
    schedule="0 7 * * *",
    start_date=pendulum.datetime(2024, 2, 4, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["commerce", "mart", "returns"],
    doc_md=__doc__,
    on_failure_callback=notify("commerce", "returns by channel daily"),
) as dag:
    build = PythonOperator(
        task_id="build_day",
        python_callable=returns_channel.build_day,
        op_kwargs={"ds": TARGET_DS},
    )

    tie = PythonOperator(
        task_id="tie_day",
        python_callable=returns_channel.tie_day,
        op_kwargs={"ds": TARGET_DS},
    )

    build >> tie
