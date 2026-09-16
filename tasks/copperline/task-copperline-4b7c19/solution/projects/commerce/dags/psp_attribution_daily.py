"""Every Meridian settlement event of a day, under the order that paid for it.

`marts.settlement_attribution`, one row per event: the event, its payment
intent, its order, its type and its amount. The refund and chargeback recovery
work reads it, and the order named on a row is what puts the money against a
store and a market.

The attribution runs through `raw.payment_intents`, not through the reference
on the event row. `docs/billing-integration.md` B-6 and
`projects/commerce/lib/attribution.py` say why.

Runs at 04:30 and takes under a minute. Owned by commerce (J. Mwangi).
"""

from __future__ import annotations

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from include.lib.notify import notify
from projects.commerce.lib import attribution

DEFAULT_ARGS = {
    "owner": "commerce",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=10),
}

#: The day this run owns. `docs/late-data-policy.md` LD-3: a daily output holds
#: back one day, so the run on the 15th builds the 14th.
TARGET_DS = "{{ macros.ds_add(ds, -1) }}"

with DAG(
    dag_id="psp_attribution_daily",
    schedule="30 4 * * *",
    start_date=pendulum.datetime(2025, 7, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["commerce", "mart", "payments"],
    doc_md=__doc__,
    on_failure_callback=notify("commerce", "settlement attribution"),
) as dag:
    build = PythonOperator(
        task_id="build_attribution",
        python_callable=attribution.build_daily,
        op_kwargs={"ds": TARGET_DS},
    )

    grain = PythonOperator(
        task_id="check_grain",
        python_callable=attribution.check_grain,
        op_kwargs={"ds": TARGET_DS},
    )

    build >> grain
