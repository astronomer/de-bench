"""Returns netted onto the day that sold them, for every day tonight moved.

`marts.returns_by_order_day`, one row per order day: the authorisations raised
against that day's orders, the money going back, and what the lines behind them
sold for. Merchandising reads it.

The run owns a night of the returns feed rather than a day of selling. An
authorisation is raised three to sixty days after its order, so a night names
order days spread over two months, rebuilds each of them whole, and leaves
every other day alone. `projects/commerce/lib/returns_netting.py` says why, and
`docs/late-data-policy.md` LD-1 and LD-2 are the policy behind it.

Runs at 06:30, after `returns_daily` has landed the night. Owned by commerce
(J. Mwangi).
"""

from __future__ import annotations

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from include.lib.notify import notify
from projects.commerce.lib import returns_netting

DEFAULT_ARGS = {
    "owner": "commerce",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=10),
}

#: The night this run owns. `CONVENTIONS.md`: the run on the 15th owns the
#: delivery of the 14th, which is the last night that is complete.
TARGET_DS = "{{ macros.ds_add(ds, -1) }}"

with DAG(
    dag_id="returns_netting_daily",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2024, 2, 4, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["commerce", "mart", "returns"],
    doc_md=__doc__,
    on_failure_callback=notify("commerce", "returns netting daily"),
) as dag:
    net = PythonOperator(
        task_id="net_delivery",
        python_callable=returns_netting.net_delivery,
        op_kwargs={"ds": TARGET_DS},
    )

    tie = PythonOperator(
        task_id="tie_days",
        python_callable=returns_netting.tie_days,
        op_kwargs={"ds": TARGET_DS},
    )

    net >> tie
