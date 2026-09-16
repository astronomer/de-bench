"""The trade account statement, one fiscal period at a time.

One row per trade account per currency for the period that has just closed,
built from the OMS's own parquet orders export rather than from a mart: the
statement has to carry the figures the OMS published, because those are the
figures the account is answered with when it queries a line.

**The cron is daily and the work is monthly.** A 4-5-4 period closes on a
Saturday, so the morning after a close is not a day a monthly cron can name.
`check_period_closed` lets one run a period through and stops the other
thirty-odd, which keeps `ds - 1` the last day of the period being reported.

`projects/finance/lib/trade_statement.py` holds the work. The period comes from
`raw.fiscal_calendar`, so a five-week period is five weeks long here too.

Owned by finance-analytics (N. Brandt).
"""

from __future__ import annotations

import pendulum
from airflow.providers.standard.operators.python import (
    PythonOperator,
    ShortCircuitOperator,
)
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.sdk import DAG

from include.lib import landing_dir
from include.lib.notify import notify
from projects.finance.lib import trade_statement

#: The last day of the period this run reports. The run that gets through the
#: short circuit fires the morning after a period closes, so the day the data
#: is about is `ds - 1`.
PERIOD_LAST_DAY = "{{ macros.ds_add(ds, -1) }}"

DEFAULT_ARGS = {
    "owner": "finance",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=15),
    "on_failure_callback": notify("finance", "the trade statement did not build"),
}

with DAG(
    dag_id="fin_trade_statement_monthly",
    schedule="0 9 * * *",
    start_date=pendulum.datetime(2024, 2, 4, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["finance", "mart", "trade"],
    doc_md=__doc__,
) as dag:
    check_period_closed = ShortCircuitOperator(
        task_id="check_period_closed",
        python_callable=trade_statement.period_closed,
        op_kwargs={"run_ds": "{{ ds }}"},
        doc_md="Did a fiscal period end yesterday? Read from "
               "`raw.fiscal_calendar`. Every other morning stops here.",
    )

    wait_for_export = FileSensor(
        task_id="wait_for_export",
        filepath=str(landing_dir("orders_export") / f"dt={PERIOD_LAST_DAY}"
                     / "part-000.parquet"),
        poke_interval=600,
        timeout=60 * 60 * 4,
        mode="reschedule",
        doc_md="The period's last export file. The rollup reads the whole "
               "period, and the last night of it is the one that lands late.",
    )

    build_statement = PythonOperator(
        task_id="build_statement",
        python_callable=trade_statement.build_statement,
        op_kwargs={"run_ds": "{{ ds }}"},
        pool="warehouse_write",
        doc_md="Replace the closing period's rows in `marts.trade_statement`. "
               "The period is the partition, keyed on the day it closed, so a "
               "rerun replaces its own rows and no other period's.",
    )

    publish_statement = PythonOperator(
        task_id="publish_statement",
        python_callable=trade_statement.publish_statement,
        op_kwargs={"run_ds": "{{ ds }}"},
        doc_md="`include/data/marts/trade_statement_<period end>.csv`, which "
               "is what the desk's mail merge reads.",
    )

    check_period_closed >> wait_for_export >> build_statement >> publish_statement
