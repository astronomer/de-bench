"""Build `marts.daily_revenue`: booked revenue by day and channel.

Booked, not recognised. `docs/semantic-definitions.md` keeps the two apart:
`booked_cents` is the order's value on the order date, gross, with no
recognition schedule behind it, and it never moves afterwards. The recognised
figures are the close file's subject and are built somewhere else.

The house shape for a finance daily: wait for the source, rebuild the day
whole, check the money and the shape, tie the total to the order spine, publish
the file. `fin_margin_daily` is its sibling and follows the same five steps for
the same reasons.

Owned by finance-analytics. When this stops, the rendered daily summary has no
numbers and the budget comparison has no actuals.
"""

from __future__ import annotations

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.sensors.python import PythonSensor
from airflow.sdk import DAG

from include.lib.notify import notify
from projects.finance.lib import revenue, warehouse_checks

#: The day this run owns. A bare cron string is a trigger timetable, so `ds` is
#: the day the run fires and the day being built is the one before it — see
#: `CONVENTIONS.md`, and `docs/late-data-policy.md` §LD-3 for why a published
#: daily output holds back a day at all.
TARGET_DS = "{{ macros.ds_add(ds, -1) }}"

TABLE = "marts.daily_revenue"
SOURCE = "int.int_orders_enriched"

DEFAULT_ARGS = {
    "owner": "finance",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
    "on_failure_callback": notify("finance", "daily revenue did not rebuild"),
}

with DAG(
    dag_id="fin_revenue_daily",
    schedule="0 6 * * *",
    start_date=pendulum.datetime(2024, 2, 4, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["finance", "mart", "revenue"],
    doc_md=__doc__,
) as dag:
    wait_for_orders = PythonSensor(
        task_id="wait_for_orders",
        python_callable=warehouse_checks.has_partition,
        op_kwargs={"table": SOURCE, "partition_col": "order_date",
                   "partition_value": TARGET_DS},
        poke_interval=300,
        timeout=60 * 60 * 3,
        mode="reschedule",
        doc_md="The 04:00 build lands the source two hours before this runs. "
               "The wait is for the mornings it did not: three hours, then give "
               "up and say so, rather than sitting in `running` all day.",
    )

    build = PythonOperator(
        task_id="build",
        python_callable=revenue.build_daily,
        op_kwargs={"target_ds": TARGET_DS, "table": TABLE},
        pool="warehouse_write",
        doc_md="Rebuild the day whole from the shared order model, in cents, "
               "one row per channel. The write is a delete-insert on the day, "
               "so a second run of the same day leaves one copy.",
    )

    check_cents = PythonOperator(
        task_id="check_cents",
        python_callable=warehouse_checks.assert_integer_cents,
        op_kwargs={"table": TABLE, "columns": ["booked_cents"]},
        doc_md="Money is an integer number of cents. A float here is a defect, "
               "and it is cheaper to catch on the day than in the close.",
    )

    check_channels = PythonOperator(
        task_id="check_channels",
        python_callable=revenue.assert_channels,
        op_kwargs={"target_ds": TARGET_DS, "table": TABLE},
        doc_md="All four channels landed a row. `trade` is the one to watch: "
               "six per cent of the orders and a third of the revenue.",
    )

    tie_to_orders = PythonOperator(
        task_id="tie_to_orders",
        python_callable=warehouse_checks.assert_total_matches,
        op_kwargs={
            "table": TABLE, "column": "booked_cents",
            "source_table": SOURCE, "source_column": "booked_cents",
            "partition_col": "ds", "source_partition_col": "order_date",
            "partition_value": TARGET_DS,
        },
        doc_md="The day's booked total equals the order spine's, to the cent. "
               "It ties against orders rather than against the ledger, because "
               "the ledger covers closed months and this is today's.",
    )

    publish = PythonOperator(
        task_id="publish",
        python_callable=warehouse_checks.publish_partition,
        op_kwargs={"table": TABLE, "partition_col": "ds",
                   "partition_value": TARGET_DS, "name": "daily_revenue",
                   "order_by": ["channel"]},
        doc_md="Write the day out as `include/data/marts/daily_revenue_<ds>.csv`.",
    )

    wait_for_orders >> build >> [check_cents, check_channels] >> tie_to_orders >> publish
