"""Build `marts.category_margin`: net sales less landed cost, by category.

The sibling of `fin_revenue_daily` and deliberately the same five steps: wait
for the source, rebuild the day whole, check the money and the shape, tie the
total, publish the file. Two DAGs with one shape is how this team keeps its
dailies readable.

The join is the part worth reading. `int_net_sales_lines` and
`int_order_lines_costed` meet one to one at line grain — a line has one landed
cost, at the order date — and a fan-out there doubles the margin while leaving
the sales figure untouched, which is a shape nobody spots in a total.

Both sources are shared `int` models under platform ownership. Finance does not
rebuild net sales and does not rebuild landed cost; commerce reads the same two,
and that is the whole point of them being shared.

Owned by finance-analytics.
"""

from __future__ import annotations

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.sensors.python import PythonSensor
from airflow.sdk import DAG

from include.lib.notify import notify
from projects.finance.lib import revenue, warehouse_checks

#: The day this run owns. See `CONVENTIONS.md`.
TARGET_DS = "{{ macros.ds_add(ds, -1) }}"

TABLE = "marts.category_margin"
SOURCE = "int.int_net_sales_lines"

DEFAULT_ARGS = {
    "owner": "finance",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
    "on_failure_callback": notify("finance", "category margin did not rebuild"),
}

with DAG(
    dag_id="fin_margin_daily",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2024, 2, 4, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["finance", "mart", "margin"],
    doc_md=__doc__,
) as dag:
    wait_for_lines = PythonSensor(
        task_id="wait_for_lines",
        python_callable=warehouse_checks.has_partition,
        op_kwargs={"table": SOURCE, "partition_col": "order_date",
                   "partition_value": TARGET_DS},
        poke_interval=300,
        timeout=60 * 60 * 3,
        mode="reschedule",
        doc_md="Waits for the line-grain source the 04:00 build lands. Three "
               "hours, then give up and say so.",
    )

    build = PythonOperator(
        task_id="build",
        python_callable=revenue.build_margin,
        op_kwargs={"target_ds": TARGET_DS, "table": TABLE},
        pool="warehouse_write",
        doc_md="Rebuild the day whole: net sales and landed cost by category, "
               "in cents, joined one to one at line grain.",
    )

    check_cents = PythonOperator(
        task_id="check_cents",
        python_callable=warehouse_checks.assert_integer_cents,
        op_kwargs={"table": TABLE,
                   "columns": ["net_sales_cents", "landed_cost_cents",
                               "merch_margin_cents"]},
        doc_md="Three money columns, all integers.",
    )

    check_rows = PythonOperator(
        task_id="check_rows",
        python_callable=warehouse_checks.assert_row_count,
        op_kwargs={"table": TABLE, "partition_col": "ds",
                   "partition_value": TARGET_DS, "at_least": 20},
        doc_md="The book has 181 categories and a normal day touches most of "
               "them. Twenty is the floor a real day has never gone under, and "
               "it catches the morning the join produced nothing.",
    )

    tie_to_sales = PythonOperator(
        task_id="tie_to_sales",
        python_callable=warehouse_checks.assert_total_matches,
        op_kwargs={
            "table": TABLE, "column": "net_sales_cents",
            "source_table": SOURCE, "source_column": "net_sales_cents",
            "partition_col": "ds", "source_partition_col": "order_date",
            "partition_value": TARGET_DS,
        },
        doc_md="The day's net sales equal the shared model's, to the cent. This "
               "is the check that catches the fan-out: a doubled join moves the "
               "margin and leaves this figure alone only when it is written "
               "against the same source, so it is written against the same "
               "source.",
    )

    publish = PythonOperator(
        task_id="publish",
        python_callable=warehouse_checks.publish_partition,
        op_kwargs={"table": TABLE, "partition_col": "ds",
                   "partition_value": TARGET_DS, "name": "category_margin",
                   "order_by": ["category_id"]},
        doc_md="Write the day out as "
               "`include/data/marts/category_margin_<ds>.csv`.",
    )

    wait_for_lines >> build >> [check_cents, check_rows] >> tie_to_sales >> publish
