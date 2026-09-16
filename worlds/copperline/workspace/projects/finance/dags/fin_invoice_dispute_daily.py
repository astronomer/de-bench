"""Disputes opened and closed, every morning at 10:00.

Builds `marts.dispute_daily` and writes the open count per entity to
`ops.dispute_open_daily`, which is what the collections team reads.

§REV-10 is the rule and it is the one people expect to be otherwise. A disputed
amount **stays recognised** while the dispute is open: recognition does not
pause, and a provision is not netted against revenue. When a dispute closes in
the customer's favour the write-off books on the close date under §REV-5, and
when it closes in Copperline's favour nothing books at all.

A row in `raw.disputes` with `closed_on` NULL is open, whatever its age. The
open figure is a count of rows rather than a sum of amounts, because a
dispute's amount moves while it is open and a moving sum is not a number
anybody can act on.

Owned by finance-analytics.
"""

from __future__ import annotations

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from include.lib import warehouse, workspace_root
from include.lib.notify import notify
from projects.finance.lib import warehouse_checks

DBT_ROOT = workspace_root() / "dbt" / "copperline_analytics"

#: The day this run owns. See `CONVENTIONS.md`.
TARGET_DS = "{{ macros.ds_add(ds, -1) }}"

TABLE = "marts.dispute_daily"
OPEN_TABLE = "ops.dispute_open_daily"

DEFAULT_ARGS = {
    "owner": "finance",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
    "on_failure_callback": notify("finance", "the dispute mart did not rebuild"),
}


def open_by_entity(target_ds: str) -> int:
    """Replace the day's open counts. Returns the rows written."""
    with warehouse.connect() as con:
        rows = con.execute(
            f"""SELECT entity, sum(open_disputes)::BIGINT AS open_disputes,
                       sum(disputed_cents)::BIGINT        AS disputed_cents
                FROM {warehouse.qualify(TABLE)}
                WHERE ds = ?
                GROUP BY entity
                ORDER BY entity""",
            [target_ds],
        ).fetchall()
        return warehouse.delete_insert(
            OPEN_TABLE, "ds", target_ds, [(target_ds, *row) for row in rows],
            columns=["ds", "entity", "open_disputes", "disputed_cents"],
            con=con,
        )


with DAG(
    dag_id="fin_invoice_dispute_daily",
    schedule="0 10 * * *",
    start_date=pendulum.datetime(2024, 2, 4, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["finance", "disputes", "mart"],
    doc_md=__doc__,
) as dag:
    build = BashOperator(
        task_id="build",
        bash_command=(
            "/opt/dbt-venv/bin/dbt run --target prod --profiles-dir . "
            "--select marts.dispute_daily "
            "--vars '{\"run_date\": \"" + TARGET_DS + "\"}'"
        ),
        cwd=str(DBT_ROOT),
        pool="warehouse_write",
    )

    check = PythonOperator(
        task_id="check",
        python_callable=warehouse_checks.assert_integer_cents,
        op_kwargs={"table": TABLE,
                   "columns": ["disputed_cents", "written_off_cents"]},
    )

    open_count = PythonOperator(
        task_id="open_count",
        python_callable=open_by_entity,
        op_kwargs={"target_ds": TARGET_DS},
        pool="warehouse_write",
    )

    publish = PythonOperator(
        task_id="publish",
        python_callable=warehouse_checks.publish_partition,
        op_kwargs={"table": TABLE, "partition_col": "ds",
                   "partition_value": TARGET_DS, "name": "dispute_daily",
                   "order_by": ["entity", "dispute_state"]},
    )

    build >> check >> open_count >> publish
