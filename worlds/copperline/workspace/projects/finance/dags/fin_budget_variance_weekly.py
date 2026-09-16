"""Plan against actual, every Tuesday.

**This is not budget variance.** `marts.budget_variance_weekly` does not exist
and cannot: there is no budget extract, and a mart with no source is worse than
an absent one. What this publishes is the plan book against actuals — the plan
lines Copperline already holds, which are a commitment a customer signed rather
than a budget finance set — and both the model and the file are named for what
they are.

The DAG keeps the name it was created with because a `dag_id` is a contract
with everything downstream and renaming one loses its history. When a budget
extract lands, the mart gets built and this points at it.

Owned by finance-analytics.
"""

from __future__ import annotations

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.sensors.python import PythonSensor
from airflow.sdk import DAG

from include.lib import workspace_root
from include.lib.notify import notify
from projects.finance.lib import warehouse_checks

DBT_ROOT = workspace_root() / "dbt" / "copperline_analytics"

#: The day this run owns. See `CONVENTIONS.md`.
TARGET_DS = "{{ macros.ds_add(ds, -1) }}"

TABLE = "marts.plan_vs_actual_weekly"
SOURCE = "marts.daily_revenue"

DEFAULT_ARGS = {
    "owner": "finance",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=10),
    "on_failure_callback": notify("finance", "plan against actual did not publish"),
}

with DAG(
    dag_id="fin_budget_variance_weekly",
    schedule="0 10 * * 2",
    start_date=pendulum.datetime(2024, 2, 4, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["finance", "plan", "weekly"],
    doc_md=__doc__,
) as dag:
    wait_for_revenue = PythonSensor(
        task_id="wait_for_revenue",
        python_callable=warehouse_checks.has_partition,
        op_kwargs={"table": SOURCE, "partition_col": "ds",
                   "partition_value": TARGET_DS},
        poke_interval=600,
        timeout=60 * 60 * 3,
        mode="reschedule",
    )

    build = BashOperator(
        task_id="build",
        bash_command=(
            "/opt/dbt-venv/bin/dbt run --target prod --profiles-dir . "
            "--select marts.plan_vs_actual_weekly "
            "--vars '{\"run_date\": \"" + TARGET_DS + "\"}'"
        ),
        cwd=str(DBT_ROOT),
        pool="warehouse_write",
    )

    check = PythonOperator(
        task_id="check",
        python_callable=warehouse_checks.assert_integer_cents,
        op_kwargs={"table": TABLE,
                   "columns": ["plan_cents", "actual_cents", "variance_cents"]},
    )

    publish = PythonOperator(
        task_id="publish",
        python_callable=warehouse_checks.publish_partition,
        op_kwargs={"table": TABLE, "partition_col": "ds",
                   "partition_value": TARGET_DS, "name": "plan_vs_actual_weekly",
                   "order_by": ["entity", "fiscal_week"]},
    )

    wait_for_revenue >> build >> check >> publish
