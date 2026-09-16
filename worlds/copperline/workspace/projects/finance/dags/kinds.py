"""This team's blueprint kinds.

`include/lib/blueprint/` ships five kinds and is shared code. A kind only one
team uses is registered from here, which is the documented entry point — the
loader imports this file when it renders a `*.dag.yaml` in this folder. See
`docs/blueprints.md`.

One kind. It is the check every finance mart runs after it rebuilds, and it is
here because five rendered DAGs wanted the same four lines of Python.
"""

from __future__ import annotations

from typing import Any

from airflow.providers.standard.operators.python import PythonOperator

from include.lib.blueprint import registry
from projects.finance.lib import warehouse_checks

__all__ = ["cents_check", "run_check"]


def cents_check(step: Any, ctx: Any) -> Any:
    """Check one partition of a finance table: the money, and the shape.

    Keys:
        table         the table to check, `schema.table`. Required.
        columns       the money columns. Every one has to be an integer type.
        partition_col the partition column. `ds` by default.
        partition_value  the partition this run owns. `{{ ds }}` by default.
        at_least      the fewest rows a real day has. 1 by default.

    Two failures, one task, because they are the same question asked twice:
    did this day land, and is it in cents. A float in a money column cannot tie
    to the ledger by construction, and an empty partition is a build that
    succeeded and produced nothing.
    """
    return PythonOperator(
        task_id=step.name,
        python_callable=run_check,
        op_kwargs={
            "table": ctx.render(step["table"]),
            "columns": [ctx.render(column) for column in step.get("columns") or []],
            "partition_col": step.get("partition_col", "ds"),
            "partition_value": ctx.render(step.get("partition_value", "{{ ds }}")),
            "at_least": int(step.get("at_least", 1)),
        },
    )


def run_check(*, table: str, columns: list[str], partition_col: str,
              partition_value: str, at_least: int) -> dict:
    """Run both checks and return what they found.

    Raises on either. The row count is checked first: an empty partition makes
    the type check pass on a table that has nothing in it, and the message
    about the day is the more useful of the two.
    """
    rows = warehouse_checks.assert_row_count(
        table, partition_col, partition_value, at_least=at_least
    )
    checked = warehouse_checks.assert_integer_cents(table, columns)
    return {"rows": rows, "money_columns": checked}


registry.register("cents_check", cents_check)
