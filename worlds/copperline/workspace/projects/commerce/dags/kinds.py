"""Commerce's own blueprint kinds.

`include/lib/blueprint/` is shared code and the five kinds in it are the ones
every team uses. This file registers the one commerce needs on top of them,
through the entry point `docs/blueprints.md` documents. The factory imports this
module when it renders a `.dag.yaml` in this directory.

`sql_file` is here rather than in the shared package because it is commerce's
convention that it serves: SQL lives in files, one statement each, under
`projects/commerce/sql/`. No other team keeps its SQL that way, so no other team
would use this kind.

Registration is guarded because the factory imports this module by its dotted
name and the DAG parser imports the same file again on its own. Registering the
same name twice with two objects is an error, and it would break the DAG that
did nothing wrong.
"""

from __future__ import annotations

from typing import Any

from airflow.providers.standard.operators.python import PythonOperator

from include.lib import warehouse
from include.lib.blueprint import registry
from projects.commerce.lib import sql


def sql_file(step: Any, ctx: Any) -> Any:
    """Run one statement from `projects/commerce/sql/`.

    Keys:
        statement     the file's name without `.sql`. Required.
        params        values bound to the statement, in order. Templated, so
                      `["{{ ds }}"]` reaches the statement as the run's own day.
                      Empty by default.
        expect_rows   true to fail when the statement reports no rows. False by
                      default, because an empty day is an answer for some
                      statements and a fault for others, and the step knows
                      which it is.

    The statement is read when the task runs, not when the DAG is parsed, so a
    corrected statement is picked up by the next run without a redeploy.
    """
    return PythonOperator(
        task_id=step.name,
        python_callable=run,
        op_kwargs={
            "statement": step["statement"],
            "params": [ctx.render(value) for value in step.get("params") or []],
            "expect_rows": bool(step.get("expect_rows", False)),
        },
    )


def run(*, statement: str, params: list, expect_rows: bool) -> int:
    """Execute the statement and return the count it reports.

    Every commerce statement returns one row with one number — the rows it
    wrote — so a step's return value is what a person would have counted.
    """
    with warehouse.connect() as con:
        row = con.execute(sql.read(statement), params).fetchone()
    written = int(row[0]) if row and row[0] is not None else 0
    if expect_rows and not written:
        raise ValueError(f"{statement} wrote no rows for {params}")
    return written


if not registry.registered("sql_file"):
    registry.register("sql_file", sql_file)
