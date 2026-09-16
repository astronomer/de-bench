"""`rollup` — group a source table and write the aggregate to a target."""

from __future__ import annotations

from typing import Any

from airflow.providers.standard.operators.python import PythonOperator

from ... import warehouse
from ..registry import BlueprintError, register

__all__ = ["rollup", "run", "AGGREGATES"]

#: The aggregates a rollup may ask for, and the SQL each one becomes.
AGGREGATES = {
    "sum": "sum({column})",
    "count": "count({column})",
    "count_distinct": "count(DISTINCT {column})",
    "avg": "avg({column})",
    "min": "min({column})",
    "max": "max({column})",
    "median": "median({column})",
}


def rollup(step: Any, ctx: Any) -> Any:
    """Build the aggregate.

    Keys:
        source        the table to read, `schema.table`. Required.
        group_by      the columns to group by, a list. Required.
        agg           one of sum, count, count_distinct, avg, min, max, median.
        column        the column the aggregate runs over. Required, and `*` for
                      a plain count.
        target        the table to write, `schema.table`. Required.
        as            the aggregate's column name in the target. Defaults to
                      `<agg>_<column>`.
        partition_col the target's partition column. `ds` by default.
        partition_value  the partition this run owns. `{{ ds }}` by default.
        where         an extra filter on the source, templated.

    THE SOURCE IS READ FOR ONE PARTITION, NOT WHOLE, whenever it has the
    partition column: the run adds `where <partition_col> = <partition_value>`
    for you. A source without that column is read whole, and then the target's
    partition holds an all-time figure, which is almost never what the step
    meant. Say so with `where` if it is.

    The write is `delete_insert` on the target's partition, so running an
    interval twice writes the same rows twice and leaves one copy.
    """
    agg = step.get("agg", "sum")
    if agg not in AGGREGATES:
        raise BlueprintError(
            f"step {step.name!r}: agg is one of {', '.join(sorted(AGGREGATES))}, "
            f"not {agg!r}"
        )
    group_by = list(step["group_by"])
    column = step["column"]
    return PythonOperator(
        task_id=step.name,
        python_callable=run,
        op_kwargs={
            "source": ctx.render(step["source"]),
            "target": ctx.render(step["target"]),
            "group_by": group_by,
            "agg": agg,
            "column": column,
            "as_column": step.get("as") or f"{agg}_{column}".replace("*", "rows"),
            "partition_col": step.get("partition_col", "ds"),
            "partition_value": ctx.render(step.get("partition_value", "{{ ds }}")),
            "where": ctx.render(step.get("where")),
        },
    )


def run(*, source: str, target: str, group_by: list[str], agg: str, column: str,
        as_column: str, partition_col: str, partition_value: str,
        where: str | None = None) -> int:
    """Read one partition of `source`, group it, and replace the same
    partition of `target`. Returns the rows written."""
    with warehouse.connect() as con:
        filters = []
        if _has_column(con, source, partition_col):
            filters.append(f"{partition_col} = '{partition_value}'")
        if where:
            filters.append(f"({where})")
        grouped = ", ".join(group_by)
        sql = (
            f"SELECT {grouped}, {AGGREGATES[agg].format(column=column)} AS {as_column} "
            f"FROM {warehouse.qualify(source)} "
            + (f"WHERE {' AND '.join(filters)} " if filters else "")
            + f"GROUP BY {grouped} ORDER BY {grouped}"
        )
        rows = con.execute(sql).fetchall()
        columns = [*group_by, as_column, partition_col]
        return warehouse.delete_insert(
            target, partition_col, partition_value,
            [(*row, partition_value) for row in rows],
            columns=columns, con=con,
        )


def _has_column(con: Any, table: str, column: str) -> bool:
    schema, _, bare = table.rpartition(".")
    return bool(con.execute(
        "SELECT 1 FROM information_schema.columns "
        "WHERE table_schema = ? AND table_name = ? AND column_name = ?",
        [schema, bare, column],
    ).fetchone())


register("rollup", rollup)
