"""`partition_export` — write one partition of a table out as a file."""

from __future__ import annotations

from typing import Any

from airflow.providers.standard.operators.python import PythonOperator

from ... import warehouse
from ..registry import register

__all__ = ["partition_export", "run"]


def partition_export(step: Any, ctx: Any) -> Any:
    """Build the export.

    Keys:
        source        the table to read, `schema.table`. Required.
        layer         the directory under `include/data/`. `marts` by default.
        name          the file's name before the date. Defaults to the source
                      table's bare name.
        columns       the columns to write, in order. Every column by default.
        order_by      the columns to sort by. The file is unordered without it,
                      and a diff between two runs is then noise.
        partition_col the column that scopes the read. `ds` by default.
        partition_value  the partition this run owns. `{{ ds }}` by default.
        where         an extra filter, templated.

    The file lands at `include/data/<layer>/<name>_<ds>.csv`, which is the one
    layout the platform's readers know. TWO SENSORS IN `projects/supply/` MATCH
    THESE FILES BY NAME PATTERN rather than by task, so renaming an export or
    moving its layer leaves a DAG waiting on a file that will never arrive:
    no error, no failed task, a run that sits in `running` until its timeout.
    `docs/lineage.md` lists who reads what.
    """
    source = ctx.render(step["source"])
    return PythonOperator(
        task_id=step.name,
        python_callable=run,
        op_kwargs={
            "source": source,
            "layer": step.get("layer", "marts"),
            "name": ctx.render(step.get("name") or source.rpartition(".")[2]),
            "columns": list(step.get("columns") or []),
            "order_by": list(step.get("order_by") or []),
            "partition_col": step.get("partition_col", "ds"),
            "partition_value": ctx.render(step.get("partition_value", "{{ ds }}")),
            "where": ctx.render(step.get("where")),
        },
    )


def run(*, source: str, layer: str, name: str, columns: list[str],
        order_by: list[str], partition_col: str, partition_value: str,
        where: str | None = None) -> str:
    """Read one partition and publish it. Returns the path written."""
    selected = ", ".join(columns) if columns else "*"
    filters = [f"{partition_col} = '{partition_value}'"]
    if where:
        filters.append(f"({where})")
    order = f" ORDER BY {', '.join(order_by)}" if order_by else ""
    with warehouse.connect(read_only=True) as con:
        result = con.execute(
            f"SELECT {selected} FROM {warehouse.qualify(source)} "
            f"WHERE {' AND '.join(filters)}{order}"
        )
        header = [description[0] for description in result.description]
        rows = result.fetchall()
    return str(warehouse.write_partition(layer, name, partition_value, header, rows))


register("partition_export", partition_export)
