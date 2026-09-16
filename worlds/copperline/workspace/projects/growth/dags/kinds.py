"""Growth's own blueprint kinds.

`include/lib/blueprint/kinds/` holds the five every team uses and is shared
code we drive rather than change. These three are ours, and they are here
because this team renders nearly everything it runs and kept hitting the same
three gaps:

    api_intake    the fixture API pages; `csv_intake` reads files
    seed_file     a dbt seed is a committed CSV, and `dbt_select` will not seed
    export_file   `partition_export` writes under `include/data/`; a consumer
                  outside the platform reads `exports/`

`docs/blueprints.md` says how registration works. `render.py` imports this
file when it renders any `projects/growth/dags/*.dag.yaml`, so nothing has to
import it by hand.

A builder takes `(step, ctx)` and returns ONE operator. The work sits in the
`run_*` function below it, so the operator stays a thin wrapper and the work
can be read without Airflow.
"""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Any

from airflow.providers.standard.operators.python import PythonOperator

from include.lib import warehouse, workspace_root
from include.lib.blueprint import registry
from include.lib.loaders import JsonApiToWarehouseOperator

__all__ = ["api_intake", "seed_file", "export_file", "run_seed_file",
           "run_export_file"]


def api_intake(step: Any, ctx: Any) -> Any:
    """Land a paginated listing endpoint into a warehouse table.

    Keys:
        endpoint      the listing path, no leading slash. Required.
        table         `schema.table` to land into. Required.
        params        query parameters, templated. Do not pass a page token.
        conn_id       the connection holding the API host. `copperline_api`.
        items_key     the response key holding the page's rows. `items`.
        mode          "append" (the operator's default) or "replace".
        partition_col the column a replace deletes on. Required for replace.
        partition_value  the partition this run owns. `{{ ds }}` by default.
        columns       explicit column types.

    The paging is the operator's, not this kind's. Every listing endpoint
    returns at most fifty rows and a `next_page_token`, `total` is always
    null, and `JsonApiToWarehouseOperator` follows the token to the end — see
    its docstring, which is where the semantics are written down.
    """
    passed = {key: step[key] for key in ("conn_id", "items_key", "mode",
                                         "partition_col", "columns")
              if key in step}
    if "partition_value" in step:
        passed["partition_value"] = ctx.render(step["partition_value"])
    return JsonApiToWarehouseOperator(
        task_id=step.name,
        endpoint=ctx.render(step["endpoint"]),
        table=ctx.render(step["table"]),
        params=ctx.render(step.get("params") or {}),
        **passed,
    )


def seed_file(step: Any, ctx: Any) -> Any:
    """Write a dbt seed CSV from a warehouse table.

    Keys:
        source     the table to read, `schema.table`. Required.
        name       the seed's name, so `markets` writes `seeds/markets.csv`.
                   Required.
        project    the dbt project under `dbt/`. `copperline_analytics`.
        columns    a mapping of source column to seed column, in the order the
                   seed carries them. Required — a seed's header is part of
                   its contract and is never taken from whatever the table
                   happens to hold.
        where      a filter on the source, so that the seed can hold fewer
                   rows than the table. Templated.
        order_by   the sort, so that two rebuilds produce the same file and a
                   diff of the seed is a real change.

    A seed is a committed file. This kind rewrites it; loading it into the
    warehouse is `dbt build --select <name>`, because `dbt_select` refuses to
    run `dbt seed` on its own.
    """
    columns = dict(step["columns"])
    return PythonOperator(
        task_id=step.name,
        python_callable=run_seed_file,
        op_kwargs={
            "source": ctx.render(step["source"]),
            "name": ctx.render(step["name"]),
            "project": step.get("project", "copperline_analytics"),
            "columns": columns,
            "where": ctx.render(step.get("where")),
            "order_by": list(step.get("order_by") or []),
        },
    )


def export_file(step: Any, ctx: Any) -> Any:
    """Write one partition of a table under `exports/`, for a reader outside
    the platform.

    Keys:
        source        the table to read, `schema.table`. Required.
        directory     the folder under `exports/`. Required.
        name          the file's name before `.csv`. Defaults to the table's
                      bare name.
        split_by      a column to split on, one file per value, named after
                      the value. Without it one file holds the partition.
        columns       the columns to write, in order. Every column by default.
        partition_col the column that scopes the read. `ds` by default.
        partition_value  the partition this run owns. `{{ ds }}` by default.
        where         an extra filter, templated. The audience files use it to
                      hold consent, per `contracts/audience-sync.md` AS-3.
        order_by      the sort. Without it a diff between two runs is noise.

    Files land at `exports/<directory>/<partition_value>/<name>.csv`.
    `partition_export` writes under `include/data/`, which is where the
    platform's own readers look; this writes where a vendor, a modelling team
    or an ad platform picks a file up.
    """
    source = ctx.render(step["source"])
    return PythonOperator(
        task_id=step.name,
        python_callable=run_export_file,
        op_kwargs={
            "source": source,
            "directory": ctx.render(step["directory"]),
            "name": ctx.render(step.get("name") or source.rpartition(".")[2]),
            "split_by": step.get("split_by"),
            "columns": list(step.get("columns") or []),
            "partition_col": step.get("partition_col", "ds"),
            "partition_value": ctx.render(step.get("partition_value", "{{ ds }}")),
            "where": ctx.render(step.get("where")),
            "order_by": list(step.get("order_by") or []),
        },
    )


def run_seed_file(*, source: str, name: str, project: str,
                  columns: dict[str, str], where: str | None,
                  order_by: list[str]) -> str:
    """Rewrite `dbt/<project>/seeds/<name>.csv`. Returns the path."""
    selected = ", ".join(f"{table_column} AS {seed_column}"
                         for table_column, seed_column in columns.items())
    order = f" ORDER BY {', '.join(order_by)}" if order_by else ""
    with warehouse.connect(read_only=True) as con:
        rows = con.execute(
            f"SELECT {selected} FROM {warehouse.qualify(source)}"
            + (f" WHERE {where}" if where else "")
            + order
        ).fetchall()
    path = workspace_root() / "dbt" / project / "seeds" / f"{name}.csv"
    _write_csv(path, list(columns.values()), rows)
    return str(path)


def run_export_file(*, source: str, directory: str, name: str,
                    split_by: str | None, columns: list[str],
                    partition_col: str, partition_value: str,
                    where: str | None, order_by: list[str]) -> list[str]:
    """Write the partition under `exports/`. Returns the paths written."""
    selected = ", ".join(columns) if columns else "*"
    order = f" ORDER BY {', '.join(order_by)}" if order_by else ""
    filters = [f"{partition_col} = '{partition_value}'"]
    if where:
        filters.append(f"({where})")
    root = workspace_root() / "exports" / directory / partition_value
    with warehouse.connect(read_only=True) as con:
        result = con.execute(
            f"SELECT {selected} FROM {warehouse.qualify(source)} "
            f"WHERE {' AND '.join(filters)}{order}"
        )
        header = [description[0] for description in result.description]
        rows = result.fetchall()
    if not split_by:
        return [str(_write_csv(root / f"{name}.csv", header, rows))]
    at = header.index(split_by)
    groups: dict[Any, list] = {}
    for row in rows:
        groups.setdefault(row[at], []).append(row)
    return [str(_write_csv(root / f"{key}.csv", header, group))
            for key, group in sorted(groups.items(), key=lambda item: str(item[0]))]


def _write_csv(path: Path, header: list[str], rows: list) -> Path:
    """Write a CSV atomically, so a reader never sees half a file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    partial = path.with_suffix(".csv.partial")
    with partial.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(header)
        writer.writerows(rows)
    partial.replace(path)
    return path


registry.register("api_intake", api_intake)
registry.register("seed_file", seed_file)
registry.register("export_file", export_file)
