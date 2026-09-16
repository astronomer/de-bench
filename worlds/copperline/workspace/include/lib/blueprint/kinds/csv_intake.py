"""`csv_intake` — land a dated CSV from the landing tree into a table."""

from __future__ import annotations

from typing import Any

from ...loaders import CsvToWarehouseOperator
from ..registry import register

__all__ = ["csv_intake", "source_path"]

#: A source that starts with one of these is a path in the repository already.
_ROOTED = ("landing/", "include/", "exports/", "fixtures/", "dbt/", "legacy/")


def csv_intake(step: Any, ctx: Any) -> Any:
    """Build the CSV load.

    Keys:
        source        the file or glob to load. `.csv` is added when there is
                      no suffix, and a relative path resolves under `landing/`,
                      so `comp_prices_{{ ds }}` reads
                      `landing/comp_prices_{{ ds }}.csv` and
                      `oms/dt={{ ds }}/orders.csv` reads
                      `landing/oms/dt={{ ds }}/orders.csv`. A path that already
                      starts at the repository root, such as `include/data/...`,
                      is used as it is.
        table         `schema.table` to load into. Required.
        mode          "append" (the operator's default) or "replace".
        partition_col the column a replace deletes on. Required for replace.
        partition_value  the partition the replace owns. `{{ ds }}` by default.
        columns       explicit column types, `{invoice_date: DATE}`.

    The append default is the operator's, not this kind's — see
    `CsvToWarehouseOperator`, which is where the rerun behaviour is written
    down. A step that loads a dated file and does not say `mode: replace`
    doubles that day's rows when it runs twice.
    """
    passed = {key: step[key] for key in ("mode", "partition_col", "columns")
              if key in step}
    if "partition_value" in step:
        passed["partition_value"] = ctx.render(step["partition_value"])
    return CsvToWarehouseOperator(
        task_id=step.name,
        source=source_path(ctx.render(step["source"])),
        table=ctx.render(step["table"]),
        **passed,
    )


def source_path(source: str) -> str:
    """A blueprint `source` as a path under the repository root."""
    if source.startswith("/") or source.startswith(_ROOTED):
        return source
    suffix = "" if "." in source.rsplit("/", 1)[-1] else ".csv"
    return f"landing/{source}{suffix}"


register("csv_intake", csv_intake)
