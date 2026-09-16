"""The four rendered finance reports, built from `config/reports.yml`.

One config entry becomes three tasks: read the mart, write the file, check the
file. The config carries the table name, the columns and the order; the Python
here is a renderer and holds no report-specific logic at all.

The table names in that file are strings, not references, so none of these four
appears in the dbt graph. `docs/lineage.md` carries them under the marts they
read, and that is the only place the connection is written down.

The config is read once, at parse, from a committed file. No network, no
warehouse, no clock — the DAG ids and the task ids are deterministic and a
reviewer can tell from the config alone what the graph will be.

Owned by finance-analytics.
"""

from __future__ import annotations

import csv
from pathlib import Path

import pendulum
import yaml
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from include.lib import warehouse, workspace_root
from include.lib.notify import notify

CONFIG_PATH = workspace_root() / "config" / "reports.yml"

# A committed file, read once at parse.
REPORTS = (yaml.safe_load(CONFIG_PATH.read_text(encoding="utf-8")) or {})["reports"]

#: The day the reports are dated. See `CONVENTIONS.md`.
TARGET_DS = "{{ macros.ds_add(ds, -1) }}"

DEFAULT_ARGS = {
    "owner": "finance",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
    "on_failure_callback": notify("finance", "a rendered report did not publish"),
}


def extract(name: str, spec: dict) -> list[list]:
    """Read one report's rows, in the order the config asks for."""
    columns = ", ".join(spec["columns"])
    order = ", ".join(spec.get("order_by") or spec["columns"])
    with warehouse.connect(read_only=True) as con:
        return con.execute(
            f"SELECT {columns} FROM {warehouse.qualify(spec['reads'])} ORDER BY {order}"
        ).fetchall()


def render(name: str, spec: dict, rows: list[list], target_ds: str) -> str:
    """Write `<lands_at>/<ds>.csv`, atomically, and return the path."""
    root = workspace_root() / spec["lands_at"]
    root.mkdir(parents=True, exist_ok=True)
    path = root / f"{target_ds}.csv"
    partial = path.with_suffix(".csv.partial")
    with partial.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(spec["columns"])
        writer.writerows(rows)
    partial.replace(path)
    return str(path)


def check(name: str, spec: dict, path: str, rows: list[list]) -> int:
    """The file has the header, the rows, and the width the config declares.

    A report published with no rows is the failure this catches. It happens
    when a mart is renamed and the config is not, and the reader downstream
    sees a file arrive on time with nothing in it.
    """
    lines = [line for line in
             Path(path).read_text(encoding="utf-8").splitlines() if line]
    if len(lines) != len(rows) + 1:
        raise ValueError(f"{path}: {len(lines) - 1} data lines and {len(rows)} rows")
    if not rows:
        raise ValueError(f"{name} published no rows from {spec['reads']}")
    return len(rows)


with DAG(
    dag_id="fin_report_publish",
    schedule="0 9 * * *",
    start_date=pendulum.datetime(2024, 2, 4, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["finance", "reports", "export"],
    doc_md=__doc__,
) as dag:
    for _name, _spec in sorted(REPORTS.items()):
        _extract = PythonOperator(
            task_id=f"extract_{_name}",
            python_callable=extract,
            op_kwargs={"name": _name, "spec": _spec},
        )
        _render = PythonOperator(
            task_id=f"render_{_name}",
            python_callable=render,
            op_kwargs={"name": _name, "spec": _spec, "rows": _extract.output,
                       "target_ds": TARGET_DS},
        )
        _check = PythonOperator(
            task_id=f"check_{_name}",
            python_callable=check,
            op_kwargs={"name": _name, "spec": _spec, "path": _render.output,
                       "rows": _extract.output},
        )
        _extract >> _render >> _check
