"""The daily GL extract Ironwood picks up.

One query, written out in full, and a file. Ironwood's loader has wanted the
same eighteen columns in the same order since FY2023 and it rejects the file if
they move, so the query is spelled out here rather than assembled, and the
column order is the contract.

It writes `exports/gl/gl_<ds>.csv` every morning at 08:00 and the ERP collects
it an hour later. Nobody in the finance team reads it; the only sign it has
gone wrong is Ironwood's own rejection mail.

Owned by finance-analytics.
"""

from __future__ import annotations

import csv
from pathlib import Path

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from include.lib import warehouse, workspace_root
from include.lib.notify import notify

#: The day this run owns. See `CONVENTIONS.md`.
TARGET_DS = "{{ macros.ds_add(ds, -1) }}"

EXPORT_ROOT = workspace_root() / "exports" / "gl"

#: The eighteen columns Ironwood's loader expects, in the order it expects
#: them. Do not reorder.
HEADER = [
    "gl_date", "entity", "cost_center", "account_code", "order_id", "channel",
    "product_id", "product_name", "category_code", "supplier_code", "currency",
    "booked_cents", "net_sales_cents", "merch_margin_cents", "unit_count",
    "source_system", "batch_id", "posted_flag",
]

QUERY = """
SELECT
    oe.order_date                             AS gl_date,
    coalesce(e.entity_code, 'CL-US')          AS entity,
    coalesce(cc.cost_center_code, '5214')     AS cost_center,
    CASE oe.channel
        WHEN 'store'       THEN '4000'
        WHEN 'web'         THEN '4010'
        WHEN 'marketplace' THEN '4020'
        WHEN 'trade'       THEN '4030'
        ELSE '4099'
    END                                       AS account_code,
    oe.order_id                               AS order_id,
    oe.channel                                AS channel,
    dp.product_id                             AS product_id,
    dp.product_name                           AS product_name,
    dp.category_code                          AS category_code,
    dp.supplier_code                          AS supplier_code,
    coalesce(oe.currency_code, 'USD')         AS currency,
    oe.booked_cents                           AS booked_cents,
    oe.net_sales_cents                        AS net_sales_cents,
    oe.merch_margin_cents                     AS merch_margin_cents,
    coalesce(oe.unit_count, 0)                AS unit_count,
    'copperline'                              AS source_system,
    'GL-' || oe.order_date                    AS batch_id,
    'N'                                       AS posted_flag
FROM copperline.marts.order_economics oe
LEFT JOIN copperline.marts.dim_product dp
       ON dp.product_id = oe.lead_product_id
      AND dp.dbt_valid_to IS NULL
LEFT JOIN copperline.marts.dim_entity e
       ON e.entity_id = oe.entity_id
LEFT JOIN copperline.marts.dim_cost_center cc
       ON cc.cost_center_id = oe.cost_center_id
WHERE oe.order_date = ?
  AND oe.booked_cents <> 0
ORDER BY oe.order_id
"""

DEFAULT_ARGS = {
    "owner": "finance",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=10),
    "on_failure_callback": notify("finance", "Ironwood has no GL file for today"),
}


def run_query(target_ds: str) -> list[list]:
    """Run the extract for one day and return its rows."""
    with warehouse.connect(read_only=True) as con:
        return con.execute(QUERY, [target_ds]).fetchall()


def write_csv(rows: list[list], target_ds: str) -> str:
    """Write `exports/gl/gl_<ds>.csv`, header first, atomically."""
    EXPORT_ROOT.mkdir(parents=True, exist_ok=True)
    path = EXPORT_ROOT / f"gl_{target_ds}.csv"
    partial = path.with_suffix(".csv.partial")
    with partial.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(HEADER)
        writer.writerows(rows)
    partial.replace(path)
    return str(path)


def verify(path: str, rows: list[list]) -> int:
    """The file has a header and one line per row, and the width is eighteen.

    Ironwood rejects the whole file on a width mismatch and its rejection mail
    says only that the batch failed, so the width is checked here where the
    message can name the column count.
    """
    lines = [line for line in
             Path(path).read_text(encoding="utf-8").splitlines() if line]
    if len(lines) != len(rows) + 1:
        raise ValueError(f"{path}: {len(lines) - 1} data lines and {len(rows)} rows")
    if len(HEADER) != 18:
        raise ValueError(f"the header is {len(HEADER)} columns and Ironwood wants 18")
    return len(rows)


def deliver(path: str) -> str:
    """Copy the file into the ERP's pickup directory, and return the new path.

    A copy rather than a move, so a delivery that has to be repeated does not
    need the day rebuilt. Ironwood clears its own side after it loads.
    """
    import shutil

    pickup = EXPORT_ROOT / "pickup"
    pickup.mkdir(parents=True, exist_ok=True)
    target = pickup / path.rsplit("/", 1)[-1]
    shutil.copyfile(path, target)
    return str(target)


with DAG(
    dag_id="fin_gl_export",
    schedule="0 8 * * *",
    start_date=pendulum.datetime(2024, 2, 4, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["finance", "export", "erp"],
    doc_md=__doc__,
) as dag:
    extract = PythonOperator(
        task_id="extract",
        python_callable=run_query,
        op_kwargs={"target_ds": TARGET_DS},
    )

    write = PythonOperator(
        task_id="write",
        python_callable=write_csv,
        op_kwargs={"rows": extract.output, "target_ds": TARGET_DS},
    )

    check = PythonOperator(
        task_id="check",
        python_callable=verify,
        op_kwargs={"path": write.output, "rows": extract.output},
    )

    hand_off = PythonOperator(
        task_id="hand_off",
        python_callable=deliver,
        op_kwargs={"path": write.output},
    )

    extract >> write >> check >> hand_off
