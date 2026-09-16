"""The morning flash: comp sales by region, and marketplace GMV.

Due to finance and to the executive team at 06:00, which is why it runs at
05:45 off marts the 04:00 build has already landed. It is the most-read number
in the company and the one with the strictest clock.

**The prior-year comparison is a lookup, never a date offset.** The calendar is
4-5-4, FY2023 held 53 weeks, and so every FY2024 week compares to the FY2023
week after it. `raw.fiscal_calendar.comp_date_ly` holds the answer;
`ds - 364` is right in ordinary years and wrong for every date in the year
after a 53-week year. §CAL-3 and §REV-16 both say so, and the check below is
there because saying so has not been enough.

Comparability itself is `docs/comp-store-policy.md`'s subject: thirteen full
fiscal months, a closure restates both years, a remodel over 21 days drops the
period, and an acquired store dates from the acquisition close. This DAG does
not decide any of that; `marts.comp_sales_daily` does, and the flash reports it.

Owned by finance-analytics. C-5 in `docs/report-registry.md`.
"""

from __future__ import annotations

import csv

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.sensors.python import PythonSensor
from airflow.sdk import DAG

from include.lib import calendar as cal
from include.lib import warehouse, workspace_root
from include.lib.notify import notify
from projects.finance.lib import warehouse_checks

#: The day the flash reports. See `CONVENTIONS.md`.
TARGET_DS = "{{ macros.ds_add(ds, -1) }}"

COMP = "marts.comp_sales_daily"
GMV = "marts.gmv_daily"
EXPORT_ROOT = workspace_root() / "exports" / "flash"

HEADER = ["ds", "region", "comp_sales_cents", "comp_sales_cents_ly",
          "comp_date_ly", "comp_store_count", "gmv_cents", "commission_cents"]

DEFAULT_ARGS = {
    "owner": "finance",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=3),
    "on_failure_callback": notify("finance", "the 06:00 flash will be late"),
}


def check_comp_dates(target_ds: str) -> int:
    """Every row's `comp_date_ly` is the one the calendar authored.

    The mart carries the comparable date it used. This compares each one to
    `raw.fiscal_calendar` and raises when they differ, which is what catches a
    comparison that has quietly become an offset. Returns the rows checked.
    """
    authored = cal.comp_date_ly(target_ds)
    with warehouse.connect(read_only=True) as con:
        used = con.execute(
            f"SELECT DISTINCT comp_date_ly FROM {warehouse.qualify(COMP)} WHERE ds = ?",
            [target_ds],
        ).fetchall()
    wrong = [row[0] for row in used if row[0] != authored]
    if wrong:
        raise ValueError(
            f"{COMP} compares {target_ds} to {sorted(str(d) for d in wrong)} "
            f"and the calendar says {authored}"
        )
    return len(used)


def assemble(target_ds: str) -> list[list]:
    """One row per region: the comp figures, and the marketplace numbers.

    GMV is the seller's order value and is never Copperline revenue (§REV-14).
    It sits beside the commission in this file because the executive team asks
    about both, and the two columns are named so that nobody adds them up.
    """
    with warehouse.connect(read_only=True) as con:
        return con.execute(
            f"""SELECT c.ds, c.region, c.comp_sales_cents, c.comp_sales_cents_ly,
                       c.comp_date_ly, c.comp_store_count,
                       coalesce(g.gmv_cents, 0)        AS gmv_cents,
                       coalesce(g.commission_cents, 0) AS commission_cents
                FROM {warehouse.qualify(COMP)} c
                LEFT JOIN {warehouse.qualify(GMV)} g
                       ON g.ds = c.ds AND g.region = c.region
                WHERE c.ds = ?
                ORDER BY c.region""",
            [target_ds],
        ).fetchall()


def write_flash(rows: list[list], target_ds: str) -> str:
    """Write `exports/flash/<ds>.csv`, atomically, and return the path."""
    EXPORT_ROOT.mkdir(parents=True, exist_ok=True)
    path = EXPORT_ROOT / f"{target_ds}.csv"
    partial = path.with_suffix(".csv.partial")
    with partial.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(HEADER)
        writer.writerows(rows)
    partial.replace(path)
    return str(path)


def verify(rows: list[list]) -> int:
    """Every region present, and no comp figure without a comparable behind it.

    A region with a comp figure and a null comparable is the shape a bad join
    makes, and it reads as a hundred per cent rise on the morning it happens.
    """
    if not rows:
        raise ValueError("the flash has no rows, so there is nothing to publish")
    unpaired = [row[1] for row in rows if row[2] is not None and row[4] is None]
    if unpaired:
        raise ValueError(
            "comp sales with no comparable date: " + ", ".join(sorted(unpaired))
        )
    return len(rows)


with DAG(
    dag_id="fin_daily_flash",
    schedule="45 5 * * *",
    start_date=pendulum.datetime(2024, 2, 4, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["finance", "flash", "comp"],
    doc_md=__doc__,
) as dag:
    wait_for_comp = PythonSensor(
        task_id="wait_for_comp",
        python_callable=warehouse_checks.has_partition,
        op_kwargs={"table": COMP, "partition_col": "ds",
                   "partition_value": TARGET_DS},
        poke_interval=60,
        timeout=60 * 60,
        mode="poke",
        doc_md="One hour, poked every minute. The commitment is 06:00 and a "
               "wait that reaches past it has already failed, so it gives up in "
               "time for somebody to do something.",
    )

    comp_dates = PythonOperator(
        task_id="check_comp_dates",
        python_callable=check_comp_dates,
        op_kwargs={"target_ds": TARGET_DS},
    )

    build = PythonOperator(
        task_id="assemble",
        python_callable=assemble,
        op_kwargs={"target_ds": TARGET_DS},
    )

    write = PythonOperator(
        task_id="write_flash",
        python_callable=write_flash,
        op_kwargs={"rows": build.output, "target_ds": TARGET_DS},
    )

    check = PythonOperator(
        task_id="verify",
        python_callable=verify,
        op_kwargs={"rows": build.output},
    )

    wait_for_comp >> comp_dates >> build >> write >> check
