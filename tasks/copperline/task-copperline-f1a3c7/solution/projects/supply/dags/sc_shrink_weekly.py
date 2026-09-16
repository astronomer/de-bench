"""Shrink by location, for the fiscal week that closed.

Shrink is what the cycle counts found: the difference between what the movement
log says should be in a bin and what somebody counted in it. Corvid calls the
movement a `cycle_count` and the application calls the result shrink; nothing
translates on the way in, so the translation is here.

The week is a fiscal week and it opens on a Sunday. This runs on Thursday and
covers the week that closed the Saturday before, because the counts for a week
are not all posted until the Wednesday after it. `week_start` comes from
`raw.fiscal_calendar`, never from weekday arithmetic — the 4-5-4 calendar
authors its weeks and a five-week period does not divide.

**A count is priced against the position it was counted against.** The network
snapshot is weekly and the A-class one is daily, so most counts fall on a day
`raw.inventory_snapshots` holds no row for. The position on such a day is the
last one taken, carried forward — the same rule `int_inventory_position_daily`
states — which is what the ASOF join below does. Joining on the snapshot date
itself priced one count in seven and filed the other six under no department at
all.

**Which method prices a count is the position's to say.** Copperline valued
stock at weighted-average cost until the first day of FY2026 and by the retail
method after it, and `docs/inventory-policy.md` INV-1 is the only place that is
written down. The position announces the change by going NULL: a row from
before the change carries `unit_cost_cents` and a row from after it carries
`retail_value_cents` and `cost_complement_bps` instead. So the branch reads the
row rather than a date, the way `fct_inventory_valuation` does.

Under the retail method the cost is a department ratio and there is no per-SKU
cost behind it (INV-2). `retail_value_cents` is the retail of everything on
hand at that site that day, so a unit's retail is that over the units on hand.
The department's retail is summed first, the complement is applied once, and
the rounding is the single half-up on that one division (INV-3).

FY2025 is not restated (INV-4). A week that closed before the change prices at
weighted-average cost for ever.

Produces `marts.shrink_weekly`. Read by the loss-prevention pack and the
merchandising weekly.

Owned by supply-chain (K. Duffy).
"""

from __future__ import annotations

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from include.lib import calendar, warehouse
from include.lib.notify import notify

DEFAULT_ARGS = {
    "owner": "supply",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=10),
}

#: How long after a week closes the counts for it are all posted.
COUNT_LAG_DAYS = 5

PACK_SCHEMA = "CREATE SCHEMA IF NOT EXISTS copperline.marts"

PACK_TABLE = """
CREATE TABLE IF NOT EXISTS copperline.marts.shrink_weekly (
    week_start DATE NOT NULL,
    location_id VARCHAR NOT NULL,
    dept_code VARCHAR NOT NULL,
    counted_delta DECIMAL(12, 3) NOT NULL,
    shrink_cents BIGINT NOT NULL
)
"""

SHRINK = """
INSERT INTO copperline.marts.shrink_weekly BY NAME
WITH counts AS (
    SELECT m.sku,
           m.from_location            AS location_id,
           m.qty,
           m.occurred_at::DATE        AS counted_on
    FROM copperline.raw.wms_movements m
    WHERE m.movement_type = 'cycle_count'
      AND m.occurred_at::DATE >= ?::DATE
      AND m.occurred_at::DATE < ?::DATE + 7
),
positioned AS (
    -- The position in force when the count was taken: the last snapshot on or
    -- before the day, per SKU and site. The snapshot cadence is weekly for the
    -- tail, so an equality join here prices one count in seven.
    SELECT c.location_id,
           s.dept_code,
           c.qty,
           s.unit_cost_cents,
           s.retail_value_cents,
           s.on_hand_units,
           s.cost_complement_bps
    FROM counts c
    ASOF LEFT JOIN copperline.raw.inventory_snapshots s
      ON s.sku = c.sku
     AND s.location_id = c.location_id
     AND s.snapshot_date <= c.counted_on
)
SELECT ?::DATE AS week_start,
       location_id,
       dept_code,
       sum(qty) AS counted_delta,
       CASE
           -- Weighted-average cost, stated per unit on the position row.
           WHEN max(cost_complement_bps) IS NULL
           THEN cast(sum(qty * unit_cost_cents) AS BIGINT)
           -- The retail method. Sum the department's retail, apply the
           -- complement once, round half up on that division. INV-3.
           ELSE cast(
               (cast(sum(qty * (retail_value_cents / on_hand_units)) AS DECIMAL(38, 0))
                * cast(max(cost_complement_bps) AS DECIMAL(38, 0)) + 5000) / 10000
               AS BIGINT)
       END AS shrink_cents
FROM positioned
GROUP BY location_id, dept_code
"""


def week_covered(ds: str) -> str:
    """The Sunday that opened the week this run covers."""
    closed = pendulum.parse(ds).subtract(days=COUNT_LAG_DAYS).date()
    return calendar.week_start(closed).isoformat()


def create_pack() -> str:
    """The pack's table, if the warehouse does not have it yet.

    Qualified on purpose: an unqualified `marts` resolves against the frozen
    `nwv` copy first, which is read-only and raises.
    """
    table = warehouse.qualify("marts.shrink_weekly")
    with warehouse.connect() as con:
        con.execute(PACK_SCHEMA)
        con.execute(PACK_TABLE)
    return table


def build_shrink(ds: str) -> int:
    """Replace the covered week's rows."""
    week = week_covered(ds)
    with warehouse.connect() as con:
        con.execute(f"DELETE FROM {warehouse.qualify('marts.shrink_weekly')} "
                    "WHERE week_start = ?", [week])
        return int(con.execute(SHRINK, [week, week, week]).fetchone()[0])


def check_counts_posted(ds: str) -> int:
    """Every location that counted in the week has a row.

    A location whose counts posted late is a location whose shrink reads zero,
    which is the one number nobody queries.
    """
    week = week_covered(ds)
    with warehouse.connect(read_only=True) as con:
        missing = int(con.execute(
            "SELECT count(*) FROM ("
            "  SELECT DISTINCT from_location FROM "
            f"  {warehouse.qualify('raw.wms_movements')}"
            "   WHERE movement_type = 'cycle_count'"
            "     AND occurred_at::DATE >= ?::DATE"
            "     AND occurred_at::DATE < ?::DATE + 7"
            "  EXCEPT"
            f"  SELECT location_id FROM {warehouse.qualify('marts.shrink_weekly')}"
            "   WHERE week_start = ?::DATE)",
            [week, week, week],
        ).fetchone()[0])
    if missing:
        raise ValueError(
            f"week of {week}: {missing} locations counted and have no shrink row"
        )
    return missing


def report_worst_locations(ds: str) -> list[str]:
    """The five locations that lost most, for loss prevention."""
    week = week_covered(ds)
    with warehouse.connect(read_only=True) as con:
        rows = con.execute(
            f"SELECT location_id, shrink_cents FROM "
            f"{warehouse.qualify('marts.shrink_weekly')} "
            "WHERE week_start = ?::DATE ORDER BY shrink_cents LIMIT 5", [week],
        ).fetchall()
    return [f"{location}: {cents} cents" for location, cents in rows]


with DAG(
    dag_id="sc_shrink_weekly",
    schedule="0 9 * * 4",
    start_date=pendulum.datetime(2024, 2, 4, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["supply", "mart", "inventory"],
    doc_md=__doc__,
    on_failure_callback=notify("supply", "weekly shrink"),
) as dag:
    create = PythonOperator(
        task_id="create_pack", python_callable=create_pack)
    build = PythonOperator(
        task_id="build_shrink", python_callable=build_shrink,
        op_kwargs={"ds": "{{ ds }}"})
    posted = PythonOperator(
        task_id="check_counts_posted", python_callable=check_counts_posted,
        op_kwargs={"ds": "{{ ds }}"})
    worst = PythonOperator(
        task_id="report_worst_locations", python_callable=report_worst_locations,
        op_kwargs={"ds": "{{ ds }}"})
    week = PythonOperator(
        task_id="report_week_covered", python_callable=week_covered,
        op_kwargs={"ds": "{{ ds }}"})

    week >> create >> build >> posted >> worst
