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

**Two valuations, and the week decides which one.** `docs/inventory-policy.md`
INV-1: stock was valued at weighted-average cost until 2026-02-01 and by the
retail inventory method from that date. The feed says the same thing and
explains nothing — `unit_cost_cents` stops arriving and `retail_value_cents`
and `cost_complement_bps` start. INV-4 says the earlier weeks are never
restated, so the week being built picks the rule and no week is valued both
ways. The change took effect with FY2026, which opens on a Sunday, so no fiscal
week straddles it.

Under the retail method the counted units are valued at retail and taken down to
cost by the department's complement, which is the only grain a cost is defined
at from here on (INV-2). The snapshot values the whole position rather than one
unit of it, so the retail on a unit is the position's retail over the units in
it. The complement for the period the count fell in comes from
`raw.dept_cost_complement`; the snapshot carries the feed's own copy of it, and
that is the fallback. INV-3 applies the complement once per department and
rounds to the cent there, which is the grain this mart publishes at.

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

#: The first day the retail inventory method applies, from
#: `docs/inventory-policy.md` INV-1. It is the first day of FY2026.
VALUATION_CHANGE = "2026-02-01"

#: The week's counted rows, with the inputs to both valuations beside each one.
#: A count carries a value only where the snapshot ran on the day it was
#: counted; the rest stay at zero, which is SC-1188 and not this job's business.
COUNTED = """
WITH counted AS (
    SELECT m.from_location                   AS location_id,
           s.dept_code                       AS dept_code,
           m.qty                             AS qty,
           m.qty * s.unit_cost_cents         AS at_cost_cents,
           CASE WHEN s.on_hand_units <> 0
                THEN m.qty * (s.retail_value_cents::DECIMAL(38, 6)
                              / s.on_hand_units)
           END                               AS at_retail_cents,
           coalesce(d.cost_complement_bps,
                    s.cost_complement_bps)   AS cost_complement_bps
    FROM copperline.raw.wms_movements m
    LEFT JOIN copperline.raw.inventory_snapshots s
           ON s.sku = m.sku AND s.location_id = m.from_location
          AND s.snapshot_date = m.occurred_at::DATE
    -- One row per department per period, so take the period the count fell in
    -- and not every period that had opened by then.
    LEFT JOIN copperline.raw.dept_cost_complement d
           ON d.dept_code = s.dept_code
          AND d.effective_from = (
              SELECT max(x.effective_from)
              FROM copperline.raw.dept_cost_complement x
              WHERE x.dept_code = s.dept_code
                AND x.effective_from <= m.occurred_at::DATE)
    WHERE m.movement_type = 'cycle_count'
      AND m.occurred_at::DATE >= ?::DATE
      AND m.occurred_at::DATE < ?::DATE + 7
)
"""

#: A week that closed before the change: units times the weighted-average cost
#: the feed carried on the day. Unchanged, so nothing is restated.
SHRINK_AT_COST = "INSERT INTO copperline.marts.shrink_weekly BY NAME " + COUNTED + """
SELECT ?::DATE AS week_start, location_id, dept_code,
       sum(qty)                                AS counted_delta,
       coalesce(sum(at_cost_cents), 0)::BIGINT AS shrink_cents
FROM counted
GROUP BY location_id, dept_code
"""

#: A week from the change on: the counted units at retail, taken down to cost by
#: the department's complement, rounded once per department.
SHRINK_AT_RETAIL = "INSERT INTO copperline.marts.shrink_weekly BY NAME " + COUNTED + """
SELECT ?::DATE AS week_start, location_id, dept_code,
       sum(qty)                                AS counted_delta,
       cast(round(coalesce(sum(at_retail_cents), 0)
                  * coalesce(max(cost_complement_bps), 0) / 10000, 0)
            AS BIGINT)                         AS shrink_cents
FROM counted
GROUP BY location_id, dept_code
"""


def week_covered(ds: str) -> str:
    """The Sunday that opened the week this run covers."""
    closed = pendulum.parse(ds).subtract(days=COUNT_LAG_DAYS).date()
    return calendar.week_start(closed).isoformat()


def build_shrink(ds: str) -> int:
    """Replace the covered week's rows, under the method that week is on."""
    week = week_covered(ds)
    statement = SHRINK_AT_RETAIL if week >= VALUATION_CHANGE else SHRINK_AT_COST
    with warehouse.connect() as con:
        con.execute(f"DELETE FROM {warehouse.qualify('marts.shrink_weekly')} "
                    "WHERE week_start = ?", [week])
        return int(con.execute(statement, [week, week, week]).fetchone()[0])


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

    week >> build >> posted >> worst
