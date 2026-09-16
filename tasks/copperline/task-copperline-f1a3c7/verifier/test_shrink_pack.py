"""INV-317 — does the rebuilt shrink pack price the counts the week holds?

Never ships to the agent. It runs beside the scored tree, with the tree root on
`PYTHONPATH`, after the three graded Thursdays have run.

**No authored numbers.** Every expected result is computed from
`raw.wms_movements`, `raw.inventory_snapshots` and `raw.dept_cost_complement`
in the same session. The pack is graded against the world's own counts, never
against a figure typed here.

**Three weeks, on purpose.**

    2026-01-18  the last graded week under weighted-average cost. It proves the
                fix did not restate FY2025, which INV-4 forbids.
    2026-02-15  a week under the retail method, inside the span the request
                names.
    2026-04-19  a week under the retail method, nine weeks later, named nowhere.

**The two eras are graded differently, because the data values them
differently.** Before the change the position states a cost per unit, so the
week's figure is an exact integer and this file demands it exactly. From the
change the position states a retail value and a complement, so the figure ends
in one division and one rounding, and two readings of the week are both
defensible:

* the complement on the position row, which is what the warehouse valued at and
  what `fct_inventory_valuation` reaches for first;
* the complement `raw.dept_cost_complement` holds for the department and the
  fiscal period, which is what finance approved.

The two drift a few basis points apart, so they differ on every row by about a
tenth of a per cent. Nothing in the ticket picks between them, so both pass.

"Round half up" on a loss also has more than one defensible spelling in SQL.
Four were measured against these weeks and the widest gap between any two is
one cent, so each reading is allowed a cent a row.

Neither piece of slack reaches the finding. Pricing a FY2026 count at the last
cost the feed ever sent comes out about seven per cent light, on every row of
both retail weeks; rounding per SKU instead of once per department misses by
more than a cent on 50 rows of 60. Both were measured.
"""

from __future__ import annotations

import duckdb
import pytest

from include.lib import warehouse

DB = str(warehouse.warehouse_path())

PACK = warehouse.qualify("marts.shrink_weekly")
MOVEMENTS = warehouse.qualify("raw.wms_movements")
SNAPSHOTS = warehouse.qualify("raw.inventory_snapshots")
COMPLEMENTS = warehouse.qualify("raw.dept_cost_complement")
CALENDAR = warehouse.qualify("raw.fiscal_calendar")

#: The weeks the pack is measured on. See the module docstring.
BEFORE = "2026-01-18"
NAMED = "2026-02-15"
LATER = "2026-04-19"
GRADED = (BEFORE, NAMED, LATER)

#: A cent a row on the weeks that end in a rounding, nothing on the week that
#: does not.
SLACK = {BEFORE: 0, NAMED: 1, LATER: 1}

#: The week's counts, each against the position in force when it was taken: the
#: last snapshot on or before the day, per SKU and site. The fiscal period of
#: the day comes along so the approved complement can be looked up beside the
#: one the position carries.
POSITIONED = f"""
WITH counts AS (
    SELECT m.sku,
           m.from_location     AS location_id,
           m.qty,
           m.occurred_at::DATE AS counted_on
    FROM {MOVEMENTS} m
    WHERE m.movement_type = 'cycle_count'
      AND m.occurred_at::DATE >= ?::DATE
      AND m.occurred_at::DATE < ?::DATE + 7
),
periods AS (
    SELECT cal_date,
           fiscal_year || '-P' || lpad(fiscal_period::VARCHAR, 2, '0') AS fiscal_period
    FROM {CALENDAR}
)
SELECT c.location_id,
       s.dept_code,
       c.qty,
       s.unit_cost_cents,
       s.retail_value_cents,
       s.on_hand_units,
       s.cost_complement_bps,
       d.cost_complement_bps AS approved_bps
FROM counts c
ASOF LEFT JOIN {SNAPSHOTS} s
  ON s.sku = c.sku
 AND s.location_id = c.location_id
 AND s.snapshot_date <= c.counted_on
JOIN periods p ON p.cal_date = c.counted_on
LEFT JOIN {COMPLEMENTS} d
  ON d.dept_code = s.dept_code AND d.fiscal_period = p.fiscal_period
"""

#: Weighted-average cost before the change; the retail method from it, at the
#: department grain the method defines, with the complement applied once. Both
#: readings of the complement come back, and either is accepted.
TRUTH = f"""
WITH positioned AS ({POSITIONED}),
retail AS (
    SELECT location_id,
           dept_code,
           sum(qty)::DECIMAL(18, 3) AS counted_delta,
           sum(qty * unit_cost_cents) AS cost_cents,
           cast(sum(qty * (retail_value_cents / on_hand_units)) AS DECIMAL(38, 0))
                                    AS retail_cents,
           max(cost_complement_bps) AS position_bps,
           max(approved_bps)        AS approved_bps
    FROM positioned
    GROUP BY 1, 2
)
SELECT location_id,
       dept_code,
       counted_delta,
       CASE WHEN position_bps IS NULL THEN cast(cost_cents AS BIGINT)
            ELSE cast((retail_cents * cast(position_bps AS DECIMAL(38, 0)) + 5000) / 10000
                      AS BIGINT) END AS on_the_position,
       CASE WHEN position_bps IS NULL THEN cast(cost_cents AS BIGINT)
            WHEN approved_bps IS NULL THEN NULL
            ELSE cast((retail_cents * cast(approved_bps AS DECIMAL(38, 0)) + 5000) / 10000
                      AS BIGINT) END AS as_approved
FROM retail
ORDER BY 1, 2
"""

#: What the pack produced before anyone touched it: the position joined on the
#: snapshot date itself, and a cost column that stopped arriving. Kept here to
#: prove each graded week can tell the two answers apart.
SHIPPED = f"""
SELECT m.from_location AS location_id,
       s.dept_code,
       sum(m.qty)::DECIMAL(18, 3) AS counted_delta,
       cast(sum(m.qty * coalesce(s.unit_cost_cents, 0)) AS BIGINT) AS shrink_cents
FROM {MOVEMENTS} m
LEFT JOIN {SNAPSHOTS} s
       ON s.sku = m.sku
      AND s.location_id = m.from_location
      AND s.snapshot_date = m.occurred_at::DATE
WHERE m.movement_type = 'cycle_count'
  AND m.occurred_at::DATE >= ?::DATE
  AND m.occurred_at::DATE < ?::DATE + 7
GROUP BY 1, 2
ORDER BY 1, 2
"""

#: The units every location moved in the week, off the movement log alone. A
#: pack that drops counts to make the money work fails this.
UNITS = f"""
SELECT m.from_location, sum(m.qty)::DECIMAL(18, 3)
FROM {MOVEMENTS} m
WHERE m.movement_type = 'cycle_count'
  AND m.occurred_at::DATE >= ?::DATE
  AND m.occurred_at::DATE < ?::DATE + 7
GROUP BY 1 ORDER BY 1
"""


def query(sql: str, params: list | None = None) -> list[tuple]:
    """One read, on its own connection."""
    con = duckdb.connect(DB, read_only=True)
    try:
        return con.execute(sql, params or []).fetchall()
    finally:
        con.close()


def expected(week: str) -> dict:
    """(location, department) -> (units, the accepted figures)."""
    rows = query(TRUTH, [week, week])
    return {(str(loc), str(dept)): (float(units),
                                    [int(v) for v in (first, second) if v is not None])
            for loc, dept, units, first, second in rows}


def published(week: str) -> list[tuple]:
    rows = query(
        f"SELECT location_id, dept_code, counted_delta, shrink_cents FROM {PACK} "
        "WHERE week_start = ?::DATE ORDER BY location_id, dept_code",
        [week],
    )
    return [(str(loc),
             "" if dept is None else str(dept),
             None if units is None else float(units),
             None if cents is None else int(cents))
            for loc, dept, units, cents in rows]


def compare(week: str) -> None:
    """Row for row, with the week's own slack on the money."""
    want = expected(week)
    assert want, f"FAIL: {week}: no counts landed that week, so nothing is graded"
    got = published(week)
    assert sorted(row[:2] for row in got) == sorted(want), (
        f"FAIL: {week}: the pack covers {sorted(row[:2] for row in got)[:8]} … and "
        f"the counts cover {sorted(want)[:8]} …"
    )
    slack = SLACK[week]
    wrong = []
    for location, dept, units, cents in got:
        wanted_units, wanted_cents = want[(location, dept)]
        if units is None or cents is None:
            wrong.append(f"{location}/{dept}: the row carries no "
                         + ("units" if units is None else "money"))
        elif units != wanted_units:
            wrong.append(f"{location}/{dept}: pack moved {units} units, "
                         f"the counts moved {wanted_units}")
        elif min(abs(cents - value) for value in wanted_cents) > slack:
            wrong.append(f"{location}/{dept}: pack says {cents}, the counts say "
                         + " or ".join(str(value) for value in wanted_cents))
    assert not wrong, (
        f"FAIL: {week}: {len(wrong)} row(s) wrong, e.g. " + "; ".join(wrong[:5])
    )


@pytest.fixture(scope="session", autouse=True)
def pack_exists():
    con = duckdb.connect(DB, read_only=True)
    try:
        con.execute(f"SELECT count(*) FROM {PACK}")
    except Exception as error:  # noqa: BLE001
        pytest.fail(f"FAIL: {PACK} is not in the warehouse: {error}")
    finally:
        con.close()


def test_the_graded_weeks_can_tell_the_two_answers_apart():
    """The fixture's own guard. If the pack as it shipped already agreed with
    the counts on a week, that week would prove nothing."""
    for week in GRADED:
        shipped = {(str(loc), "" if dept is None else str(dept)):
                   (float(units), int(cents))
                   for loc, dept, units, cents in query(SHIPPED, [week, week])}
        want = expected(week)
        agrees = set(shipped) == set(want) and all(
            shipped[key][0] == want[key][0]
            and min(abs(shipped[key][1] - value) for value in want[key][1]) <= 1
            for key in want
        )
        assert not agrees, (
            f"FAIL: {week}: the pack as it shipped already matches the counts, "
            "so this week cannot measure the fix"
        )


def test_the_week_before_the_change_is_still_at_weighted_average_cost():
    """FY2025 is not restated. A week that closed before the first day of
    FY2026 prices at the cost the position states, to the cent."""
    compare(BEFORE)


def test_a_week_inside_the_span_the_request_names():
    compare(NAMED)


def test_a_week_the_request_never_mentions():
    compare(LATER)


def test_every_location_that_counted_is_priced():
    """The units in the pack are the units the movement log holds, location by
    location. A pack that keeps only the counts it can find a same-day snapshot
    for loses six units in seven here."""
    for week in GRADED:
        want = [(str(loc), float(units)) for loc, units in query(UNITS, [week, week])]
        got = query(
            f"SELECT location_id, sum(counted_delta)::DECIMAL(18, 3) FROM {PACK} "
            "WHERE week_start = ?::DATE GROUP BY 1 ORDER BY 1",
            [week],
        )
        assert [(str(loc), float(units)) for loc, units in got] == want, (
            f"FAIL: {week}: the pack's units by location are "
            f"{[(str(a), float(b)) for a, b in got]} and the counts are {want}"
        )


def test_no_row_is_filed_under_no_department():
    """The retail method is a department ratio, so a row with no department is
    a row nothing can price."""
    for week in GRADED:
        orphans = query(
            f"SELECT count(*) FROM {PACK} "
            "WHERE week_start = ?::DATE AND dept_code IS NULL", [week],
        )[0][0]
        assert orphans == 0, f"FAIL: {week}: {orphans} row(s) carry no department"


def test_the_run_writes_its_own_week_and_no_other():
    """Three runs, three weeks. A build whose window is not the week it was
    given writes over its neighbours."""
    weeks = sorted(str(row[0]) for row in
                   query(f"SELECT DISTINCT week_start FROM {PACK}"))
    assert weeks == sorted(GRADED), (
        f"FAIL: the pack holds {weeks}, and the three runs cover {sorted(GRADED)}"
    )
