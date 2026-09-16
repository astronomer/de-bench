"""INV-208 — does the patched valuation step value the days the warehouse holds
a count for, and value them off the live snapshot?

Never ships to the agent. It runs beside the scored tree, with the tree root on
`PYTHONPATH`, so it imports the patched DAG module the same way Airflow would
and calls the same `build_valuation` the `build_valuation` task calls.

**Why a verifier rather than a DAG run.** The world ships the landed half of the
warehouse only (`AGENTS.md`, "The warehouse"): `marts.*` is derived and nothing
is on disk until a run has made it. Standing the whole DAG up also wants the
day's Corvid files, and the landing tree keeps three weeks. The fixture below
makes the one table the valuation writes and calls the build directly, which
grades the same code in seconds and needs no landing file.

**No authored numbers.** Every expected result is computed from
`raw.inventory_snapshots` and `raw.dept_cost_complement` in the same session,
with the same aggregation the shipped build makes of them. The ticket says the
method does not change, so the oracle holds the method still and moves only what
the ticket asks about: which database the snapshot is read from.

**Three days, on purpose.**

    2026-04-15  a daily-count day in FY2026, inside the months finance asked for
    2026-05-12  eleven days after those months, and named nowhere
    2026-01-11  before the FY2026 valuation change, so the cost method applies

A repair that reaches only the months in the ticket passes the first and fails
the second. A repair that only handles the retail branch fails the third. All
three sit outside the world's reserved windows.
"""

from __future__ import annotations

import duckdb
import pytest

# The scored tree is on PYTHONPATH (scoring.build_score_env), so the house
# library resolves the warehouse the same way every DAG in the world does —
# `DUCKDB_PATH` against the root that holds `include/`, whatever the working
# directory happens to be.
from include.lib import warehouse

DB = str(warehouse.warehouse_path())
TABLE = "marts.fct_inventory_valuation"

# Every name here is qualified onto the live warehouse, for the reason
# `include/lib/warehouse.py` gives: the frozen `nwv` copy sits first on the
# search path and carries `raw.inventory_snapshots` under the same name, so an
# unqualified read answers with the count Northwave took on the day its
# namespace froze.
SNAPSHOTS = warehouse.qualify("raw.inventory_snapshots")
COMPLEMENT = warehouse.qualify("raw.dept_cost_complement")
MARTS_SCHEMA = warehouse.qualify("marts")
MART = warehouse.qualify(TABLE)

#: The same table name inside the frozen copy. Used once, by the oracle's own
#: guard, to prove each graded day can tell the two answers apart.
FROZEN = "nwv.raw.inventory_snapshots"

#: The days the fix is measured on. See the module docstring.
IN_MONTHS = "2026-04-15"
LATER = "2026-05-12"
BEFORE_CHANGE = "2026-01-11"
GRADED = (IN_MONTHS, LATER, BEFORE_CHANGE)

#: The columns the build projects, in the order it projects them. The build
#: inserts `BY NAME`, so the table it writes into has to carry these names.
COLUMNS = ("ds", "dept_code", "location_id", "on_hand_units", "cost_cents")

#: The day's stock value, by the method in force on it, straight from the landed
#: snapshot. This is the expected result and there is nothing else behind it:
#: `docs/inventory-policy.md` INV-1 puts the boundary at the first day of FY2026
#: and INV-3 rounds once per department after the complement is applied.
TRUTH = f"""
SELECT s.dept_code, s.location_id,
       sum(s.on_hand_units)::DOUBLE                            AS on_hand_units,
       CASE WHEN s.snapshot_date < DATE '2026-02-01'
            THEN round(sum(s.on_hand_units * s.unit_cost_cents))
            ELSE round(sum(s.retail_value_cents)
                       * max(c.cost_complement_bps) / 10000.0)
       END::DOUBLE                                             AS cost_cents
FROM {{source}} s
LEFT JOIN {COMPLEMENT} c
       ON c.dept_code = s.dept_code
      AND c.effective_from <= s.snapshot_date
WHERE s.snapshot_date = ?::DATE
GROUP BY s.snapshot_date, s.dept_code, s.location_id
ORDER BY 1, 2
"""


def query(sql: str, params: list | None = None, attach_frozen: bool = False) -> list[tuple]:
    """One read, on its own connection. DuckDB takes one writer and the build
    under test opens its own, so nothing here may hold the file open."""
    con = duckdb.connect(DB)
    try:
        if attach_frozen:
            con.execute(f"ATTACH '{warehouse.northwave_path()}' AS nwv (READ_ONLY)")
        return con.execute(sql, params or []).fetchall()
    finally:
        con.close()


@pytest.fixture(scope="session", autouse=True)
def warehouse_ready():
    """The one derived table the valuation writes."""
    con = duckdb.connect(DB)
    try:
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {MARTS_SCHEMA}")
        con.execute(
            f"CREATE TABLE IF NOT EXISTS {MART} ("
            "ds DATE, dept_code VARCHAR, location_id VARCHAR, "
            "on_hand_units DECIMAL(18,3), cost_cents DOUBLE)"
        )
    finally:
        con.close()


def build(ds: str) -> None:
    """Run the patched valuation for one day, the way the DAG's
    `build_valuation` task does."""
    from projects.supply.dags import sc_inventory_snapshot_daily as job

    job.build_valuation(ds)


def truth(ds: str) -> list[tuple]:
    """The day's value off the landed snapshot: the expected result."""
    return query(TRUTH.format(source=SNAPSHOTS), [ds])


def frozen_answer(ds: str) -> list[tuple]:
    """The same day off the frozen copy, for the oracle's own guard."""
    assert warehouse.northwave_path().exists(), (
        "FAIL: the frozen copy is not on disk, so this task has nothing to grade"
    )
    return query(TRUTH.format(source=FROZEN), [ds], attach_frozen=True)


def built(ds: str) -> list[tuple]:
    return query(
        f"SELECT dept_code, location_id, on_hand_units::DOUBLE, "
        f"cost_cents::DOUBLE FROM {MART} WHERE ds = ? ORDER BY 1, 2",
        [ds],
    )


def clear() -> None:
    con = duckdb.connect(DB)
    try:
        con.execute(f"DELETE FROM {MART}")
    finally:
        con.close()


def check_day(ds: str) -> None:
    expected = truth(ds)
    assert expected, f"FAIL: {ds}: the snapshot holds no count that day, so nothing is graded"
    build(ds)
    actual = built(ds)
    differences = [pair for pair in zip(actual, expected) if pair[0] != pair[1]]
    assert actual == expected, (
        f"FAIL: {ds}: the valuation wrote {len(actual)} row(s) and the snapshot "
        f"says {len(expected)}; first row that differs: {differences[:1]}"
    )


def test_the_graded_days_can_tell_the_two_answers_apart():
    """The oracle's own guard. Each graded day has to answer differently off the
    live snapshot and off the frozen copy, or the day proves nothing."""
    for ds in GRADED:
        assert truth(ds) != frozen_answer(ds), (
            f"FAIL: {ds}: the live snapshot and the frozen copy already agree, "
            "so this day cannot grade the fix"
        )


def test_a_day_in_the_months_finance_asked_for_values_the_landed_count():
    check_day(IN_MONTHS)


def test_a_day_after_those_months_values_the_landed_count():
    check_day(LATER)


def test_a_day_before_the_valuation_change_values_the_landed_count():
    """INV-1 puts the method change at the first day of FY2026. Before it the
    value is weighted-average cost per SKU, and a fix that only reaches the
    retail branch leaves this day where it was."""
    check_day(BEFORE_CHANGE)


def test_a_rebuild_of_a_day_leaves_one_copy():
    """The day is replaced, not added to. A repair that has to be run twice
    must not double the day."""
    build(LATER)
    once = built(LATER)
    build(LATER)
    assert built(LATER) == once, f"FAIL: {LATER}: the second build changed the day"


def test_a_build_touches_only_the_day_it_was_given():
    """The ticket's second rule. A build that values every day the snapshot
    holds would fill the months finance asked for and restate every month
    before them, which INV-4 does not allow."""
    clear()
    build(IN_MONTHS)
    days = [str(row[0]) for row in query(f"SELECT DISTINCT ds FROM {MART} ORDER BY 1")]
    assert days == [IN_MONTHS], f"FAIL: one build wrote {len(days)} day(s): {days[:5]}"
