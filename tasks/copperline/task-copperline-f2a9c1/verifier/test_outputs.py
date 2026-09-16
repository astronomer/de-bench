"""SC-1204 — does the patched shrink build value a week by the method that week
is on, and leave the closed weeks where they are?

Never ships to the agent. It runs beside the scored tree, with the tree root on
`PYTHONPATH`, so it imports the patched DAG module the same way Airflow would
and calls the same `build_shrink` the `build_shrink` task calls.

**Why a verifier rather than a DAG run.** The world ships the landed half of the
warehouse only (`AGENTS.md`, "The warehouse"): `marts.*` is derived and nothing
is on disk until a run has made it, and `marts.shrink_weekly` has no DDL
anywhere in the tree. The fixture below stands the one table up and calls the
build directly, which grades the same code in seconds. It also runs the build's
own `week_covered`, so a run date maps to a week the way the patched job maps
it.

**No authored numbers.** Every expected result is computed from
`raw.wms_movements`, `raw.inventory_snapshots` and `raw.dept_cost_complement`
in the same session, both ways: units at the weighted-average cost the feed
carried before the change, and the counted units at retail taken down by the
department's cost complement after it. `docs/inventory-policy.md` INV-1 puts the
boundary at the first day of FY2026, INV-2 puts the cost at department grain,
INV-3 rounds once per department and INV-4 forbids restating what closed
earlier.

**Two complements, both accepted.** The complement is written down twice: on the
snapshot row the feed sends, and in `raw.dept_cost_complement`, which carries a
period's own approved value and drifts a few basis points away from the feed's
copy. INV-3 calls it "the value on the row" and the policy names both tables as
sources, so either is a defensible read and the two never differ by more than
about a seventh of a per cent. Every comparison here is made against both, at a
tolerance that swallows that gap and nothing larger: the closest wrong answer on
the board (FY2025 cost carried forward into FY2026) sits between 5.7 and 8.1 per
cent out on every row, which is the spread the two methods are built to.

**Three weeks, on purpose.**

    2026-01-11  closed three weeks before the change; the cost method applies
                and INV-4 says the figure does not move
    2026-02-15  the third week under the retail method
    2026-05-10  thirteen weeks later, and named nowhere in the ticket

A repair that reaches only the weeks finance complained about passes nothing. A
repair that applies the retail method to every week fails the first: before the
change no row carries a retail value at all, so the week collapses to zero. A
repair that reads the calendar year rather than the fiscal one fails the first
for the same reason. All three weeks sit outside the world's reserved windows.
"""

from __future__ import annotations

import re

import duckdb
import pytest

# The scored tree is on PYTHONPATH (scoring.build_score_env), so the house
# library resolves the warehouse the same way every DAG in the world does —
# `DUCKDB_PATH` against the root that holds `include/`, whatever the working
# directory happens to be.
from include.lib import warehouse

DB = str(warehouse.warehouse_path())
TABLE = "marts.shrink_weekly"

# Every name here is qualified onto the live warehouse, for the reason
# `include/lib/warehouse.py` gives: the frozen `nwv` copy sits first on the
# search path and carries `raw.wms_movements` and `raw.inventory_snapshots`
# under the same names, so an unqualified read answers with the counts
# Northwave took before its namespace froze.
MOVEMENTS = warehouse.qualify("raw.wms_movements")
SNAPSHOTS = warehouse.qualify("raw.inventory_snapshots")
COMPLEMENT = warehouse.qualify("raw.dept_cost_complement")
MARTS_SCHEMA = warehouse.qualify("marts")
MART = warehouse.qualify(TABLE)

#: The run dates the fix is measured on, and the week each one covers. The job
#: runs on a Thursday and covers the week that closed the Saturday before, so a
#: week that opens on a Sunday is built by the run eleven days later. The weeks
#: are in the module docstring; the run dates are derived from them here and
#: checked against the patched `week_covered` before anything is graded.
BEFORE_CHANGE = ("2026-01-22", "2026-01-11")
AFTER_CHANGE = ("2026-02-26", "2026-02-15")
LATER = ("2026-05-21", "2026-05-10")
GRADED = (BEFORE_CHANGE, AFTER_CHANGE, LATER)

#: The five columns the loss-prevention pack selects by name, which the ticket
#: asks the fix to keep.
COLUMNS = ("week_start", "location_id", "dept_code", "counted_delta", "shrink_cents")

#: How far a graded row may sit from the expected result. See the module
#: docstring: it has to swallow the gap between the two complements (0.14 per
#: cent at the worst row of any graded week) and reject the nearest wrong
#: method (5.7 per cent at its best row). The absolute floor keeps a row worth a
#: few dollars from being graded on its rounding.
RELATIVE_TOLERANCE = 0.01
ABSOLUTE_TOLERANCE_CENTS = 500

#: One week of counted rows, with the inputs to both valuations beside each. The
#: match is the shipped one — count and snapshot on the same day, same SKU, same
#: location — because the ticket puts the match out of scope.
COUNTED = f"""
WITH counted AS (
    SELECT m.from_location                   AS location_id,
           s.dept_code                       AS dept_code,
           m.qty                             AS qty,
           m.qty * s.unit_cost_cents         AS at_cost_cents,
           CASE WHEN s.on_hand_units <> 0
                THEN m.qty * (s.retail_value_cents::DECIMAL(38, 6)
                              / s.on_hand_units)
           END                               AS at_retail_cents,
           s.cost_complement_bps             AS feed_bps,
           d.cost_complement_bps             AS record_bps
    FROM {MOVEMENTS} m
    LEFT JOIN {SNAPSHOTS} s
           ON s.sku = m.sku AND s.location_id = m.from_location
          AND s.snapshot_date = m.occurred_at::DATE
    LEFT JOIN {COMPLEMENT} d
           ON d.dept_code = s.dept_code
          AND d.effective_from = (
              SELECT max(x.effective_from)
              FROM {COMPLEMENT} x
              WHERE x.dept_code = s.dept_code
                AND x.effective_from <= m.occurred_at::DATE)
    WHERE m.movement_type = 'cycle_count'
      AND m.occurred_at::DATE >= ?::DATE
      AND m.occurred_at::DATE < ?::DATE + 7
)
"""

#: The week by both methods, at the grain the mart publishes at.
EXPECTED = COUNTED + """
SELECT location_id,
       dept_code,
       sum(qty)::DOUBLE                                  AS counted_delta,
       coalesce(sum(at_cost_cents), 0)::DOUBLE           AS at_cost,
       cast(round(coalesce(sum(at_retail_cents), 0)
                  * coalesce(max(feed_bps), 0) / 10000, 0) AS DOUBLE)
                                                         AS at_retail_feed,
       cast(round(coalesce(sum(at_retail_cents), 0)
                  * coalesce(max(record_bps), 0) / 10000, 0) AS DOUBLE)
                                                         AS at_retail_record
FROM counted
GROUP BY location_id, dept_code
ORDER BY location_id, dept_code
"""


def query(sql: str, params: list | None = None) -> list[tuple]:
    """One read, on its own connection. DuckDB takes one writer and the build
    under test opens its own, so nothing here may hold the file open."""
    con = duckdb.connect(DB)
    try:
        return con.execute(sql, params or []).fetchall()
    finally:
        con.close()


@pytest.fixture(scope="session", autouse=True)
def warehouse_ready():
    """The one table the build writes.

    Nothing in the world creates it, so this stands it up with the five columns
    the pack reads. A build that projects a sixth gets the column added rather
    than a binder error: the mart has no DDL for anyone to have agreed on, so a
    fix that carries an extra column is not wrong for carrying it.
    """
    con = duckdb.connect(DB)
    try:
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {MARTS_SCHEMA}")
        con.execute(
            f"CREATE TABLE IF NOT EXISTS {MART} ("
            "week_start DATE, location_id VARCHAR, dept_code VARCHAR, "
            "counted_delta DECIMAL(18,3), shrink_cents BIGINT)"
        )
    finally:
        con.close()


def add_column(name: str) -> None:
    con = duckdb.connect(DB)
    try:
        con.execute(f'ALTER TABLE {MART} ADD COLUMN "{name}" VARCHAR')
    finally:
        con.close()


def build(ds: str) -> None:
    """Run the patched build for one run date, the way the DAG's `build_shrink`
    task does. A missing column is added and the build retried, at most a
    handful of times, so an extra column costs the fix nothing."""
    from projects.supply.dags import sc_shrink_weekly as job

    for _ in range(6):
        try:
            job.build_shrink(ds)
            return
        except duckdb.BinderException as error:
            found = re.search(r'does not have a column with name "([^"]+)"', str(error))
            if not found:
                raise
            add_column(found.group(1))
    raise AssertionError(f"FAIL: {ds}: the build kept asking for new columns")


def week_of(ds: str) -> str:
    """The week the patched job says that run date covers."""
    from projects.supply.dags import sc_shrink_weekly as job

    return str(job.week_covered(ds))


def expected(week: str) -> dict[tuple[str, str], tuple[float, float, float, float]]:
    rows = query(EXPECTED, [week, week])
    return {(row[0], row[1]): tuple(row[2:]) for row in rows}


def built(week: str) -> dict[tuple[str, str], tuple[float, float]]:
    rows = raw_rows(week)
    return {(row[0], row[1]): (row[2], row[3]) for row in rows}


def raw_rows(week: str) -> list[tuple]:
    """The week's published rows, one per row in the table. A row a second build
    added shows up here and is lost by the dict, so the rebuild test uses this.

    A NULL `shrink_cents` reads as zero. The mart has no DDL and nothing in the
    world says which spelling an unvalued row takes, so the two are graded the
    same and both are wrong wherever a figure is expected."""
    return query(
        f"SELECT location_id, dept_code, counted_delta::DOUBLE, "
        f"coalesce(shrink_cents, 0)::DOUBLE "
        f"FROM {MART} WHERE week_start = ?::DATE ORDER BY 1, 2",
        [week],
    )


def in_order(keys) -> list[tuple[str, str]]:
    """Location-departments in a stable order. A count that found no snapshot
    row carries no department, so a key holds a NULL and cannot be sorted
    straight."""
    return sorted(keys, key=lambda key: (key[0] or "", key[1] or ""))


def near(actual: float, candidates: tuple[float, ...]) -> bool:
    """Is the built figure one of the defensible answers? See the module
    docstring for what the tolerance has to admit and what it has to reject."""
    return any(
        abs(actual - want)
        <= max(abs(want) * RELATIVE_TOLERANCE, ABSOLUTE_TOLERANCE_CENTS)
        for want in candidates
    )


def check_week(ds: str, week: str, method: str) -> None:
    """Build the run date and grade every row of the week it covers."""
    assert week_of(ds) == week, (
        f"FAIL: {ds}: the job says it covers the week of {week_of(ds)}, and the "
        f"counts for it are the week of {week}"
    )
    want = expected(week)
    assert want, f"FAIL: {week}: no counts landed that week, so nothing is graded"
    build(ds)
    got = built(week)
    assert got, f"FAIL: {week}: the build wrote no rows for the week it covers"

    missing = in_order(set(want) - set(got))
    assert not missing, (
        f"FAIL: {week}: no row for {len(missing)} of the {len(want)} "
        f"location-departments that counted, first {missing[:3]}"
    )

    index = {"cost": (0,), "retail": (1, 2)}[method]
    wrong = []
    for key in in_order(want):
        candidates = tuple(want[key][1:][i] for i in index)
        actual = got[key][1]
        if not near(actual, candidates):
            wrong.append(f"{key[0]}/{key[1]}: built {actual:.0f}, wanted {candidates}")
    assert not wrong, (
        f"FAIL: {week}: {len(wrong)} of {len(want)} rows are not the week's "
        f"{method} figure; first three: {'; '.join(wrong[:3])}"
    )


def test_the_graded_weeks_can_tell_the_two_methods_apart():
    """The oracle's own guard. Each graded week has to answer differently under
    the two methods, or the week proves nothing about which one was applied."""
    for ds, week in GRADED:
        rows = expected(week)
        assert rows, f"FAIL: {week}: no counts landed, so this week grades nothing"
        at_cost = sum(row[1] for row in rows.values())
        at_retail = sum(row[2] for row in rows.values())
        assert abs(at_cost - at_retail) > abs(at_cost) * 0.5, (
            f"FAIL: {week}: the two methods answer {at_cost:.0f} and "
            f"{at_retail:.0f}, which is too close to grade a choice between them"
        )


def test_a_week_that_closed_before_the_change_is_not_restated():
    """INV-4. The weeks finance closed on the cost method stay on it, to the
    cent. A fix that values every week at retail empties this one: before the
    change no snapshot row carries a retail value at all."""
    check_week(*BEFORE_CHANGE, "cost")


def test_the_week_after_the_change_is_valued_at_retail():
    check_week(*AFTER_CHANGE, "retail")


def test_a_week_the_ticket_never_names_is_valued_at_retail():
    """Thirteen weeks past the one finance pulled. A repair aimed at the weeks
    in the complaint stops here."""
    check_week(*LATER, "retail")


def test_the_shrink_line_is_a_loss_again():
    """The ticket's third rule, and the symptom it opens with. Every cycle
    count in this world is a shortfall, so a week's shrink is negative money.
    Zero is the state being complained about, and a positive figure is the
    position valued instead of the loss."""
    ds, week = LATER
    build(ds)
    total = query(
        f"SELECT coalesce(sum(shrink_cents), 0)::DOUBLE FROM {MART} "
        "WHERE week_start = ?::DATE",
        [week],
    )[0][0]
    assert total < 0, f"FAIL: {week}: the week's shrink comes to {total:.0f} cents"


def test_the_counted_units_did_not_move():
    """`counted_delta` is the units the counts found and the ticket does not ask
    for it to change. A fix that moves it has changed what the row means."""
    ds, week = AFTER_CHANGE
    build(ds)
    want = expected(week)
    got = built(week)
    wrong = [
        f"{key[0]}/{key[1]}: {got[key][0]} against {want[key][0]}"
        for key in in_order(want)
        if key in got and abs(got[key][0] - want[key][0]) > 0.001
    ]
    assert not wrong, f"FAIL: {week}: the counted units moved on {len(wrong)} rows"


def test_every_location_that_counted_still_gets_a_row():
    """The job's own `check_counts_posted` rule: a location that counted and has
    no row reads as a location with nothing to lose. Dropping the rows that
    cannot be valued would make the mart look healthier and lose the count."""
    ds, week = LATER
    build(ds)
    counted = {row[0] for row in query(
        f"SELECT DISTINCT from_location FROM {MOVEMENTS} "
        "WHERE movement_type = 'cycle_count' "
        "AND occurred_at::DATE >= ?::DATE AND occurred_at::DATE < ?::DATE + 7",
        [week, week],
    )}
    published = {key[0] for key in built(week)}
    missing = sorted(counted - published)
    assert not missing, f"FAIL: {week}: {len(missing)} locations counted and have no row: {missing[:5]}"


def test_rebuilding_a_week_leaves_one_copy():
    """A repair that has to be run again over the four months finance is
    missing must not double the week it re-runs."""
    ds, week = AFTER_CHANGE
    build(ds)
    once = raw_rows(week)
    build(ds)
    again = raw_rows(week)
    assert again == once, (
        f"FAIL: {week}: the second build changed the week — {len(once)} rows "
        f"became {len(again)}"
    )
