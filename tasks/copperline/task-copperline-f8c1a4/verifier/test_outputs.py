"""LF-1121 — does a scheduled run cover its week, and does the tie read that week?

Never ships to the agent. It runs beside the scored tree, with the tree root on
`PYTHONPATH`, so it imports the patched DAG module the same way the scheduler
does.

**Why a verifier rather than a DAG run.** `AGENTS.md`, "The warehouse": the
world ships the landed half only, so `staging.*` and `marts.*` are not on disk
until a run has built them. `payment_settlement_weekly` cannot build them here —
`apply_fee_schedule` reads `staging.fee_schedule`, and no job in this tree
writes that table — so `airflow dags test` on this DAG stops at the fee step
whatever the agent does. The ticket says so and puts the fee step out of scope.
The fixture below stands up the two schemas and the one mart table, and drives
the three steps the ticket does name: `build_week`, `publish_week` and
`tie_to_payouts`.

**No authored numbers, and no authored window.** Every expected result is
computed in the same session from `raw.fiscal_calendar`,
`raw.marketplace_settlements` and `raw.marketplace_payouts`. The week a run owns
is read out of the calendar table, not written down here: the row for the run
date names the week the date falls in, and the day before that week opens is the
last day of the week that closed.

**Two run dates, both Mondays.**

    2026-03-09  a Monday inside FY2026 Q1
    2026-05-11  a Monday nine weeks later, inside the graded May month

Both are Mondays because `schedule="0 5 * * 1"` and `CONVENTIONS.md` "Dates"
makes `ds` the day the run fires, so a Monday is the only shape a scheduled run
has — and a Monday is where the shipped window collapses. Nine weeks apart, and
neither named in the ticket, so a repair that special-cases one week fails the
other. Both weeks sit inside the world's graded regions
(`docs/copperline-spec/01-timeline.md` §5), outside the reserved
window and outside the never-grade tail.

**Only Mondays are pinned to a week.** For a Monday `ds` every honest reading
agrees: the fiscal week that closed, the seven days the DAG's own contract
names, and the house pattern the other weekly jobs use (land a lag inside the
closed week, then take the calendar's week) all give the same seven days. For a
mid-week `ds` they diverge by a week and the DAG is not scheduled to run then,
so the mid-week dates below are asked one question only — is the window one
fiscal week — and never which one.
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

# Every name is qualified onto the live warehouse, for the reason
# `include/lib/warehouse.py` gives: the frozen `nwv` copy sits first on the
# search path and carries twelve tables under Copperline's own names.
FISCAL = warehouse.qualify("raw.fiscal_calendar")
SETTLEMENTS = warehouse.qualify("raw.marketplace_settlements")
PAYOUTS = warehouse.qualify("raw.marketplace_payouts")
STAGING_SCHEMA = warehouse.qualify("staging")
MARTS_SCHEMA = warehouse.qualify("marts")
MART = warehouse.qualify("marts.settlement_weekly")

#: The two scheduled run dates the fix is measured on. See the module docstring.
GRADED = ("2026-03-09", "2026-05-11")

#: Run dates the window is asked about for its shape alone: two Mondays, a
#: Thursday, a Sunday and a Saturday. A repair that mends the Monday with a
#: special case and leaves the rest one day long fails here.
SHAPE = ("2026-02-19", "2026-03-09", "2026-04-05", "2026-05-02", "2026-05-11")

#: `contracts/settlement_weekly.yml`: six columns, grain week_start × seller_id.
MART_DDL = f"""
CREATE TABLE IF NOT EXISTS {MART} (
    week_start DATE,
    seller_id VARCHAR,
    gmv_cents BIGINT,
    commission_cents BIGINT,
    fee_cents BIGINT,
    payout_cents BIGINT
)
"""

#: The fiscal week that had closed when a run fired. `this` is the calendar row
#: for the run date; `this.week_start - 1` is the day before that week opened,
#: which is the last day of the week before it. Authored in
#: `raw.fiscal_calendar`, derived nowhere.
CLOSED_WEEK = f"""
SELECT prev.week_start, prev.week_end
FROM {FISCAL} AS this
JOIN {FISCAL} AS prev ON prev.cal_date = this.week_start - 1
WHERE this.cal_date = CAST(? AS DATE)
"""

#: Whether the calendar calls a pair of dates one fiscal week: the row for the
#: first date has to open its week on that date and close it on the second.
IS_ONE_WEEK = f"""
SELECT count(*) FROM {FISCAL}
WHERE cal_date = CAST(? AS DATE)
  AND week_start = CAST(? AS DATE)
  AND week_end = CAST(? AS DATE)
"""

#: The Sunday that opens the week a run date falls in. On a Monday `ds` that is
#: the day before the run, and it is the whole of the shipped window.
OPENING_SUNDAY = f"""
SELECT week_start FROM {FISCAL} WHERE cal_date = CAST(? AS DATE)
"""

#: The week's summary, straight from the landed remittance. This is the expected
#: result and there is nothing else behind it: the same six columns the contract
#: names, over the week the calendar gives, from the two raw tables the build
#: reads. `seller_id` reaches a settlement line through its payout header, which
#: is the only place it is written down.
TRUTH = f"""
SELECT p.seller_id,
       sum(CASE WHEN s.line_type = 'principal' THEN -s.amount_cents ELSE 0 END)::BIGINT
           AS gmv_cents,
       sum(CASE WHEN s.line_type = 'commission' THEN s.amount_cents ELSE 0 END)::BIGINT
           AS commission_cents,
       sum(CASE WHEN s.line_type = 'fulfilment_fee' THEN s.amount_cents ELSE 0 END)::BIGINT
           AS fee_cents,
       sum(-s.amount_cents)::BIGINT AS payout_cents
FROM {SETTLEMENTS} s
JOIN {PAYOUTS} p ON p.payout_id = s.payout_id
WHERE s.payout_date >= CAST(? AS DATE)
  AND s.payout_date <= CAST(? AS DATE)
GROUP BY p.seller_id
ORDER BY p.seller_id
"""

#: What the payout table says left the account that week, which is what
#: `tie_to_payouts` returns when it agrees with the summary.
PAID = f"""
SELECT coalesce(sum(net_paid_cents), 0)::BIGINT FROM {PAYOUTS}
WHERE payout_date >= CAST(? AS DATE)
  AND payout_date <= CAST(? AS DATE)
  AND payout_status = 'paid'
"""

PUBLISHED = f"""
SELECT seller_id, gmv_cents, commission_cents, fee_cents, payout_cents
FROM {MART}
WHERE week_start = CAST(? AS DATE)
ORDER BY seller_id
"""


def query(sql: str, params: list | None = None) -> list[tuple]:
    """One statement, on its own connection. DuckDB takes one writer and the
    steps under test open their own, so nothing here may hold the file."""
    con = duckdb.connect(DB)
    try:
        return con.execute(sql, params or []).fetchall()
    finally:
        con.close()


def module():
    """The patched DAG module, imported the way the dag processor imports it."""
    from projects.commerce.dags import payment_settlement_weekly

    return payment_settlement_weekly


def bounds(ds: str) -> tuple[str, str]:
    """The window this run covers, as the patched module computes it."""
    start, end = module().week_bounds(ds)
    return str(start)[:10], str(end)[:10]


def closed_week(ds: str) -> tuple[str, str]:
    """The fiscal week that had closed when the run fired, from the calendar."""
    rows = query(CLOSED_WEEK, [ds])
    assert rows, f"FAIL: raw.fiscal_calendar has no week before {ds}"
    return str(rows[0][0])[:10], str(rows[0][1])[:10]


def expected(ds: str) -> list[tuple]:
    return query(TRUTH, list(closed_week(ds)))


def paid(ds: str) -> int:
    return int(query(PAID, list(closed_week(ds)))[0][0])


def published(ds: str) -> list[tuple]:
    return query(PUBLISHED, [closed_week(ds)[0]])


def publish(ds: str) -> None:
    """Build the week's lines and publish them, the way the DAG's `build_week`
    and `publish_week` tasks do."""
    mod = module()
    mod.build_week(ds)
    mod.publish_week(ds)


def clear() -> None:
    """Start from an empty mart. The world ships without one, so this is the
    state a first run of the week meets."""
    query(f"DELETE FROM {MART}")


def tie(ds: str) -> int | str:
    """`tie_to_payouts`, with whatever it raises turned into something a test
    can print. The step raises when the two sides disagree, which is the whole
    reason the job is red; a statement left with a placeholder nobody binds
    raises something else, and that is worth reading too."""
    try:
        return int(module().tie_to_payouts(ds))
    except Exception as broken:  # noqa: BLE001 - the message is the blame
        return f"{type(broken).__name__}: {broken}"


@pytest.fixture(scope="session", autouse=True)
def warehouse_ready():
    """The two schemas and the one table a run would have made by now."""
    con = duckdb.connect(DB)
    try:
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {STAGING_SCHEMA}")
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {MARTS_SCHEMA}")
        con.execute(MART_DDL)
    finally:
        con.close()


def test_the_two_graded_weeks_can_tell_the_answers_apart():
    """The fixture's own guard, and the shape of the world the rest rests on:
    two different closed weeks, each with sellers and money in it, and neither
    holding a payout on the Sunday the shipped window collapsed to."""
    weeks = set()
    for ds in GRADED:
        start, end = closed_week(ds)
        weeks.add((start, end))
        sellers = expected(ds)
        assert len(sellers) >= 2, (
            f"FAIL: {ds}: the week of {start} covers {len(sellers)} sellers, "
            "so it cannot show a week being summarised"
        )
        assert paid(ds) > 0, f"FAIL: {ds}: the week of {start} paid out nothing"
        sunday = str(query(OPENING_SUNDAY, [ds])[0][0])[:10]
        assert query(PAID, [sunday, sunday])[0][0] == 0, (
            f"FAIL: {ds}: a payout lands on {sunday}, so the one-day window the "
            "shipped code returns is not empty and this task grades nothing"
        )
    assert len(weeks) == len(GRADED), (
        f"FAIL: the graded dates resolve to {weeks}, which is not two weeks"
    )


def test_a_scheduled_run_covers_the_fiscal_week_that_closed():
    """The find. A Monday run owns the week that closed on the Saturday before
    it — seven days, Sunday to Saturday, both ends authored in
    `raw.fiscal_calendar`. The shipped window is that Sunday twice over."""
    for ds in GRADED:
        assert bounds(ds) == closed_week(ds), (
            f"FAIL: {ds}: the run covers {bounds(ds)} and the fiscal week that "
            f"closed before it is {closed_week(ds)}"
        )


def test_the_window_is_one_fiscal_week_whatever_day_the_run_is():
    """A window that is one fiscal week on a Monday and one day on a Thursday is
    a special case, not a repair. This asks the calendar the question and takes
    whichever week the answer names."""
    for ds in SHAPE:
        start, end = bounds(ds)
        assert query(IS_ONE_WEEK, [start, start, end])[0][0] == 1, (
            f"FAIL: {ds}: the run covers {start} to {end}, and "
            "raw.fiscal_calendar has no fiscal week with those two ends"
        )


def test_a_scheduled_run_summarises_its_week():
    """What seller-ops are missing. The published week holds what the landed
    remittance says it holds, one row per seller, and it is not empty."""
    for ds in GRADED:
        clear()
        publish(ds)
        start = closed_week(ds)[0]
        rows, truth = published(ds), expected(ds)
        assert rows, (
            f"FAIL: {ds}: the run published no rows at all for the week of {start}"
        )
        assert rows == truth, (
            f"FAIL: {ds}: the published week is {rows[:3]} and the "
            f"remittance says {truth[:3]}"
        )


def test_the_payout_tie_passes_on_a_week_the_run_summarised():
    """The step that has been red. Once the summary covers the week and the tie
    reads the same week, the two sides are the same money twice —
    `raw.marketplace_payouts.net_paid_cents` is rolled up from the settlement
    lines the summary sums — so the step passes and returns the week's total."""
    for ds in GRADED:
        clear()
        publish(ds)
        assert tie(ds) == paid(ds), (
            f"FAIL: {ds}: tie_to_payouts gave {tie(ds)} and the week of "
            f"{closed_week(ds)[0]} paid {paid(ds)}"
        )


def test_the_tie_reads_the_week_it_was_asked_for():
    """Both weeks in the mart at once. A payout side that opens at the week and
    closes nowhere sums every payout from then on, so it cannot tell the two
    weeks apart; nor can a summary side that stopped naming one week."""
    clear()
    for ds in GRADED:
        publish(ds)
    for ds in GRADED:
        assert tie(ds) == paid(ds), (
            f"FAIL: {ds}: with both weeks published, tie_to_payouts gave "
            f"{tie(ds)} and the week of {closed_week(ds)[0]} paid {paid(ds)}"
        )
