"""LF-1042 — can a settlement week that has already gone out be published again?

Never ships to the agent. It runs beside the scored tree, with the tree root on
`PYTHONPATH`, so it imports the patched DAG module the same way the scheduler
does.

**Why a verifier rather than a DAG run.** `AGENTS.md`, "The warehouse": the
world ships the landed half only, so `staging.*` and `marts.*` are not on disk
until a run has built them. `payment_settlement_weekly` cannot build them here —
`apply_fee_schedule` reads `staging.fee_schedule`, and no job in this tree
writes that table — so `airflow dags test` on this DAG stops at the fee step
whatever the agent does to the publish. The ticket says so and puts the fee step
out of scope. The fixture below stands up the two schemas and the one mart
table, and drives the two steps the ticket does name, `build_week` and
`publish_week`, straight off the module.

**No authored numbers.** Every expected result is computed from
`raw.marketplace_settlements` and `raw.marketplace_payouts` in the same session,
over the window the patched `week_bounds` returns. A fix that moves the window
is graded against its own window, because the window is not what the ticket
asks about — the write is.

**Two weeks, on purpose.**

    2026-02-08  one of the dates seller-ops re-triggered. Its fiscal week is the
                week the ticket is about.
    2026-05-16  fourteen weeks later, and named nowhere.

A repair that special-cases February passes the first week and fails the
second. The second week is also what convicts a publish that replaces the whole
table instead of the week it owns: publish February, publish May, and February
has to still be there.

Both dates sit inside the world's graded regions, outside the safe zone and
outside the never-grade tail. Neither is a Monday, and that is deliberate: the
run for a Monday `ds` covers the Sunday alone, and every marketplace payout
lands on a Monday, so a Monday `ds` summarises an empty week and grades
nothing.
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
SETTLEMENTS = warehouse.qualify("raw.marketplace_settlements")
PAYOUTS = warehouse.qualify("raw.marketplace_payouts")
STAGING_SCHEMA = warehouse.qualify("staging")
MARTS_SCHEMA = warehouse.qualify("marts")
MART = warehouse.qualify("marts.settlement_weekly")

#: The two run dates the fix is measured on. See the module docstring.
IN_REPLAY = "2026-02-08"
LATER = "2026-05-16"
GRADED = (IN_REPLAY, LATER)

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

#: The week's summary, straight from the landed remittance. This is the expected
#: result and there is nothing else behind it: the same six columns the contract
#: names, over the same window the publish covers, from the two raw tables the
#: build reads. `seller_id` reaches a settlement line through its payout header,
#: which is the only place it is written down.
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

PUBLISHED = f"""
SELECT seller_id, gmv_cents, commission_cents, fee_cents, payout_cents
FROM {MART}
WHERE week_start = CAST(? AS DATE)
ORDER BY seller_id
"""


def query(sql: str, params: list | None = None) -> list[tuple]:
    """One statement, on its own connection. DuckDB takes one writer and the
    publish under test opens its own, so nothing here may hold the file."""
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
    return str(start), str(end)


def expected(ds: str) -> list[tuple]:
    return query(TRUTH, list(bounds(ds)))


def published(ds: str) -> list[tuple]:
    return query(PUBLISHED, [bounds(ds)[0]])


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
    """The fixture's own guard. Two different weeks, both with sellers in them,
    or nothing below grades anything."""
    starts = set()
    for ds in GRADED:
        rows = expected(ds)
        assert len(rows) >= 2, (
            f"FAIL: {ds}: the week covers {len(rows)} sellers, so it cannot "
            "show a week being written twice"
        )
        starts.add(bounds(ds)[0])
    assert len(starts) == len(GRADED), (
        f"FAIL: the graded dates resolve to {starts}, which is not two weeks"
    )


def test_a_published_week_matches_the_landed_remittance():
    """A week published once holds what the remittance says it holds. This is
    the check the shipped publish already passes, and it is here so that a fix
    cannot pass the rest by writing nothing."""
    clear()
    publish(IN_REPLAY)
    assert published(IN_REPLAY) == expected(IN_REPLAY), (
        f"FAIL: {IN_REPLAY}: the published week is {published(IN_REPLAY)[:3]} "
        f"and the remittance says {expected(IN_REPLAY)[:3]}"
    )


def test_republishing_a_week_leaves_one_row_per_seller():
    """`contracts/settlement-summary.md` SS-3, and the whole ticket. A re-run
    for a week that has already been delivered replaces that week's rows; two
    rows for the same seller and the same week is a defect."""
    clear()
    publish(IN_REPLAY)
    once = published(IN_REPLAY)
    publish(IN_REPLAY)
    twice = published(IN_REPLAY)
    start = bounds(IN_REPLAY)[0]
    rows, sellers = query(
        f"SELECT count(*), count(DISTINCT seller_id) FROM {MART} "
        "WHERE week_start = CAST(? AS DATE)",
        [start],
    )[0]
    assert rows == sellers, (
        f"FAIL: week of {start}: {rows} rows for {sellers} sellers after a "
        "second run of the same week"
    )
    assert twice == once == expected(IN_REPLAY), (
        f"FAIL: week of {start}: the second run changed the week"
    )


def test_publishing_one_week_leaves_the_other_week_alone():
    """A replay owns the week it was asked for. A publish that empties the
    table, or deletes by seller rather than by week, passes the test above and
    dies here."""
    clear()
    publish(IN_REPLAY)
    publish(LATER)
    publish(LATER)
    assert published(IN_REPLAY) == expected(IN_REPLAY), (
        f"FAIL: publishing {LATER} moved the week of {bounds(IN_REPLAY)[0]}"
    )
    assert published(LATER) == expected(LATER), (
        f"FAIL: {LATER}: the published week is {published(LATER)[:3]} and the "
        f"remittance says {expected(LATER)[:3]}"
    )


def test_a_replay_rewrites_the_week_rather_than_skipping_it():
    """The other half of SS-3. Seller-ops replay a week to correct the figures
    on it, so a publish that sees the week already there and does nothing has
    not fixed the doubling, it has stopped the replay working at all."""
    clear()
    publish(LATER)
    start = bounds(LATER)[0]
    query(
        f"UPDATE {MART} SET gmv_cents = gmv_cents + 1, payout_cents = payout_cents + 1 "
        "WHERE week_start = CAST(? AS DATE)",
        [start],
    )
    publish(LATER)
    assert published(LATER) == expected(LATER), (
        f"FAIL: week of {start}: a second run left the figures it was asked to "
        "correct where they were"
    )
