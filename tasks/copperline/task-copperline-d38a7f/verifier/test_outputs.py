"""OPS-412 — does the patched revenue build put the day's money under the
right channel?

Never ships to the agent. It runs beside the scored tree, with the tree root on
`PYTHONPATH`, so it imports the patched module the same way a DAG would.

**Why a verifier rather than a DAG run.** The world ships the landed half of
the warehouse only — `AGENTS.md`, "The warehouse": `staging.*` and `marts.*`
are derived, so neither `int.int_orders_enriched` nor `marts.daily_revenue` is
on disk until the 04:00 dbt build and the 06:00 DAG have made them. Standing
that up costs a 143-model dbt run for one group-by. Instead the fixture below
makes the two relations `build_daily` reads and writes, out of `raw.orders`,
with the same projection `stg_sales__orders` and `int_orders_enriched` make of
it for the columns this build touches — including the two filters staging
applies and nothing else does.

**No authored numbers.** Every expected result is computed from `raw.orders` in
the same session. A fix is graded against the world's own orders, never against
a figure typed here.

**Three days, on purpose.**

    2026-03-04  inside the week the ticket names, and a day it does not name
    2026-02-25  a week before the ticket's window opens
    2026-05-19  eleven weeks after it closes, and named nowhere

A repair that special-cases the ticket's dates passes the first and fails the
other two.
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
TABLE = "marts.daily_revenue"

# Every name here is qualified onto the live warehouse, for the reason
# `include/lib/warehouse.py` gives: the frozen `nwv` copy sits first on the
# search path and carries `raw.orders` under the same name, so an unqualified
# read inside a view would answer with September 2025's Northwave numbers.
ORDERS = warehouse.qualify("raw.orders")
STORES = warehouse.qualify("raw.stores")
INT_SCHEMA = warehouse.qualify('"int"')
MARTS_SCHEMA = warehouse.qualify("marts")
INT_MODEL = f"{INT_SCHEMA}.int_orders_enriched"
MART = warehouse.qualify(TABLE)

#: The days the fix is measured on. See the module docstring.
IN_WEEK = "2026-03-04"
BEFORE = "2026-02-25"
LATER = "2026-05-19"
GRADED = (IN_WEEK, BEFORE, LATER)

#: What `stg_sales__orders` selects out of `raw.orders`, plus the two renames
#: `int_orders_enriched` makes of it. Test orders and rows the OMS deleted are
#: dropped in staging and nowhere else, which is why they are here.
INT_VIEW = f"""
CREATE OR REPLACE VIEW {INT_MODEL} AS
SELECT order_id,
       order_ref,
       local_order_date            AS order_date,
       channel,
       brand,
       market_code,
       store_id,
       customer_ref,
       loyalty_id,
       source_system,
       order_status,
       event_time_utc,
       event_time_local,
       currency_code,
       fx_rate_ppm,
       subtotal_cents              AS booked_cents,
       order_discount_cents,
       tax_cents,
       shipping_cents,
       grand_total_cents,
       gift_card_applied_cents,
       updated_at,
       loaded_at
FROM {ORDERS}
WHERE NOT coalesce(is_test, false)
  AND deleted_at IS NULL
"""

#: The day's booked revenue by channel, straight from the landed orders. This
#: is the expected result and there is nothing else behind it.
TRUTH = f"""
SELECT channel,
       sum(subtotal_cents)::BIGINT AS booked_cents,
       count(*)::BIGINT            AS order_count
FROM {ORDERS}
WHERE local_order_date = ?
  AND NOT coalesce(is_test, false)
  AND deleted_at IS NULL
GROUP BY channel
ORDER BY channel
"""

#: What the shipped build produces: the selling store's origin decides the
#: channel, so an acquired store's counter sales come out under `trade`. Kept
#: here to prove the graded days can tell the two answers apart.
SHIPPED = f"""
SELECT CASE WHEN s.acquired_from = 'northwave' THEN 'trade' ELSE o.channel END AS channel,
       sum(o.subtotal_cents)::BIGINT AS booked_cents,
       count(*)::BIGINT              AS order_count
FROM {ORDERS} o
LEFT JOIN {STORES} s ON s.store_id = o.store_id AND s.is_current
WHERE o.local_order_date = ?
  AND NOT coalesce(o.is_test, false)
  AND o.deleted_at IS NULL
GROUP BY 1
ORDER BY 1
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
    """The two derived relations the build reads and writes."""
    con = duckdb.connect(DB)
    try:
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {INT_SCHEMA}")
        con.execute(INT_VIEW)
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {MARTS_SCHEMA}")
        con.execute(
            f"CREATE TABLE IF NOT EXISTS {MART} ("
            "ds DATE, channel VARCHAR, booked_cents BIGINT, order_count BIGINT)"
        )
    finally:
        con.close()


def build(ds: str) -> None:
    """Run the patched build for one day, the way the DAG's `build` task does."""
    from projects.finance.lib import revenue

    revenue.build_daily(ds, TABLE)


def built(ds: str) -> list[tuple]:
    return query(
        f"SELECT channel, booked_cents, order_count FROM {MART} "
        "WHERE ds = ? ORDER BY channel",
        [ds],
    )


def check_day(ds: str) -> None:
    expected = query(TRUTH, [ds])
    assert expected, f"FAIL: {ds}: no orders landed that day, so nothing is graded"
    build(ds)
    actual = built(ds)
    assert actual == expected, (
        f"FAIL: {ds}: the day's channel split is {actual}, and the order spine "
        f"says {expected}"
    )


def test_the_graded_days_can_tell_the_two_answers_apart():
    """The fixture's own guard. Each graded day has to hold acquired-store
    sales, or the shipped build and a correct one agree and the day proves
    nothing."""
    for ds in GRADED:
        assert query(SHIPPED, [ds]) != query(TRUTH, [ds]), (
            f"FAIL: {ds}: the shipped rollup and the order spine already agree, "
            "so this day cannot grade the fix"
        )


def test_a_day_inside_the_week_matches_the_order_spine():
    check_day(IN_WEEK)


def test_a_day_before_the_window_matches_the_order_spine():
    check_day(BEFORE)


def test_a_day_the_ticket_never_mentions_matches_the_order_spine():
    check_day(LATER)


def test_every_channel_still_lands_a_row():
    """The ticket's second rule. A fix that drops a channel to make the split
    move is not a fix."""
    from projects.finance.lib import revenue

    build(LATER)
    found = {row[0] for row in built(LATER)}
    missing = sorted(set(revenue.CHANNELS) - found)
    assert not missing, f"FAIL: {LATER}: no row for {', '.join(missing)}"


def test_the_day_total_ties_to_the_order_spine():
    """The ticket's first rule, and the check the shipped build already passes:
    whatever the split, the day's total is the spine's total."""
    build(IN_WEEK)
    total = query(
        f"SELECT coalesce(sum(booked_cents), 0) FROM {MART} WHERE ds = ?",
        [IN_WEEK],
    )[0][0]
    spine = query(
        f"SELECT coalesce(sum(subtotal_cents), 0) FROM {ORDERS} "
        "WHERE local_order_date = ? AND NOT coalesce(is_test, false) "
        "AND deleted_at IS NULL",
        [IN_WEEK],
    )[0][0]
    assert total == spine, f"FAIL: {IN_WEEK}: built {total} cents against {spine}"


def test_rebuilding_the_same_day_leaves_one_copy():
    """A repair that has to be run twice must not double the day."""
    build(BEFORE)
    once = built(BEFORE)
    build(BEFORE)
    assert built(BEFORE) == once, f"FAIL: {BEFORE}: the second build changed the day"
