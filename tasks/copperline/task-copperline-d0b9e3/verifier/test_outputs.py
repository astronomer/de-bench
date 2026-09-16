"""LF-1103 — does the weekly settlement summary hold back what Copperline kept?

Never ships to the agent. It runs beside the scored tree, with the tree root on
`PYTHONPATH`, so it resolves the warehouse the way every DAG in the world does.

**No authored numbers.** Every expected result is computed from
`raw.marketplace_orders`, `raw.marketplace_settlements` and
`raw.fiscal_calendar` in the same session. Nothing here holds a cent, a row
count or a date typed by hand, so a rebuild of the world cannot make this
verifier stale.

**The arithmetic, and why it needs no case analysis.** The marketplace feed
signs its own lines from the operator's side: the principal is negative because
that money leaves for the seller, the commission and the fulfilment fee are
positive because Copperline keeps them, and a refund is negative because it
hands part of the commission back. So what Copperline kept out of an order is
the sum of every line on it that is not the principal, exactly as the feed
signs it — one `sum`, no `CASE`, and no decision made here about which way a
refund points.

**The split between the two columns is not graded.** SS-1 defines
`commission_cents` and `fee_cents` and says the payout is the order value less
both of them. It does not say which of the two a refund comes off, and both
readings are honest: the refund reverses commission, so it can be netted there,
and the shipped model books it against the fee. The tests below grade the pair
together, and separately require the commission column to be either the posted
commission or the posted commission net of the refunds — which admits both
readings and convicts a split that moves money into a column SS-1 defines
another way.

**Why dbt runs here.** `marts.settlement_weekly` is built by a dbt model and
the world ships the landed half of the warehouse only (`AGENTS.md`, "The
warehouse"), so the table does not exist until dbt has made it. The `prepare`
step in checks.yaml builds it and its upstream — ten models, about a second.
This file runs dbt once more for the mart that reads it, and nothing here may
hold the warehouse open while it does: DuckDB takes one writer.
"""

from __future__ import annotations

import os
import subprocess

import duckdb
import pytest

from include.lib import warehouse, workspace_root

DB = str(warehouse.warehouse_path())

# Every name is qualified onto the live warehouse, for the reason
# `include/lib/warehouse.py` gives: the frozen `nwv` copy sits first on the
# search path and carries twelve tables under Copperline's own names.
ORDERS = warehouse.qualify("raw.marketplace_orders")
LINES = warehouse.qualify("raw.marketplace_settlements")
CALENDAR = warehouse.qualify("raw.fiscal_calendar")
SUMMARY = warehouse.qualify("marts.settlement_weekly")

#: The mart that reads the summary. `marts.dispute_daily` joins a week's GMV on
#: to every dispute day, so a summary that stops carrying a column it selects,
#: or stops building at all, takes that mart down with it.
READER = "marts.dispute_daily"

#: The pinned dbt. `dbt/README.md` says a bare `dbt` is either not on the PATH
#: or is not the pinned one. The override exists so this file can be exercised
#: against a tree on a laptop; nothing in a trial or in scoring sets it.
DBT = os.environ.get("DE_BENCH_DBT", "/opt/dbt-venv/bin/dbt")
PROJECT = str(workspace_root() / "dbt" / "copperline_analytics")

#: What Copperline kept out of each seller's fiscal week, and the figures that
#: say how the summary's columns may divide it. `take_cents` is every settlement
#: line that is not the principal, summed as the feed signs it.
#: `commission_cents` is the posted commission and `refund_cents` is the credit,
#: both signed the same way, so `commission_cents + refund_cents` is the
#: commission net of what was handed back.
#:
#: `gmv_once` is the week's order value counted once an order and `gmv_spread`
#: counts it once a payout the order's lines were split over. The summary is
#: built through a pivot that keys on the payout as well as the order, so a
#: refunded order reaches it twice — that is DQ-91's finding, this ticket puts
#: it out of scope, and `gmv_cents` is therefore graded as unmoved rather than
#: as right.
#:
#: The week and the seller come off the order, which is how the summary itself
#: reaches them. An order with no settlement lines against it kept nothing.
EXPECTED = f"""
WITH per_order AS (
    SELECT marketplace_order_id,
           sum(amount_cents) FILTER (WHERE line_type <> 'principal')  AS take_cents,
           sum(amount_cents) FILTER (WHERE line_type = 'commission')  AS commission_cents,
           sum(amount_cents) FILTER (WHERE line_type = 'refund')      AS refund_cents,
           count(DISTINCT (payout_id, currency_code))                 AS payouts
    FROM {LINES}
    GROUP BY 1
)
SELECT c.week_start,
       o.seller_id,
       sum(coalesce(p.take_cents, 0))::BIGINT       AS take_cents,
       sum(coalesce(p.commission_cents, 0))::BIGINT AS commission_cents,
       sum(coalesce(p.refund_cents, 0))::BIGINT     AS refund_cents,
       sum(o.gmv_cents)::BIGINT                     AS gmv_once,
       sum(o.gmv_cents * greatest(coalesce(p.payouts, 1), 1))::BIGINT AS gmv_spread
FROM {ORDERS} o
JOIN {CALENDAR} c ON c.cal_date = CAST(o.placed_at AS DATE)
LEFT JOIN per_order p ON p.marketplace_order_id = o.marketplace_order_id
GROUP BY 1, 2
"""

PUBLISHED = f"""
SELECT week_start, seller_id, commission_cents, fee_cents, gmv_cents
FROM {SUMMARY}
"""


def query(sql: str, params: list | None = None) -> list[tuple]:
    """One read, on its own connection. Nothing holds the file open: dbt wants
    the single writer and this runs between its invocations."""
    con = duckdb.connect(DB)
    try:
        return con.execute(sql, params or []).fetchall()
    finally:
        con.close()


def one(sql: str, params: list | None = None):
    rows = query(sql, params)
    return rows[0][0] if rows else None


def dbt(*args: str) -> subprocess.CompletedProcess:
    """Run the pinned dbt in the project directory, profile alongside."""
    return subprocess.run(
        [DBT, *args, "--profiles-dir", "."],
        cwd=PROJECT, capture_output=True, text=True, timeout=600, check=False,
    )


def summary_built() -> bool:
    try:
        query(f"SELECT 1 FROM {SUMMARY} LIMIT 1")
    except duckdb.Error:
        return False
    return True


def expected() -> dict:
    return {(row[0], row[1]): row[2:] for row in query(EXPECTED)}


def published() -> dict:
    return {(row[0], row[1]): row[2:] for row in query(PUBLISHED)}


@pytest.fixture(scope="session", autouse=True)
def built():
    """The summary has to exist before anything is graded. The prepare step
    builds it; if that failed, the model does not compile and that is the
    finding."""
    if not summary_built():
        result = dbt("run", "--select", "+settlement_weekly")
        assert summary_built(), (
            "FAIL: marts.settlement_weekly does not build, so there is no "
            f"summary to grade:\n{(result.stdout or '')[-2000:]}"
        )


# ---------------------------------------------------------------- the oracle

def test_the_feed_still_signs_its_lines_the_way_the_summary_reads_them():
    """The oracle checks itself against the world before it grades anything.

    Three properties of the landed feed hold this whole verifier up: the
    commission and the fulfilment fee sign positive, the principal and the
    refund sign negative, and there are refunds in the feed at all. If a rebuild
    of the world moves any of them, this says so rather than letting the tests
    below grade against a feed that has changed shape.
    """
    wrong_way = one(f"""
        SELECT count(*) FROM {LINES}
        WHERE (line_type IN ('commission', 'fulfilment_fee') AND amount_cents < 0)
           OR (line_type IN ('principal', 'refund') AND amount_cents > 0)
    """)
    assert wrong_way == 0, (
        f"FAIL: {wrong_way} settlement lines sign against the convention the "
        "feed states, so what Copperline kept can no longer be read off the sign"
    )

    refunds = one(f"SELECT count(*) FROM {LINES} WHERE line_type = 'refund'")
    assert refunds, "FAIL: the feed holds no refund lines, so nothing below grades anything"

    types = {row[0] for row in query(f"SELECT DISTINCT line_type FROM {LINES}")}
    assert types == {"principal", "commission", "fulfilment_fee", "refund"}, (
        f"FAIL: the feed carries the line types {sorted(types)}, and this "
        "verifier is written for the four the summary reads"
    )


def test_the_weeks_with_a_credit_can_tell_the_answers_apart():
    """The fixture's own guard. The summary has to hold both kinds of week, or
    a fix and the defect it replaces look the same from here."""
    rows = expected()
    with_credit = [key for key, value in rows.items() if value[2]]
    assert len(rows) - len(with_credit) >= 100, (
        f"FAIL: {len(rows) - len(with_credit)} seller-weeks hold no credit, so "
        "a summary that moved every week would pass unnoticed"
    )
    assert len(with_credit) >= 100, (
        f"FAIL: {len(with_credit)} seller-weeks hold a credit, which is too few "
        "to grade the thing this task is about"
    )


# --------------------------------------------------------------- the summary

def test_one_row_per_seller_per_fiscal_week():
    """SS-1's grain, and the population it covers. A summary that dropped the
    sellers with credits in them would pass the arithmetic below on what is
    left, so the set of weeks is graded before the figures on them."""
    rows, keys = query(
        f"SELECT count(*), count(DISTINCT (week_start, seller_id)) FROM {SUMMARY}"
    )[0]
    assert rows == keys, (
        f"FAIL: the summary holds {rows} rows for {keys} seller-weeks, and SS-1 "
        "is one row per seller per fiscal week"
    )
    missing = sorted(set(expected()) - set(published()))
    extra = sorted(set(published()) - set(expected()))
    assert not missing and not extra, (
        f"FAIL: {len(missing)} seller-weeks the marketplace feed has are not in "
        f"the summary and {len(extra)} are in it that the feed does not have. "
        f"First of each: {missing[:3]} {extra[:3]}"
    )


def test_the_two_held_back_columns_come_to_what_copperline_kept():
    """The ticket. `commission_cents` and `fee_cents` together are what was held
    back out of the seller's orders before the payout left, and a credit hands
    part of it back — so the pair comes down by the credit rather than up by it.

    Which of the two columns carries the credit is not graded here. See the
    module docstring.
    """
    want, got = expected(), published()
    wrong = [(key, got[key], want[key]) for key in sorted(want)
             if got[key][0] + got[key][1] != want[key][0]]
    off_by = sum(abs(got[key][0] + got[key][1] - want[key][0]) for key in want)
    assert not wrong, (
        f"FAIL: {len(wrong)} of {len(want)} seller-weeks hold back the wrong "
        f"amount, {off_by} cents in all. First three "
        f"(week and seller, published commission and fee, "
        f"kept and commission and credit from the feed): {wrong[:3]}"
    )


def test_the_commission_column_still_states_the_commission():
    """SS-1 names two held-back columns and this is one of them. Netting the
    credit here rather than against the fee is an honest reading and passes;
    emptying the column into the other one, or into `gmv_cents`, is not a
    reading of anything and fails."""
    want, got = expected(), published()
    wrong = [(key, got[key][0], want[key][1], want[key][1] + want[key][2])
             for key in sorted(want)
             if got[key][0] not in (want[key][1], want[key][1] + want[key][2])]
    assert not wrong, (
        f"FAIL: on {len(wrong)} seller-weeks `commission_cents` is neither the "
        "commission the settlement posted nor that commission net of the "
        "credits. First three (week and seller, published, posted, net): "
        f"{wrong[:3]}"
    )


def test_the_order_value_is_not_restated():
    """`gmv_cents` is the seller's order value, REV-14 owns it, and the ticket
    says it does not move. A summary that made the arithmetic work by taking the
    credit off the top line fails here.

    Two figures pass: the week's order value counted once an order, and the same
    value counted once a payout the order was split over, which is what the
    shipped pivot produces. The second is DQ-91's double count and this ticket
    puts it out of scope, so both are accepted and anything else is a
    restatement.
    """
    want, got = expected(), published()
    wrong = [(key, got[key][2], want[key][3], want[key][4]) for key in sorted(want)
             if got[key][2] not in (want[key][3], want[key][4])]
    assert not wrong, (
        f"FAIL: `gmv_cents` moved on {len(wrong)} seller-weeks. First three "
        f"(week and seller, published, order value, order value once a payout): "
        f"{wrong[:3]}"
    )


def test_the_mart_that_reads_the_summary_still_builds():
    """`marts.dispute_daily` selects a week's GMV out of this summary. A model
    that drops a column it reads, or stops building, takes that mart down with
    it — a larger break than the one the ticket is about."""
    result = dbt("run", "--select", f"+{READER.split('.')[-1]}")
    blob = (result.stdout or "") + (result.stderr or "")
    assert result.returncode == 0, (
        f"FAIL: the mart that reads settlement_weekly no longer builds:\n{blob[-2000:]}"
    )
    rows = one(f"SELECT count(*) FROM {warehouse.qualify(READER)}")
    assert rows, f"FAIL: {READER} built no rows"
