"""DQ-91 — does `int_settlement_matched` say what the settlement feed says, and
does the tie still find a broken settlement line?

Never ships to the agent. It runs beside the scored tree, with the tree root on
`PYTHONPATH`, so it resolves the warehouse the way every DAG in the world does.

**No authored numbers.** Every expected result is computed from
`raw.marketplace_orders`, `raw.marketplace_settlements` and
`raw.marketplace_payouts` in the same session. Nothing here holds a cent, a row
count or a date typed by hand, so a fix is graded against the world's own feeds
and a rebuild of the world cannot make this verifier stale.

**The three feeds.** An order carries `gmv_cents`. Its settlement lines carry
signed amounts written from the operator's side. A payout carries
`net_paid_cents`, which is the platform's own sum of the lines it paid. The
first two are what the model reads; the third is the independent record it has
to agree with, and it is what makes `seller_net_cents` gradeable without anybody
deciding by hand what the seller was paid.

**Why dbt runs here.** `int_settlement_matched` is a view and the world ships
the landed half of the warehouse only (`AGENTS.md`, "The warehouse"), so the
relation does not exist until dbt has made it. The `prepare` step in checks.yaml
builds it and the two published marts that read it — nine models, about a
second. This file runs dbt again for the tie and for the mutation, and nothing
here may hold the warehouse open while it does: DuckDB takes one writer.
"""

from __future__ import annotations

import os
import subprocess

import duckdb
import pytest

from include.lib import warehouse, workspace_root

DB = str(warehouse.warehouse_path())

ORDERS = warehouse.qualify("raw.marketplace_orders")
LINES = warehouse.qualify("raw.marketplace_settlements")
PAYOUTS = warehouse.qualify("raw.marketplace_payouts")
MODEL = warehouse.qualify('"int".int_settlement_matched')

#: The two published marts that read the model. Both are built by the prepare
#: step and both are checked, because a model that stops carrying a column one of
#: them selects takes that mart down with it.
READERS = ("marts.gmv_daily", "marts.settlement_weekly")

#: The pinned dbt. `dbt/README.md` says a bare `dbt` is either not on the PATH
#: or is not the pinned one. The override exists so this file can be exercised
#: against a tree on a laptop; nothing in a trial or in scoring sets it.
DBT = os.environ.get("DE_BENCH_DBT", "/opt/dbt-venv/bin/dbt")
PROJECT = str(workspace_root() / "dbt" / "copperline_analytics")

#: The name of the tie under test.
TIE = "tie_gmv_settlement_reconstructs_order"

#: How far one principal line is moved for the mutation test. Large enough that
#: no tolerance a contract could set would swallow it, and it is a movement
#: rather than a value, so nothing about the world is authored by choosing it.
BREAK_CENTS = 500_000


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


def model_built() -> bool:
    try:
        query(f"SELECT 1 FROM {MODEL} LIMIT 1")
    except duckdb.Error:
        return False
    return True


@pytest.fixture(scope="session", autouse=True)
def built():
    """The model has to exist before anything is graded. The prepare step builds
    it; if that failed, the model does not compile and that is the finding."""
    if not model_built():
        result = dbt("run", "--select", "+int_settlement_matched")
        assert model_built(), (
            "FAIL: int_settlement_matched does not build, so nothing downstream "
            f"of it can be read:\n{(result.stdout or '')[-2000:]}"
        )


# ---------------------------------------------------------------- the oracle

def test_the_settlement_feed_still_puts_the_order_value_on_the_principal_line():
    """The oracle checks itself against the world before it grades anything.

    Three properties of the landed feeds hold this whole verifier up: every
    settled order has exactly one principal line, that line's magnitude is the
    order's `gmv_cents`, and no settlement line belongs to an order that has not
    landed. If a rebuild of the world moves any of them, this says so rather
    than letting the tests below grade against a feed that has changed shape.
    """
    not_one_principal = one(f"""
        SELECT count(*) FROM (
            SELECT marketplace_order_id FROM {LINES}
            GROUP BY 1 HAVING count(*) FILTER (WHERE line_type = 'principal') <> 1
        )
    """)
    assert not_one_principal == 0, (
        f"FAIL: {not_one_principal} settled orders do not carry exactly one principal line"
    )

    off_the_order = one(f"""
        SELECT count(*)
        FROM {LINES} s
        JOIN {ORDERS} o USING (marketplace_order_id)
        WHERE s.line_type = 'principal' AND abs(s.amount_cents) <> o.gmv_cents
    """)
    assert off_the_order == 0, (
        f"FAIL: {off_the_order} principal lines do not carry the order's gmv_cents, "
        "so the reconstruction this task is about no longer holds in the data"
    )

    orphans = one(f"""
        SELECT count(*) FROM {LINES} s
        WHERE NOT EXISTS (SELECT 1 FROM {ORDERS} o
                          WHERE o.marketplace_order_id = s.marketplace_order_id)
    """)
    assert orphans == 0, (
        f"FAIL: {orphans} settlement lines belong to no landed order, so the "
        "payout feed and the model cannot be compared total for total"
    )


# ----------------------------------------------------------------- the model

def test_one_row_per_marketplace_order():
    """The ticket's third rule. A refund posts with a later payout run than the
    commission it reverses, so a pivot keyed on the payout as well as the order
    puts every refunded order in this model twice — and every reader of it
    counts that order's GMV twice."""
    rows, ids = query(
        f"SELECT count(*), count(DISTINCT marketplace_order_id) FROM {MODEL}"
    )[0]
    orders = one(f"SELECT count(*) FROM {ORDERS}")
    assert (rows, ids) == (orders, orders), (
        f"FAIL: the model holds {rows} rows for {ids} orders, and {orders} "
        "marketplace orders have landed"
    )


def test_the_order_value_is_not_restated():
    """The ticket's fourth rule. `gmv_cents` is the seller's order value and the
    settlement lines do not get to redefine it."""
    moved = one(f"""
        SELECT count(*) FROM {MODEL} m
        JOIN {ORDERS} o USING (marketplace_order_id)
        WHERE m.gmv_cents <> o.gmv_cents
    """)
    assert moved == 0, f"FAIL: {moved} orders come out of the model with a different gmv_cents"


def test_an_order_counts_as_settled_when_it_has_settlement_lines():
    """A tie that passes because nothing is settled any more has not passed.
    `is_settled` is the test's own filter, so it is graded against the feed."""
    settled = one(f"SELECT count(*) FROM {MODEL} WHERE is_settled")
    with_lines = one(f"""
        SELECT count(*) FROM {ORDERS} o
        WHERE EXISTS (SELECT 1 FROM {LINES} s
                      WHERE s.marketplace_order_id = o.marketplace_order_id)
    """)
    assert settled == with_lines, (
        f"FAIL: the model calls {settled} orders settled and {with_lines} orders "
        "have settlement lines against them"
    )


def test_what_the_seller_kept_is_what_left_the_account():
    """`seller_net_cents`, order by order, against the feed's own arithmetic.

    The lines sign themselves from the operator's side, so what left Copperline
    for one order is the negative of the sum of that order's lines — which is
    exactly how the platform computes `net_paid_cents` on a payout. An order
    with no lines kept nothing.
    """
    wrong = query(f"""
        SELECT m.marketplace_order_id, m.seller_net_cents, coalesce(t.net_cents, 0)
        FROM {MODEL} m
        LEFT JOIN (
            SELECT marketplace_order_id, -sum(amount_cents) AS net_cents
            FROM {LINES} GROUP BY 1
        ) t USING (marketplace_order_id)
        WHERE m.seller_net_cents <> coalesce(t.net_cents, 0)
        ORDER BY 1 LIMIT 5
    """)
    total = one(f"""
        SELECT count(*) FROM {MODEL} m
        LEFT JOIN (
            SELECT marketplace_order_id, -sum(amount_cents) AS net_cents
            FROM {LINES} GROUP BY 1
        ) t USING (marketplace_order_id)
        WHERE m.seller_net_cents <> coalesce(t.net_cents, 0)
    """)
    assert not wrong, (
        f"FAIL: seller_net_cents is wrong on {total} orders. First five "
        f"(order, model, feed): {wrong}"
    )


def test_the_marketplace_total_ties_to_the_payout_feed():
    """The third feed, which the model never reads. Everything the model says
    the sellers kept, against everything the payout feed says was paid."""
    model_total = one(f"SELECT coalesce(sum(seller_net_cents), 0) FROM {MODEL}")
    paid = one(f"SELECT coalesce(sum(net_paid_cents), 0) FROM {PAYOUTS} WHERE payout_status = 'paid'")
    assert model_total == paid, (
        f"FAIL: the model totals {model_total} cents kept and the payout feed "
        f"totals {paid} cents paid"
    )


def test_the_marts_that_read_the_model_still_build():
    """`marts.gmv_daily` and `marts.settlement_weekly` both select from this
    model and both are published. A pivot that drops a column one of them reads
    takes that mart down with it, which is a larger break than the one the ticket
    is about."""
    result = dbt("run", "--select", "+gmv_daily", "+settlement_weekly")
    blob = (result.stdout or "") + (result.stderr or "")
    assert result.returncode == 0, (
        f"FAIL: the marts that read int_settlement_matched no longer build:\n{blob[-2000:]}"
    )
    for reader in READERS:
        rows = one(f"SELECT count(*) FROM {warehouse.qualify(reader)}")
        assert rows, f"FAIL: {reader} built no rows"


# ------------------------------------------------------------------- the tie

def test_the_nightly_tie_passes():
    """One test, selected by name, and it has to pass. A deleted tie selects
    nothing and fails here as surely as a failing one."""
    result = dbt("test", "--select", TIE)
    blob = (result.stdout or "") + (result.stderr or "")
    assert "PASS=1" in blob and result.returncode == 0, (
        f"FAIL: `dbt test --select {TIE}` did not pass one test:\n{blob[-2000:]}"
    )


def test_the_tie_still_notices_a_broken_settlement_line():
    """The ticket's second rule, and the only check that separates a tie which
    holds from a tie which stopped asking.

    One principal line is moved, the model is rebuilt over the moved feed and
    the tie is run again. It has to go red. A tie emptied to `where false`, one
    that compares a column against itself, and a model that takes the principal
    from the order header rather than from the line all survive every check
    above and die here. The line is put back afterwards whatever happens.
    """
    victim = one(f"""
        SELECT settlement_id FROM {LINES}
        WHERE line_type = 'principal' ORDER BY settlement_id LIMIT 1
    """)
    assert victim, "FAIL: no principal line in the feed to break"

    def move(delta: int) -> None:
        con = duckdb.connect(DB)
        try:
            con.execute(
                f"UPDATE {LINES} SET amount_cents = amount_cents + ? WHERE settlement_id = ?",
                [delta, victim],
            )
        finally:
            con.close()

    move(-BREAK_CENTS)
    try:
        dbt("run", "--select", "int_settlement_matched")
        result = dbt("test", "--select", TIE)
        blob = (result.stdout or "") + (result.stderr or "")
        assert result.returncode != 0, (
            f"FAIL: principal line {victim} was moved {BREAK_CENTS} cents off the "
            f"order it settles and {TIE} still passed:\n{blob[-2000:]}"
        )
    finally:
        move(BREAK_CENTS)
        dbt("run", "--select", "int_settlement_matched")
