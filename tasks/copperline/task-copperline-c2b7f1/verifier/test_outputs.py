"""DQ-93 — does `int_payment_matched` take the money from the book that was
authoritative, and does the tie still find a payment counted twice?

Never ships to the agent. It runs beside the scored tree, with the tree root on
`PYTHONPATH`, so it resolves the warehouse the way every DAG in the world does.

**No authored numbers.** Every expected result is computed from `raw.orders`,
`raw.payment_intents`, `raw.pay_meridian_settlements`, `raw.pay_halcyon_settlements`
and `raw.pay_processor_windows` in the same session. Nothing here holds a cent, a
row count or a date typed by hand — the two window dates come off the windows
table, which is the whole point of the ticket, so a rebuild of the world that
moves them moves this verifier with them.

**The rule.** Both processors fed settlements for the same payments through the
migration quarter. `raw.pay_processor_windows` holds the window each processor
was authoritative for, and `docs/runbooks/processor-migration.md` says to take
each row on its own date and leave the other book's copy alone. So the money on
an order is the Meridian events settled inside Meridian's window plus the Halcyon
transaction settled inside Halcyon's, and nothing else.

**Which orders are graded.** The order-to-attempt match is the shipped model's
and the ticket puts it out of scope, but its tie-break is arbitrary where two
attempts rank equal — about sixty thousand orders can match either of two
intents from one run to the next. Those orders are left out of the money
comparison, by the only property of them that is stable: how many attempts fall
in the window at all. Every wrong reading of the overlap is still convicted
hundreds of thousands of times over on what is left.

**Why dbt runs here.** `int_payment_matched` is a view and the world ships the
landed half of the warehouse only (`AGENTS.md`, "The warehouse"), so the relation
does not exist until dbt has made it. The `prepare` step in checks.yaml builds
it. This file runs dbt again for the published mart, for the tie and for the two
mutations, and nothing here may hold the warehouse open while it does: DuckDB
takes one writer.
"""

from __future__ import annotations

import datetime
import os
import subprocess

import duckdb
import pytest

from include.lib import warehouse, workspace_root

DB = str(warehouse.warehouse_path())

ORDERS = warehouse.qualify("raw.orders")
INTENTS = warehouse.qualify("raw.payment_intents")
MERIDIAN = warehouse.qualify("raw.pay_meridian_settlements")
HALCYON = warehouse.qualify("raw.pay_halcyon_settlements")
WINDOWS = warehouse.qualify("raw.pay_processor_windows")
MODEL = warehouse.qualify('"int".int_payment_matched')
STG_MERIDIAN = warehouse.qualify("stg.stg_payments__meridian_settlements")
STG_HALCYON = warehouse.qualify("stg.stg_payments__halcyon_settlements")

#: The published mart that selects nearly every column of the model. `fct_order`
#: reads four more of the same columns, so a pivot that drops one takes this mart
#: down first.
READER = "marts.fct_payments"

#: The pinned dbt. `dbt/README.md` says a bare `dbt` is either not on the PATH
#: or is not the pinned one. The override exists so this file can be exercised
#: against a tree on a laptop; nothing in a trial or in scoring sets it.
DBT = os.environ.get("DE_BENCH_DBT", "/opt/dbt-venv/bin/dbt")
PROJECT = str(workspace_root() / "dbt" / "copperline_analytics")

#: The name of the tie under test.
TIE = "tie_settlement_processor_overlap"

#: `macros/money.sql`, copied exactly. A Halcyon amount lands as text and the
#: cast happens once, in staging; the expectation has to make the same cent.
TO_CENTS = "cast(round(cast(amount as decimal(18, 4)) * 100, 0) as bigint)"


def query(sql: str, params: list | None = None) -> list[tuple]:
    """One read, on its own connection. Nothing holds the file open: dbt wants
    the single writer and this runs between its invocations."""
    con = duckdb.connect(DB)
    try:
        return con.execute(sql, params or []).fetchall()
    finally:
        con.close()


def write(sql: str, params: list | None = None) -> None:
    """One statement, on its own connection, closed before dbt is called."""
    con = duckdb.connect(DB)
    try:
        con.execute(sql, params or [])
    finally:
        con.close()


def one(sql: str, params: list | None = None):
    rows = query(sql, params)
    return rows[0][0] if rows else None


def dbt(*args: str) -> subprocess.CompletedProcess:
    """Run the pinned dbt in the project directory, profile alongside."""
    return subprocess.run(
        [DBT, *args, "--profiles-dir", "."],
        cwd=PROJECT, capture_output=True, text=True, timeout=900, check=False,
    )


# ------------------------------------------------------------------ the oracle
#
# One block of CTEs, used four times: for the oracle's own check, for the money
# comparison, for the same comparison after the boundary has been moved, and for
# choosing the transaction the second mutation duplicates. It reads the windows
# table rather than naming a date, so moving that table moves the expectation
# with it.

CTES = f"""
with win as (
    select
        processor,
        authoritative_from,
        coalesce(authoritative_to, date '9999-12-31')    as authoritative_to
    from {WINDOWS}
),
mw as (select * from win where processor = 'meridian'),
hw as (select * from win where processor = 'halcyon'),

-- The order spine, as `stg_sales__orders` draws it.
orders as (
    select
        order_id,
        order_ref,
        local_order_date                        as order_date,
        cast(grand_total_cents as bigint)       as grand_total_cents
    from {ORDERS}
    where not coalesce(is_test, false) and deleted_at is null
),

-- The attempts inside the seven-day window either side of the order. Where an
-- order has more than one, the shipped model picks between them on a key that
-- can tie, so that order is not graded on its money.
candidates as (
    select o.order_id, i.intent_id
    from orders o
    join {INTENTS} i
      on i.order_ref = o.order_ref
     and cast(i.created_at as date) between o.order_date - 7 and o.order_date + 7
),
candidate_count as (
    select order_id, count(*) as n, min(intent_id) as intent_id
    from candidates
    group by 1
),
graded as (
    select
        o.order_id, o.order_ref, o.order_date, o.grand_total_cents,
        c.intent_id
    from orders o
    left join candidate_count c on c.order_id = o.order_id
    where coalesce(c.n, 0) <= 1
),

-- Meridian, as `stg_payments__meridian_settlements` draws it: tombstones gone,
-- superseded events marked and dropped. Netted over the events settled inside
-- Meridian's own window.
events as (select * from {MERIDIAN} where deleted_at is null),
superseded as (
    select distinct restates_event_id as event_id
    from events
    where restates_event_id is not null
),
live_events as (
    select e.*
    from events e
    left join superseded s on s.event_id = e.event_id
    where s.event_id is null
),
meridian as (
    select
        e.intent_id,
        sum(case when e.event_type = 'captured'
                  and e.settlement_date between w.authoritative_from and w.authoritative_to
                 then abs(cast(e.amount_cents as bigint)) else 0 end)
      - sum(case when e.event_type = 'refunded'
                  and e.settlement_date between w.authoritative_from and w.authoritative_to
                 then abs(cast(e.amount_cents as bigint)) else 0 end)   as net_cents
    from live_events e
    cross join mw w
    group by 1
),

-- Halcyon, grouped and matched the way the shipped model matches it. One group
-- per reference and settlement date, so the pick cannot tie.
halcyon as (
    select
        merchant_ref                            as order_ref,
        settled_on                              as settled_date,
        sum(case when upper(status) = 'SETTLED' then {TO_CENTS} else 0 end)  as settled_cents,
        sum(case when upper(status) = 'REVERSED' then {TO_CENTS} else 0 end) as reversed_cents
    from {HALCYON}
    group by 1, 2
),
halcyon_best as (
    select * from (
        select
            o.order_id, h.settled_date, h.settled_cents, h.reversed_cents,
            row_number() over (partition by o.order_id order by h.settled_date) as pick
        from orders o
        join halcyon h
          on h.order_ref = o.order_ref
         and h.settled_date between o.order_date and o.order_date + 30
    ) r where pick = 1
),
halcyon_net as (
    select
        h.order_id,
        h.settled_date,
        case when h.settled_date between hw.authoritative_from and hw.authoritative_to
             then h.settled_cents - h.reversed_cents else 0 end         as net_cents
    from halcyon_best h
    cross join hw
)
"""

#: Per order, what the authoritative books add up to. Graded orders only.
EXPECTED = CTES + """
select
    g.order_id,
    g.order_ref,
    g.intent_id,
    g.grand_total_cents,
    h.settled_date                              as halcyon_settled_date,
    coalesce(m.net_cents, 0) + coalesce(h.net_cents, 0)  as expected_cents
from graded g
left join meridian m on m.intent_id = g.intent_id
left join halcyon_net h on h.order_id = g.order_id
"""

#: The most any order could settle for, over every attempt it could have been
#: matched to. Used by the oracle: it holds for every order, so no tie-break the
#: model happens to take can make a correct model's tie go red.
WORST = CTES + """
, best_attempt as (
    select c.order_id, max(coalesce(m.net_cents, 0)) as net_cents
    from candidates c
    left join meridian m on m.intent_id = c.intent_id
    group by 1
)
select count(*)
from orders o
left join best_attempt b on b.order_id = o.order_id
left join halcyon_net h on h.order_id = o.order_id
where coalesce(b.net_cents, 0) + coalesce(h.net_cents, 0) - o.grand_total_cents > 100
"""


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
        result = dbt("run", "--select", "+int_payment_matched")
        assert model_built(), (
            "FAIL: int_payment_matched does not build, so nothing downstream "
            f"of it can be read:\n{(result.stdout or '')[-2000:]}"
        )


def test_the_windows_table_is_still_two_windows_that_do_not_overlap():
    """The oracle checks itself against the world before it grades anything.

    Two processors, one window each, and the windows do not touch — that is what
    makes "which book was authoritative on this date" a question with one answer.
    If a rebuild of the world ever lands a third row or an overlap, this says so
    rather than letting the tests below grade against a record that has changed
    shape.
    """
    rows = query(f"SELECT processor, authoritative_from, authoritative_to FROM {WINDOWS} ORDER BY 1")
    assert len(rows) == 2, f"FAIL: the windows table holds {len(rows)} rows, not two"
    assert [r[0] for r in rows] == ["halcyon", "meridian"], (
        f"FAIL: the windows table names {[r[0] for r in rows]}"
    )
    assert rows[0][2] is not None, "FAIL: Halcyon's window has no end, so no boundary is recorded"
    overlapping = one(f"""
        SELECT count(*)
        FROM {WINDOWS} a JOIN {WINDOWS} b ON a.processor <> b.processor
        WHERE a.authoritative_from <= coalesce(b.authoritative_to, DATE '9999-12-31')
          AND b.authoritative_from <= coalesce(a.authoritative_to, DATE '9999-12-31')
    """)
    assert overlapping == 0, "FAIL: the two authoritative windows overlap"


def test_both_feeds_carry_rows_outside_their_own_window():
    """The overlap is real in the data, and the boundary sits inside it.

    Halcyon goes on delivering after its window closes and Meridian delivers
    before its own opens: that is the shadow traffic the ticket is about. If
    either half were empty there would be nothing to count twice.
    """
    halcyon_after, meridian_before = query(f"""
        SELECT
            (SELECT count(*) FROM {HALCYON} h, {WINDOWS} w
              WHERE w.processor = 'halcyon' AND h.settled_on > w.authoritative_to),
            (SELECT count(*) FROM {MERIDIAN} m, {WINDOWS} w
              WHERE w.processor = 'meridian' AND m.settlement_date < w.authoritative_from
                AND m.deleted_at IS NULL)
    """)[0]
    assert halcyon_after > 0 and meridian_before > 0, (
        f"FAIL: {halcyon_after} Halcyon rows land after its window and "
        f"{meridian_before} Meridian rows land before its own — the overlap this "
        "task is about is not in the feeds"
    )


def test_no_order_settles_for_more_than_it_was_placed_for():
    """The property the rewritten tie rests on, checked against the feeds.

    Once each row is taken from the book whose window holds its date, no order in
    the world settles above its own total — and that holds for every attempt the
    recycled reference could have matched, not only the one the model picks. If
    it ever stops being true of the built data, a tie asserting it cannot go
    green, and this says which of the two it is.
    """
    over = one(WORST)
    assert over == 0, (
        f"FAIL: {over} orders can settle for more than their own total once the "
        "windows are applied, so the feeds no longer support the assertion"
    )


# ------------------------------------------------------------------- the model

def test_one_row_per_live_order():
    """The grain the ticket pins. Every reader of this model joins it to the
    order spine, so an order that arrives twice is counted twice everywhere."""
    rows, ids = query(f"SELECT count(*), count(DISTINCT order_id) FROM {MODEL}")[0]
    orders = one(f"""
        SELECT count(*) FROM {ORDERS}
        WHERE NOT coalesce(is_test, false) AND deleted_at IS NULL
    """)
    assert (rows, ids) == (orders, orders), (
        f"FAIL: the model holds {rows} rows for {ids} orders, and {orders} live "
        "orders are on the spine"
    )


def test_the_order_total_is_not_restated():
    """`grand_total_cents` is the order's own money. Nothing about the
    processors gets to move it."""
    moved = one(f"""
        SELECT count(*) FROM {MODEL} m
        JOIN {ORDERS} o USING (order_id)
        WHERE m.grand_total_cents <> cast(o.grand_total_cents AS BIGINT)
    """)
    assert moved == 0, (
        f"FAIL: {moved} orders come out of the model with a different grand_total_cents"
    )


def test_the_money_is_the_authoritative_book_and_nothing_else():
    """`settled_net_cents`, order by order, against the two feeds and the windows.

    This is the whole of the fix. A model that adds both books together is wrong
    on every order the overlap touches. One that keeps a feed whole is wrong
    wherever that feed reaches. One that decides on the order's own date rather
    than on the date of the settlement row is wrong on the orders that straddle
    the boundary, which is the case the runbook spends its last paragraph on.
    """
    wrong = query(f"""
        SELECT m.order_id, m.settled_net_cents, e.expected_cents
        FROM {MODEL} m JOIN ({EXPECTED}) e USING (order_id)
        WHERE m.settled_net_cents <> e.expected_cents
        ORDER BY 1 LIMIT 5
    """)
    total = one(f"""
        SELECT count(*) FROM {MODEL} m JOIN ({EXPECTED}) e USING (order_id)
        WHERE m.settled_net_cents <> e.expected_cents
    """)
    assert not wrong, (
        f"FAIL: settled_net_cents is wrong on {total} orders. First five "
        f"(order, model, the authoritative books): {wrong}"
    )


def test_the_overlap_is_still_visible():
    """The ticket's fifth rule. A model that resolves the overlap by forgetting
    it has taken the reconciliation's subject away: `marts.recon_exceptions`
    reports the dual-fed orders off this flag and `marts.fct_payments` publishes
    it."""
    flagged, rows = query(
        f"SELECT count(*) FILTER (WHERE is_shadow_quarter), count(*) FROM {MODEL}"
    )[0]
    assert 0 < flagged < rows, (
        f"FAIL: is_shadow_quarter is true on {flagged} of {rows} rows, so it "
        "either marks nothing or marks everything"
    )


def test_the_staging_views_still_carry_the_whole_feed():
    """The ticket's fourth rule. Staging is 1:1 with the landed table and other
    teams read it — the ledger's card postings are built from these two views.
    Filtering the overlap out down here takes it out of everyone else's sight,
    which is a larger change than the one that was asked for."""
    halcyon_stg = one(f"SELECT count(*) FROM {STG_HALCYON}")
    halcyon_raw = one(f"SELECT count(*) FROM {HALCYON}")
    assert halcyon_stg == halcyon_raw, (
        f"FAIL: the Halcyon staging view returns {halcyon_stg} rows and the feed "
        f"landed {halcyon_raw}"
    )
    meridian_stg = one(f"SELECT count(*) FROM {STG_MERIDIAN}")
    meridian_raw = one(f"SELECT count(*) FROM {MERIDIAN} WHERE deleted_at IS NULL")
    assert meridian_stg == meridian_raw, (
        f"FAIL: the Meridian staging view returns {meridian_stg} rows and the feed "
        f"landed {meridian_raw} live ones"
    )


def test_the_mart_that_reads_the_model_still_builds():
    """`marts.fct_payments` selects nearly every column this model publishes and
    `marts.fct_order` selects four more. A pivot that drops one takes the mart
    down with it, which is a larger break than the one the ticket is about."""
    result = dbt("run", "--select", "fct_payments")
    blob = (result.stdout or "") + (result.stderr or "")
    assert result.returncode == 0, (
        f"FAIL: the mart that reads int_payment_matched no longer builds:\n{blob[-2000:]}"
    )
    rows = one(f"SELECT count(*) FROM {warehouse.qualify(READER)}")
    assert rows, f"FAIL: {READER} built no rows"


# --------------------------------------------------------------------- the tie

def test_the_nightly_tie_passes():
    """One test, selected by name, and it has to pass. A deleted tie selects
    nothing and fails here as surely as a failing one."""
    result = dbt("test", "--select", TIE)
    blob = (result.stdout or "") + (result.stderr or "")
    assert "PASS=1" in blob and result.returncode == 0, (
        f"FAIL: `dbt test --select {TIE}` did not pass one test:\n{blob[-2000:]}"
    )


def test_the_model_takes_the_boundary_from_the_windows_table():
    """The ticket's first rule, and the only check that separates a model which
    reads the record from one with the two dates typed into it.

    The boundary is moved back to the start of the month it falls in — the
    windows still do not overlap, and nothing else about the world is touched —
    the model is rebuilt, and `settled_net_cents` has to follow. The expectation
    is the same SQL reading the same table, so it moves too. The dates go back
    afterwards whatever happens.
    """
    before = query(
        f"SELECT processor, authoritative_from, authoritative_to FROM {WINDOWS} ORDER BY 1"
    )
    halcyon_to = [row[2] for row in before if row[0] == "halcyon"][0]
    moved_from = halcyon_to.replace(day=1)
    moved_to = moved_from - datetime.timedelta(days=1)
    assert moved_to < halcyon_to, "FAIL: the moved boundary is not earlier than the shipped one"

    def put(processor: str, column: str, value) -> None:
        write(f"UPDATE {WINDOWS} SET {column} = ? WHERE processor = ?", [value, processor])

    put("halcyon", "authoritative_to", moved_to)
    put("meridian", "authoritative_from", moved_from)
    try:
        dbt("run", "--select", "int_payment_matched")
        total = one(f"""
            SELECT count(*) FROM {MODEL} m JOIN ({EXPECTED}) e USING (order_id)
            WHERE m.settled_net_cents <> e.expected_cents
        """)
        assert total == 0, (
            f"FAIL: the boundary was moved to {moved_to} in raw.pay_processor_windows "
            f"and settled_net_cents did not follow it on {total} orders, so the "
            "model is not reading the record"
        )
    finally:
        for processor, from_date, to_date in before:
            put(processor, "authoritative_from", from_date)
            put(processor, "authoritative_to", to_date)
        dbt("run", "--select", "int_payment_matched")


def test_the_tie_still_notices_a_payment_settled_twice():
    """The ticket's sixth rule, and the only check that separates a tie which
    holds from a tie which stopped asking.

    One Halcyon transaction is settled a second time under a new id — the same
    order, the same day, inside the window Halcyon was authoritative for, on an
    order the other book claims as well. The money on that order goes above the
    order's own total and the tie has to go red. A tie emptied to `where false`,
    one that compares a column against itself and one that asserts nothing about
    the money all survive every check above and die here. The duplicate is
    removed afterwards whatever happens.
    """
    victim = query(f"""
        WITH e AS ({EXPECTED})
        SELECT h.txn_id
        FROM e
        JOIN {HALCYON} h
          ON h.merchant_ref = e.order_ref
         AND h.settled_on = e.halcyon_settled_date
         AND upper(h.status) = 'SETTLED'
        JOIN {WINDOWS} w ON w.processor = 'halcyon'
        WHERE e.halcyon_settled_date BETWEEN w.authoritative_from AND w.authoritative_to
          AND e.expected_cents > 10000
          AND EXISTS (SELECT 1 FROM {MERIDIAN} m WHERE m.intent_id = e.intent_id)
        ORDER BY h.txn_id
        LIMIT 1
    """)
    assert victim, "FAIL: no Halcyon transaction inside its own window to settle twice"
    txn_id = victim[0][0]
    copy_id = f"{txn_id}-DUP"

    write(
        f"INSERT INTO {HALCYON} SELECT * REPLACE (txn_id || '-DUP' AS txn_id) "
        f"FROM {HALCYON} WHERE txn_id = ?",
        [txn_id],
    )
    try:
        dbt("run", "--select", "int_payment_matched+")
        result = dbt("test", "--select", TIE)
        blob = (result.stdout or "") + (result.stderr or "")
        assert result.returncode != 0, (
            f"FAIL: Halcyon transaction {txn_id} was settled a second time and "
            f"{TIE} still passed:\n{blob[-2000:]}"
        )
    finally:
        write(f"DELETE FROM {HALCYON} WHERE txn_id = ?", [copy_id])
        dbt("run", "--select", "int_payment_matched")
