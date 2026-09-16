"""PLAT-471 — does the ledger post the card book that was authoritative, and
does `marts.cash_recon_daily` still report what the other book sent?

Never ships to the agent. It runs beside the scored tree, with the tree root on
`PYTHONPATH`, so it resolves the warehouse the way every DAG in the world does.

**No authored numbers.** Every expected result is computed from
`raw.pay_meridian_settlements`, `raw.pay_halcyon_settlements` and
`raw.pay_processor_windows` in the same session. Nothing here holds a cent, a
row count or a date typed by hand — the two window dates come off the windows
table, which is the whole point of the ticket, so a rebuild of the world that
moves them moves this file with them.

**The rule.** Both processors delivered settlement files for the same payments
through the migration quarter. `raw.pay_processor_windows` holds the window each
processor was authoritative for, and `docs/runbooks/processor-migration.md`
PRM-1 says to take each row on its own date and leave the other book's copy
alone; `docs/billing-integration.md` B-8 says a row outside its processor's
window is shadow traffic and is not counted. So the card subledger posts the
Meridian events whose settlement date falls inside Meridian's window and the
Halcyon transactions whose settlement date falls inside Halcyon's, two legs
each, and nothing else.

**Why the recon mart is graded too.** Today the ledger's card cash for a day and
`marts.cash_recon_daily`'s processor totals agree exactly, on every day of the
overlap — because both of them add the two books together. The agreement is a
tautology, and a fix that lands in the ledger alone turns it into a break. So
the mart's three day-grain money columns are graded against the same windows.

**Why dbt runs here.** `AGENTS.md` ("The warehouse") says the snapshot ships the
landed half only, so neither the ledger nor the mart exists until dbt has made
them. The `prepare` step in checks.yaml builds the chain. This file runs dbt
again for the published reader, for the nightly ties and for the mutation, and
nothing here may hold the warehouse open while it does: DuckDB takes one writer.
"""

from __future__ import annotations

import datetime
import os
import subprocess

import duckdb
import pytest

from include.lib import warehouse, workspace_root

DB = str(warehouse.warehouse_path())

MERIDIAN = warehouse.qualify("raw.pay_meridian_settlements")
HALCYON = warehouse.qualify("raw.pay_halcyon_settlements")
WINDOWS = warehouse.qualify("raw.pay_processor_windows")

MODEL = warehouse.qualify('"int".int_gl_postings_unified')
MART = warehouse.qualify("marts.cash_recon_daily")
POSTINGS = warehouse.qualify("marts.fct_gl_postings")

STG_MERIDIAN = warehouse.qualify("stg.stg_payments__meridian_settlements")
STG_HALCYON = warehouse.qualify("stg.stg_payments__halcyon_settlements")

#: The four books the ticket does not touch, and what each of them posts. Every
#: figure is a count read out of the staging layer in the same session; none is
#: typed. `ar_invoice` posts a line per invoice line, a tax line per invoice
#: whose lines carry tax, and a receivable line per invoice.
OTHER_BOOKS = {
    "ar_invoice": f"""
        select
            (select count(*)
               from {warehouse.qualify("stg.stg_finance__invoice_lines")} l
               join {warehouse.qualify("stg.stg_finance__invoices")} i
                 on i.invoice_id = l.invoice_id)
          + (select count(*)
               from {warehouse.qualify("stg.stg_finance__invoices")} i
               join (select invoice_id, sum(tax_cents) as tax_cents
                       from {warehouse.qualify("stg.stg_finance__invoice_lines")}
                      group by 1) t on t.invoice_id = i.invoice_id
              where t.tax_cents <> 0)
          + (select count(*)
               from {warehouse.qualify("stg.stg_finance__invoices")} i
               join (select invoice_id
                       from {warehouse.qualify("stg.stg_finance__invoice_lines")}
                      group by 1) t on t.invoice_id = i.invoice_id)
    """,
    "ar_credit": f"""
        select 2 * count(*)
          from {warehouse.qualify("stg.stg_finance__credit_memos")} m
          join {warehouse.qualify("stg.stg_finance__invoices")} i on i.invoice_id = m.invoice_id
    """,
    "gift_card": f"""
        select 2 * count(*)
          from {warehouse.qualify("stg.stg_sales__gift_card_ledger")} g
          join {warehouse.qualify("stg.stg_sales__gift_cards")} c on c.card_id = g.card_id
    """,
    "marketplace": f"""
        select 2 * count(*) from {warehouse.qualify("stg.stg_marketplace__settlements")}
    """,
}

#: The published mart that reads the postings and is not about cash at all. It
#: reads the revenue accounts, which no card row touches, so it is here to catch
#: a fix that took the postings model apart rather than filtered it.
READER = "revenue_entity_daily"

#: The two nightly ties over the ledger. The first is the reconciliation the
#: whole postings model exists to make possible.
TIES = ["tie_postings_balance_by_document", "tie_revenue_accounts_are_credits"]

#: The pinned dbt. `dbt/README.md` says a bare `dbt` is either not on the PATH
#: or is not the pinned one. The override exists so this file can be exercised
#: against a tree on a laptop; nothing in a trial or in scoring sets it.
DBT = os.environ.get("DE_BENCH_DBT", "/opt/dbt-venv/bin/dbt")
PROJECT = str(workspace_root() / "dbt" / "copperline_analytics")

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
# One block of CTEs, used four times: for the oracle's own check, for the card
# subledger, for the recon mart, and for both of those again after the boundary
# has been moved. It reads the windows table rather than naming a date, so
# moving that table moves the expectation with it.

CTES = f"""
with win as (
    select
        processor,
        authoritative_from,
        coalesce(authoritative_to, date '9999-12-31')   as authoritative_to
    from {WINDOWS}
),

-- Meridian, as `stg_payments__meridian_settlements` draws it: tombstones gone,
-- superseded events marked and dropped. Only the three event types the ledger
-- posts.
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

-- Every card row either book delivered, with the date the ledger posts it on
-- and the money it carries, and whether its own date falls inside its own
-- processor's window.
feed_rows as (
    select
        'meridian'                                                      as processor,
        e.event_id                                                      as row_ref,
        coalesce(e.settlement_date, cast(e.event_time_utc as date))     as posting_date,
        cast(e.amount_cents as bigint)                                  as net_cents
    from live_events e
    where e.event_type in ('captured', 'refunded', 'chargeback')

    union all

    select
        'halcyon',
        h.txn_id,
        coalesce(h.settled_on, h.file_date),
        case when upper(h.status) = 'SETTLED' then {TO_CENTS} else -{TO_CENTS} end
    from {HALCYON} h
    where upper(h.status) in ('SETTLED', 'REVERSED')
),
tagged as (
    select
        f.*,
        f.posting_date between w.authoritative_from and w.authoritative_to  as is_authoritative
    from feed_rows f
    join win w on w.processor = f.processor
)
"""

#: Every posting the card subledger should hold: two legs per authoritative row,
#: named the way the shipped model names them.
EXPECTED_REFS = CTES + """
select row_ref || '-CASH' as source_line_ref from tagged where is_authoritative
union all
select row_ref || '-CLR'  from tagged where is_authoritative
"""

#: Per day, what each book settled on the days it owned, and what the other book
#: sent for the same day.
EXPECTED_DAYS = CTES + """
select
    posting_date                                                            as ds,
    coalesce(sum(net_cents) filter (where processor = 'meridian' and is_authoritative), 0)
                                                                            as meridian_cents,
    coalesce(sum(net_cents) filter (where processor = 'halcyon' and is_authoritative), 0)
                                                                            as halcyon_cents,
    coalesce(sum(net_cents) filter (where not is_authoritative), 0)          as shadow_cents
from tagged
group by 1
"""

#: The mart's three day-grain money columns, one row per day. They are keyed on
#: the day alone, so a mart at the shipped grain collapses to one row per date.
MART_DAYS = f"""
select distinct ds, meridian_cents, halcyon_cents, shadow_overlap_cents
from {MART}
"""


def built() -> bool:
    try:
        query(f"SELECT 1 FROM {MODEL} LIMIT 1")
        query(f"SELECT 1 FROM {MART} LIMIT 1")
    except duckdb.Error:
        return False
    return True


@pytest.fixture(scope="session", autouse=True)
def chain_is_built():
    """The ledger and the mart have to exist before anything is graded. The
    prepare step builds them; if that failed, something in the chain does not
    compile and that is the finding."""
    if not built():
        result = dbt("run", "--select", "+cash_recon_daily")
        assert built(), (
            "FAIL: the postings chain does not build, so nothing downstream of "
            f"it can be read:\n{(result.stdout or '')[-2000:]}"
        )


# ---------------------------------------------------- the oracle checks itself

def test_the_windows_table_is_still_two_windows_that_do_not_overlap():
    """Two processors, one window each, and the windows do not touch — that is
    what makes "which book was authoritative on this date" a question with one
    answer. If a rebuild of the world ever lands a third row or an overlap, this
    says so rather than letting the tests below grade against a record that has
    changed shape.
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
    either half were empty there would be nothing posted twice.
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


# ------------------------------------------------------------------ the ledger

def test_the_staging_views_still_carry_the_whole_feed():
    """The ticket's rule about the layer below. Staging is 1:1 with the landed
    feed and other teams read it — commerce's settlement reconciliation is built
    on these two views and has its own ticket open on the same overlap. Cutting
    the shadow rows out down here takes them out of everyone else's sight, which
    is a larger change than the one that was asked for.
    """
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


def test_the_card_book_posts_exactly_the_authoritative_rows():
    """The whole of the ledger half, posting by posting.

    A model that leaves both books whole carries every shadow row and fails on
    the extras. One that drops a feed entirely, or takes the boundary as the end
    of the quarter rather than the windows table's, or decides on the payment
    rather than on the row's own date, is missing rows the authoritative book
    delivered. One that compares against `authoritative_to` without closing the
    open window loses every Meridian posting there is.
    """
    extra = one(f"""
        SELECT count(*) FROM (
            SELECT source_line_ref FROM {MODEL} WHERE subledger = 'card'
        ) a
        LEFT JOIN ({EXPECTED_REFS}) e USING (source_line_ref)
        WHERE e.source_line_ref IS NULL
    """)
    missing = one(f"""
        SELECT count(*) FROM ({EXPECTED_REFS}) e
        LEFT JOIN (
            SELECT source_line_ref FROM {MODEL} WHERE subledger = 'card'
        ) a USING (source_line_ref)
        WHERE a.source_line_ref IS NULL
    """)
    sample = query(f"""
        SELECT source_line_ref, posting_date, posting_note FROM {MODEL}
        WHERE subledger = 'card'
          AND source_line_ref NOT IN (SELECT source_line_ref FROM ({EXPECTED_REFS}))
        ORDER BY 1 LIMIT 5
    """)
    assert (extra, missing) == (0, 0), (
        f"FAIL: the card subledger posts {extra} rows the authoritative books do "
        f"not support and is missing {missing} they do. First five of the extras "
        f"(ref, date, note): {sample}"
    )


def test_the_card_money_follows_the_authoritative_book_day_by_day():
    """The same finding read as money rather than as rows, so a failure says how
    much. Cash is account 1000 and the card book's cash for a day is what the
    authoritative processor settled that day."""
    wrong = query(f"""
        SELECT a.ds, a.card_cash_cents, e.expected_cents
        FROM (
            SELECT posting_date AS ds, sum(signed_amount_cents) AS card_cash_cents
            FROM {MODEL} WHERE subledger = 'card' AND account_code = '1000'
            GROUP BY 1
        ) a
        JOIN (
            SELECT ds, meridian_cents + halcyon_cents AS expected_cents
            FROM ({EXPECTED_DAYS})
        ) e USING (ds)
        WHERE a.card_cash_cents <> e.expected_cents
        ORDER BY 1 LIMIT 5
    """)
    assert not wrong, (
        "FAIL: the card book's cash disagrees with the authoritative processors. "
        f"First five days (date, ledger, the authoritative books): {wrong}"
    )


def test_the_other_four_books_are_untouched():
    """Nothing outside the card book moves. Each figure is counted out of the
    staging layer in this session, so a world rebuilt at another scale grades
    against its own numbers."""
    for subledger, expectation in OTHER_BOOKS.items():
        expected = one(expectation)
        actual = one(f"SELECT count(*) FROM {MODEL} WHERE subledger = ?", [subledger])
        assert actual == expected, (
            f"FAIL: the {subledger} book posts {actual} rows and its source "
            f"supports {expected}"
        )


def test_the_ledger_still_balances():
    """Debits positive, credits negative, and a document sums to zero — the
    reconciliation the postings model exists to make possible. Both legs of a
    card row go together, so a fix that filtered the cash leg and left the
    clearing leg behind dies here. Both ties are selected by name and both have
    to run: a deleted tie selects nothing and fails here as surely as a failing
    one."""
    result = dbt("test", "--select", *TIES)
    blob = (result.stdout or "") + (result.stderr or "")
    assert f"PASS={len(TIES)}" in blob and result.returncode == 0, (
        f"FAIL: the {len(TIES)} nightly ties over the ledger no longer pass:"
        f"\n{blob[-3000:]}"
    )


def test_the_published_reader_still_builds():
    """`revenue_entity_daily` reads the postings for the revenue accounts, which
    no card row touches. A fix that took the model apart rather than cut rows
    out of one book takes this down with it."""
    result = dbt("run", "--select", READER)
    blob = (result.stdout or "") + (result.stderr or "")
    assert result.returncode == 0, (
        f"FAIL: the mart that reads the postings no longer builds:\n{blob[-2000:]}"
    )
    rows = one(f"SELECT count(*) FROM {warehouse.qualify('marts.' + READER)}")
    assert rows, f"FAIL: marts.{READER} built no rows"


# --------------------------------------------------------------- the recon mart

def test_the_mart_is_still_one_row_per_day_and_entity_and_currency():
    """The grain the ticket pins. The three money columns below are keyed on the
    day, so they repeat across a day's rows; if the grain has moved they no
    longer collapse to one row per date and the comparison would be grading
    something else.
    """
    days, combos = query(f"""
        SELECT count(DISTINCT ds), count(*) FROM ({MART_DAYS})
    """)[0]
    assert days == combos, (
        f"FAIL: {combos} distinct day figures over {days} days, so the day-grain "
        "money columns no longer hold one figure per day"
    )
    rows, grain = query(f"""
        SELECT count(*), count(DISTINCT (ds, entity, currency_key)) FROM {MART}
    """)[0]
    assert rows == grain, (
        f"FAIL: the mart holds {rows} rows over {grain} day-entity-currency keys"
    )


def test_the_mart_reports_the_authoritative_money_and_the_shadow_money():
    """The recon half of the fix.

    A mart left alone adds the two books together, so `meridian_cents` and
    `halcyon_cents` both carry a whole feed on every day of the overlap. One
    whose processor totals were cut to the window but whose overlap figure is
    still the smaller of the two collapses to zero on nearly every day of the
    quarter, because one of the two is now zero.
    """
    wrong = query(f"""
        SELECT a.ds, a.meridian_cents, e.meridian_cents,
               a.halcyon_cents, e.halcyon_cents,
               a.shadow_overlap_cents, e.shadow_cents
        FROM ({MART_DAYS}) a JOIN ({EXPECTED_DAYS}) e USING (ds)
        WHERE a.meridian_cents <> e.meridian_cents
           OR a.halcyon_cents <> e.halcyon_cents
           OR a.shadow_overlap_cents <> e.shadow_cents
        ORDER BY 1 LIMIT 5
    """)
    total = one(f"""
        SELECT count(*) FROM ({MART_DAYS}) a JOIN ({EXPECTED_DAYS}) e USING (ds)
        WHERE a.meridian_cents <> e.meridian_cents
           OR a.halcyon_cents <> e.halcyon_cents
           OR a.shadow_overlap_cents <> e.shadow_cents
    """)
    assert not wrong, (
        f"FAIL: the mart's day figures are wrong on {total} days. First five "
        "(date, meridian mart/expected, halcyon mart/expected, overlap "
        f"mart/expected): {wrong}"
    )


def test_every_day_the_processors_delivered_is_still_in_the_mart():
    """The mart's spine is the postings, so a day can only leave it if the
    ledger stopped posting anything at all for that day. Every day either
    processor was authoritative for has cash on it."""
    lost = query(f"""
        SELECT e.ds FROM ({EXPECTED_DAYS}) e
        LEFT JOIN (SELECT DISTINCT ds FROM {MART}) a USING (ds)
        WHERE a.ds IS NULL
          AND (e.meridian_cents <> 0 OR e.halcyon_cents <> 0)
        ORDER BY 1 LIMIT 5
    """)
    assert not lost, (
        f"FAIL: days the authoritative books settled money on are missing from "
        f"the mart: {lost}"
    )


def test_the_mart_and_the_ledger_agree_on_the_day():
    """The tie the mart exists for, which today is a tautology: the ledger's card
    cash for a day equals what the processor feeds say they settled that day,
    because both sides add the two books together. It has to still hold when
    both sides have been cut to the window — a fix that lands in the ledger
    alone breaks it, and so does one that lands in the mart alone.
    """
    wrong = query(f"""
        SELECT l.ds, l.card_cash_cents, m.meridian_cents + m.halcyon_cents
        FROM (
            SELECT posting_date AS ds, sum(signed_amount_cents) AS card_cash_cents
            FROM {MODEL} WHERE subledger = 'card' AND account_code = '1000'
            GROUP BY 1
        ) l
        JOIN ({MART_DAYS}) m USING (ds)
        WHERE l.card_cash_cents <> m.meridian_cents + m.halcyon_cents
        ORDER BY 1 LIMIT 5
    """)
    assert not wrong, (
        "FAIL: the ledger's card cash and the mart's processor totals no longer "
        f"describe the same money. First five days: {wrong}"
    )


# -------------------------------------------------------------- the record moves

def test_the_boundary_is_read_from_the_record():
    """The ticket's first rule, and the only check that separates a fix which
    reads the record from one with the two dates typed into it.

    The boundary is moved back to the start of the month it falls in — the
    windows still do not overlap, and nothing else about the world is touched —
    the chain is rebuilt, and both the ledger and the mart have to follow. The
    expectation is the same SQL reading the same table, so it moves too. The
    dates go back afterwards whatever happens.
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
        dbt("run", "--select", "+cash_recon_daily")
        stale_postings = one(f"""
            SELECT count(*) FROM (
                SELECT source_line_ref FROM {MODEL} WHERE subledger = 'card'
            ) a
            FULL OUTER JOIN ({EXPECTED_REFS}) e USING (source_line_ref)
            WHERE a.source_line_ref IS NULL OR e.source_line_ref IS NULL
        """)
        assert stale_postings == 0, (
            f"FAIL: the boundary was moved to {moved_to} in raw.pay_processor_windows "
            f"and the card subledger did not follow it — {stale_postings} postings "
            "disagree, so the ledger is not reading the record"
        )
        stale_days = one(f"""
            SELECT count(*) FROM ({MART_DAYS}) a JOIN ({EXPECTED_DAYS}) e USING (ds)
            WHERE a.meridian_cents <> e.meridian_cents
               OR a.halcyon_cents <> e.halcyon_cents
               OR a.shadow_overlap_cents <> e.shadow_cents
        """)
        assert stale_days == 0, (
            f"FAIL: the boundary was moved to {moved_to} and the recon mart did not "
            f"follow it on {stale_days} days, so the mart is not reading the record"
        )
    finally:
        for processor, from_date, to_date in before:
            put(processor, "authoritative_from", from_date)
            put(processor, "authoritative_to", to_date)
        dbt("run", "--select", "+cash_recon_daily")
