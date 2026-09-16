"""CLS-207: the close's line economics, the promotion push-down and the returns
orphan count, after the line ordinal stops being treated as a line key.

Nothing here is an authored figure. Every expected number is read out of
`raw.orders`, `raw.order_lines`, `raw.returns` and `raw.promo_applications` in
the warehouse the scorer rebuilt from the image, in the same session that runs
the statements under test, so a tree edit cannot move the answer.

The three statements read tables the close builds upstream of them, and the
close cannot be run in this warehouse: `close_price_book` reads
`marts.dim_product`, which no job in the tree has ever built. So this file
builds those two inputs itself:

  * `staging.close_priced` from `raw.order_lines` and `raw.orders`, the same
    projection `close_apply_list_price` writes, with a NULL list price. Nothing
    under test reads the list price.
  * `staging.returns_landing` from `raw.returns`, the shape
    `returns_daily.dag.yaml` lands from the night's file, plus rows this file
    makes up so that the orphan count has something to find.

Both are written here rather than by running the tree's own statements: an
oracle that builds its input with the tree under test grades the answer with the
answer.

The first two tests grade the world, not the answer. If a later change makes
`order_line_id` unique, or leaves a return that no line answers, the seam this
task is about is gone and these say so loudly instead of letting the rest grade
something that no longer exists.
"""

from __future__ import annotations

import os
from pathlib import Path

import duckdb
import pytest

# The scorer lays the tree at /work and runs pytest there. The override exists
# so this file can be exercised against a tree on a laptop; nothing in a trial
# or in scoring sets it.
WORKDIR = Path(os.environ.get("DE_BENCH_WORKDIR") or "/work")
SQL = WORKDIR / "projects" / "commerce" / "sql"
DB = WORKDIR / os.environ.get("DUCKDB_PATH", "include/data/copperline.duckdb")

#: The two order days the close is replayed over. Both are ordinary trading
#: days outside the world's reserved windows, both carry returns raised against
#: them, and neither is named in the ticket: a fix that works has to work on a
#: day nobody pointed at.
ORDER_DAYS = ("2025-11-06", "2026-02-18")

#: The night of the returns feed the orphan count is run over. Ordinary, and
#: outside the reserved windows.
RETURN_NIGHT = "2026-02-24"


def connect() -> duckdb.DuckDBPyConnection:
    assert DB.exists(), f"no warehouse at {DB}"
    return duckdb.connect(str(DB))


def statement(name: str) -> str:
    """One commerce statement, as the DAG's `sql.read` would hand it over."""
    path = SQL / f"{name}.sql"
    assert path.exists(), f"{path} is gone; the close runs it every night"
    text = path.read_text(encoding="utf-8")
    assert text.strip(), f"{path} is empty"
    return text


def build_close_priced(con: duckdb.DuckDBPyConnection, ds: str) -> int:
    """The priced lines for one order day, as `close_apply_list_price` writes
    them. The list price is NULL because the price book comes off
    `marts.dim_product` and nothing in the tree builds it; no statement graded
    here reads that column."""
    con.execute("CREATE SCHEMA IF NOT EXISTS staging")
    con.execute(
        """CREATE OR REPLACE TABLE copperline.staging.close_priced AS
           SELECT l.order_id, l.order_line_id, l.sku, l.qty,
                  l.unit_price_cents, l.unit_cost_cents, l.line_discount_cents,
                  l.tax_cents, l.line_total_cents,
                  CAST(NULL AS BIGINT) AS list_price_cents
           FROM copperline.raw.order_lines l
           JOIN copperline.raw.orders o ON o.order_id = l.order_id
           WHERE o.local_order_date = ?""",
        [ds],
    )
    return int(con.execute("SELECT count(*) FROM copperline.staging.close_priced").fetchone()[0])


def disagreements(con: duckdb.DuckDBPyConnection, produced: str, expected_sql: str,
                  ds: str) -> list[tuple]:
    """Rows where what the statement wrote and what the raw tables say differ.

    Compared on the pair, both sides cast to the same width, so a column typed
    differently by a rewrite is not read as a wrong number.
    """
    return con.execute(
        f"""WITH got AS (
                SELECT order_id, order_line_id,
                       CAST(booked_cents AS DECIMAL(38,3))       AS booked_cents,
                       CAST(net_sales_cents AS DECIMAL(38,3))    AS net_sales_cents,
                       CAST(merch_margin_cents AS DECIMAL(38,3)) AS merch_margin_cents
                FROM {produced}
            ),
            want AS ({expected_sql})
            SELECT coalesce(g.order_id, w.order_id),
                   coalesce(g.order_line_id, w.order_line_id),
                   g.booked_cents, w.booked_cents,
                   g.net_sales_cents, w.net_sales_cents,
                   g.merch_margin_cents, w.merch_margin_cents
            FROM got g
            FULL OUTER JOIN want w
              ON w.order_id = g.order_id AND w.order_line_id = g.order_line_id
            WHERE g.order_id IS NULL OR w.order_id IS NULL
               OR g.booked_cents IS DISTINCT FROM w.booked_cents
               OR g.net_sales_cents IS DISTINCT FROM w.net_sales_cents
               OR g.merch_margin_cents IS DISTINCT FROM w.merch_margin_cents
            LIMIT 5""",
        [ds],
    ).fetchall()


#: What the day's line economics have to say, read out of the raw tables. The
#: arithmetic is the statement's own, unchanged; the match is on the pair the
#: source declares, which is the whole of the ticket.
EXPECTED_LINES = """
    SELECT p.order_id, p.order_line_id,
           CAST(p.line_total_cents AS DECIMAL(38,3))                     AS booked_cents,
           CAST(p.line_total_cents - coalesce(r.refund_cents, 0)
                AS DECIMAL(38,3))                                        AS net_sales_cents,
           CAST(p.line_total_cents - coalesce(r.refund_cents, 0)
                - round(p.qty * p.unit_cost_cents) AS DECIMAL(38,3))     AS merch_margin_cents
    FROM copperline.raw.order_lines p
    JOIN copperline.raw.orders o ON o.order_id = p.order_id
    LEFT JOIN (
        SELECT order_id, order_line_id, sum(refund_cents) AS refund_cents
        FROM copperline.raw.returns
        GROUP BY order_id, order_line_id
    ) r ON r.order_id = p.order_id AND r.order_line_id = p.order_line_id
    WHERE o.local_order_date = ?
"""


def test_the_line_number_still_repeats_across_orders():
    """The oracle checks the world before it grades anything.

    The whole task is that `order_line_id` is a line ordinal and not a line key.
    If the order book ever starts issuing a unique line id, every test below is
    grading a difference that no longer exists, and this one fails first.
    """
    con = connect()
    try:
        lines, ordinals, pairs = con.execute(
            """SELECT count(*), count(DISTINCT order_line_id),
                      count(DISTINCT (order_id, order_line_id))
               FROM copperline.raw.order_lines"""
        ).fetchone()
    finally:
        con.close()
    assert ordinals < 100, f"order_line_id holds {ordinals} distinct values; it is not an ordinal any more"
    assert pairs == lines, f"{lines} lines resolve to {pairs} distinct pairs; the pair is not the key"


def test_every_return_still_answers_to_exactly_one_line():
    """Matched on the pair, the returns feed has no orphans and fans out
    nowhere: one authorisation per line, one line per authorisation.

    That is what makes the expected net sales below well defined. It is also why
    the shipped orphan count reads as healthy, and why a repair that joins the
    feed onto the line without adding it up first comes out at the same numbers.
    """
    con = connect()
    try:
        orphans = con.execute(
            """SELECT count(*)
               FROM copperline.raw.returns r
               LEFT JOIN copperline.raw.order_lines l
                      ON l.order_id = r.order_id AND l.order_line_id = r.order_line_id
               WHERE l.order_id IS NULL"""
        ).fetchone()[0]
        fanned = con.execute(
            """SELECT count(*) FROM (
                   SELECT order_id, order_line_id FROM copperline.raw.order_lines
                   GROUP BY 1, 2 HAVING count(*) > 1)"""
        ).fetchone()[0]
        twice = con.execute(
            """SELECT count(*) FROM (
                   SELECT order_id, order_line_id FROM copperline.raw.returns
                   GROUP BY 1, 2 HAVING count(*) > 1)"""
        ).fetchone()[0]
    finally:
        con.close()
    assert orphans == 0, f"{orphans} returns in the feed answer to no line on the pair"
    assert fanned == 0, f"{fanned} pairs appear on more than one line"
    assert twice == 0, f"{twice} lines carry more than one authorisation"


@pytest.mark.parametrize("ds", ORDER_DAYS)
def test_the_close_nets_a_line_against_its_own_returns(ds: str):
    """`close_line_economics` over one order day, line for line.

    `net_sales_cents` is the line less the refunds raised against that line, and
    `merch_margin_cents` is that less the stated cost. Netting the feed's whole
    ordinal against the line — every L-01 refund in history off every L-01 line
    — is the state the ticket is about and it lands nowhere near this.
    """
    con = connect()
    try:
        priced = build_close_priced(con, ds)
        assert priced > 0, f"{ds} priced no lines; the day is not a trading day"
        con.execute(statement("close_line_economics"), [ds])
        wrong = disagreements(
            con, "copperline.staging.close_line_economics", EXPECTED_LINES, ds,
        )
    finally:
        con.close()
    assert not wrong, (
        f"{ds}: {len(wrong)} line(s) disagree with the order book and the returns feed "
        f"(order, line, booked got/want, net got/want, margin got/want): {wrong}"
    )


@pytest.mark.parametrize("ds", ORDER_DAYS)
def test_the_close_still_books_and_still_keeps_one_row_a_line(ds: str):
    """Two things the repair may not cost.

    `booked_cents` is what the close ties to `raw.orders` and it is the reason
    nobody saw this: it never moved. A repair that closes the gap by moving the
    booked figure breaks the tie the close publishes on.

    The grain is one row per priced line. A repair that joins the feed straight
    onto the line without adding it up first fans a line into one row per
    authorisation, and every figure downstream doubles.
    """
    con = connect()
    try:
        priced = build_close_priced(con, ds)
        con.execute(statement("close_line_economics"), [ds])
        rows, pairs = con.execute(
            """SELECT count(*), count(DISTINCT (order_id, order_line_id))
               FROM copperline.staging.close_line_economics"""
        ).fetchone()
        booked, ordered = con.execute(
            """SELECT (SELECT coalesce(sum(booked_cents), 0)
                       FROM copperline.staging.close_line_economics),
                      (SELECT coalesce(sum(l.line_total_cents), 0)
                       FROM copperline.raw.order_lines l
                       JOIN copperline.raw.orders o ON o.order_id = l.order_id
                       WHERE o.local_order_date = ?)""",
            [ds],
        ).fetchone()
    finally:
        con.close()
    assert rows == priced, f"{ds}: {priced} priced lines came out as {rows} rows"
    assert pairs == rows, f"{ds}: {rows} rows over {pairs} lines; the grain has fanned out"
    assert booked == ordered, f"{ds}: the close books {booked} cents against {ordered} on the lines"


@pytest.mark.parametrize("ds", ORDER_DAYS)
def test_the_promotion_push_down_only_moves_the_order_s_own_money(ds: str):
    """`close_apply_promotions` over one order day.

    The register cites the order and the ordinal, so a line takes what the
    register recorded against that line and nothing else. What the push-down
    adds is not this ticket's — the ticket says so — so the amount expected here
    is the amount the shipped statement already adds, landed on the line the
    register names instead of on every line that shares its ordinal.

    A header application carries no ordinal and matches no line, before the
    repair and after it. That is the gap MER-312 is for.
    """
    con = connect()
    try:
        build_close_priced(con, ds)
        con.execute(
            """CREATE OR REPLACE TABLE copperline.staging.priced_before AS
               SELECT order_id, order_line_id, line_discount_cents
               FROM copperline.staging.close_priced"""
        )
        con.execute(statement("close_apply_promotions"), [ds])
        wrong = con.execute(
            """WITH applied AS (
                   SELECT order_id, order_line_id, sum(discount_cents) AS discount_cents
                   FROM copperline.raw.promo_applications
                   WHERE order_id IN (SELECT order_id FROM copperline.raw.orders
                                      WHERE local_order_date = ?)
                     AND order_line_id IS NOT NULL
                   GROUP BY order_id, order_line_id
               )
               SELECT b.order_id, b.order_line_id, b.line_discount_cents,
                      p.line_discount_cents, coalesce(a.discount_cents, 0)
               FROM copperline.staging.priced_before b
               JOIN copperline.staging.close_priced p
                 ON p.order_id = b.order_id AND p.order_line_id = b.order_line_id
               LEFT JOIN applied a
                 ON a.order_id = b.order_id AND a.order_line_id = b.order_line_id
               WHERE p.line_discount_cents
                     IS DISTINCT FROM b.line_discount_cents + coalesce(a.discount_cents, 0)
               LIMIT 5""",
            [ds],
        ).fetchall()
        touched = con.execute(
            """SELECT count(*)
               FROM copperline.staging.priced_before b
               JOIN copperline.staging.close_priced p
                 ON p.order_id = b.order_id AND p.order_line_id = b.order_line_id
               WHERE p.line_discount_cents IS DISTINCT FROM b.line_discount_cents"""
        ).fetchone()[0]
    finally:
        con.close()
    assert not wrong, (
        f"{ds}: {len(wrong)} line(s) took a discount that is not the register's for that line "
        f"(order, line, before, after, that line's applications): {wrong}"
    )
    assert touched > 0, f"{ds}: the push-down moved nothing; the day's register has line applications"


def land_returns_night(con: duckdb.DuckDBPyConnection, ds: str) -> int:
    """One night of the returns feed in `staging.returns_landing`, as the
    intake step lands it, plus authorisations that answer to no line.

    The made-up rows are the point. The feed the world holds has no orphans, so
    a count that only ever sees the real file cannot tell a working check from a
    broken one. Each one cites a real order and a line ordinal that order does
    not have, which is the shape the ordinal join cannot see: the ordinal exists
    on some other order, so a match on it alone finds a line every time.
    """
    con.execute("CREATE SCHEMA IF NOT EXISTS staging")
    con.execute(
        """CREATE OR REPLACE TABLE copperline.staging.returns_landing AS
           SELECT rma_id, order_id, order_line_id, qty, initiated_at, received_at,
                  refund_cents, restock_flag
           FROM copperline.raw.returns
           WHERE initiated_at::DATE = ?""",
        [ds],
    )
    con.execute(
        """INSERT INTO copperline.staging.returns_landing
           SELECT 'RMA-ORPHAN-' || row_number() OVER (ORDER BY o.order_id),
                  o.order_id, 'L-12', 1.0,
                  CAST(? AS TIMESTAMP) + INTERVAL 9 HOUR, NULL, 100, false
           FROM (
               SELECT l.order_id
               FROM copperline.raw.order_lines l
               GROUP BY l.order_id
               HAVING count(*) = 1
               ORDER BY l.order_id
               LIMIT 3
           ) o""",
        [ds],
    )
    return int(
        con.execute("SELECT count(*) FROM copperline.staging.returns_landing").fetchone()[0]
    )


def test_the_orphan_check_finds_the_returns_that_answer_to_no_line():
    """`returns_orphan_check` over one night, with orphans in it.

    The statement's own comment says what it is for: an authorisation with no
    line behind it nets nothing off and overstates net revenue. Matched on the
    ordinal alone it finds a line for everything and cannot report anything but
    nought.
    """
    con = connect()
    try:
        landed = land_returns_night(con, RETURN_NIGHT)
        want = con.execute(
            """SELECT count(*)
               FROM copperline.staging.returns_landing r
               LEFT JOIN copperline.raw.order_lines l
                      ON l.order_id = r.order_id AND l.order_line_id = r.order_line_id
               WHERE l.order_id IS NULL AND r.initiated_at::DATE = ?""",
            [RETURN_NIGHT],
        ).fetchone()[0]
        got = con.execute(statement("returns_orphan_check"), [RETURN_NIGHT]).fetchone()[0]
    finally:
        con.close()
    assert landed > want > 0, f"the night landed {landed} rows of which {want} are orphans"
    assert got == want, f"the check counts {got} orphans in a night that holds {want}"


def test_the_orphan_check_passes_a_night_whose_returns_all_answer():
    """The same night without the made-up rows counts nothing.

    A count that reports every authorisation, or every one whose ordinal it
    cannot place, would pass the test above and fail here. The real feed
    resolves on the pair, every row of it.
    """
    con = connect()
    try:
        con.execute("CREATE SCHEMA IF NOT EXISTS staging")
        con.execute(
            """CREATE OR REPLACE TABLE copperline.staging.returns_landing AS
               SELECT rma_id, order_id, order_line_id, qty, initiated_at, received_at,
                      refund_cents, restock_flag
               FROM copperline.raw.returns
               WHERE initiated_at::DATE = ?""",
            [RETURN_NIGHT],
        )
        landed = con.execute(
            "SELECT count(*) FROM copperline.staging.returns_landing"
        ).fetchone()[0]
        got = con.execute(statement("returns_orphan_check"), [RETURN_NIGHT]).fetchone()[0]
    finally:
        con.close()
    assert landed > 0, f"{RETURN_NIGHT} landed no returns; the night is not a working night"
    assert got == 0, f"the check calls {got} of {landed} authorisations orphans on a clean night"
