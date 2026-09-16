"""PAY-264 — does the backfill put every Halcyon settlement under the currency
it was taken in?

Never ships to the agent. It runs beside the scored tree, after
`halcyon_currency_backfill` has been run once, and reads what that run landed in
`ops.halcyon_settlement_currency`.

**No authored numbers.** Every expected result is computed in the same session
from `raw.pay_halcyon_settlements`, `raw.merchant_regions` and the shipped
`fixtures/reference/market_config.csv`. The answer is graded against the world's
own tables, never against a figure typed here.

**Why the market file rather than `raw.market_config`.** The oracle has to be
something the trial cannot move. `fixtures/**` is on the task's `do_not_modify`
list, so the file is the one copy of the market record a patch cannot touch; the
warehouse table it loads from is not. The two hold the same rows.

**The three readings the feed separates.**

    blank means dollars      right for every row before 2025-04-07 and wrong
                             after it. `docs/finance-policy.md` REV-7 says a
                             NULL currency is USD by construction, of a
                             different table, and it is the sentence that makes
                             this the plausible wrong answer.
    the market's country     `int_gl_postings_unified`, `int_orders_enriched`
      decides the currency   and `dim_geography` all hold a hand-written
                             region-to-currency map that reads CA as CAD. The
                             Canadian market bills USD —
                             `docs/runbooks/market-setup.md` MKT-1 says the
                             billing currency is a commercial decision and not a
                             geographical fact, and `raw.market_config` is where
                             it is recorded.
    the amount is dollars    `amount` is a two-decimal string. Read as a number
                             it is a hundred times too small.

`test_the_feed_still_separates_the_readings` is the oracle's own guard: it fails
loudly if a change to the world empties any of those populations, rather than
letting the rest of this file grade a distinction the data no longer makes.
"""

from __future__ import annotations

import duckdb
import pytest

# The scored tree is on PYTHONPATH (scoring.build_score_env), so the house
# library resolves the warehouse and the tree root the same way every DAG in the
# world does, whatever the working directory happens to be.
from include.lib import warehouse, workspace_root

DB = str(warehouse.warehouse_path())

# Every name is qualified onto the live warehouse, for the reason
# `include/lib/warehouse.py` gives: the frozen `nwv` copy sits first on the
# search path, so an unqualified read can answer out of a database that stopped
# refreshing in September 2025.
FEED = warehouse.qualify("raw.pay_halcyon_settlements")
REGIONS = warehouse.qualify("raw.merchant_regions")
LANDED = warehouse.qualify("ops.halcyon_settlement_currency")

#: The market record, read off the shipped file rather than out of the warehouse.
MARKETS = (
    "read_csv_auto('"
    + str(workspace_root() / "fixtures" / "reference" / "market_config.csv")
    + "')"
)

#: The currency each merchant account bills in: the account's region, and the
#: currency the market record says that region bills. Nothing here names a
#: currency.
ACCOUNT_CURRENCY = f"""
SELECT r.merchant_acct,
       upper(m.billing_currency) AS currency_code
FROM {REGIONS} r
JOIN {MARKETS} m ON m.market_code = r.region_code
WHERE r.valid_to IS NULL
"""

#: What the backfill is graded against, row by row: the currency the feed sent
#: where it sent one, the currency the merchant account bills where it did not,
#: the text amount parsed to cents, and the day the row settled.
TRUTH = f"""
SELECT h.txn_id,
       upper(coalesce(h.currency, a.currency_code))                    AS currency_code,
       cast(round(cast(h.amount AS DECIMAL(18,4)) * 100, 0) AS BIGINT) AS amount_cents,
       h.settled_on,
       h.currency IS NULL                                              AS was_blank,
       h.merchant_acct
FROM {FEED} h
LEFT JOIN ({ACCOUNT_CURRENCY}) a ON a.merchant_acct = h.merchant_acct
"""

#: What the run landed. Named columns only, so a table carrying more than the
#: ticket asked for is not convicted for it, and cast so that a currency in
#: lower case or a date held as text is judged on its value.
LANDED_ROWS = f"""
SELECT txn_id,
       upper(trim(currency_code))  AS currency_code,
       cast(amount_cents AS BIGINT) AS amount_cents,
       cast(settled_on AS DATE)     AS settled_on
FROM {LANDED}
"""


def query(sql: str, params: list | None = None) -> list[tuple]:
    """One read, on its own connection.

    Opened writable rather than read-only: DuckDB refuses a read-only open on a
    file that still has a write-ahead log to replay, and the backfill run that
    goes before this leaves one.
    """
    con = duckdb.connect(DB)
    try:
        return con.execute(sql, params or []).fetchall()
    finally:
        con.close()


def one(sql: str) -> tuple:
    return query(sql)[0]


@pytest.fixture(scope="session", autouse=True)
def landed_table_exists():
    """The run has to have made the table before anything else is worth saying."""
    schema, table = "ops", "halcyon_settlement_currency"
    found = one(
        "SELECT count(*) FROM duckdb_tables() "
        f"WHERE database_name = '{warehouse.warehouse_path().stem}' "
        f"AND schema_name = '{schema}' AND table_name = '{table}'"
    )[0]
    assert found, (
        f"FAIL: {schema}.{table} is not in the warehouse; the backfill landed nothing"
    )
    assert one(f"SELECT count(*) FROM {LANDED}")[0], (
        f"FAIL: {schema}.{table} is empty"
    )


def test_the_feed_still_separates_the_readings():
    """The oracle checks the world before it grades anything.

    Three things have to be true of the feed, or this file grades distinctions
    the data no longer makes and would pass a backfill that gets them wrong.
    """
    blank, filled = one(
        f"SELECT count(*) FILTER (WHERE currency IS NULL), "
        f"count(*) FILTER (WHERE currency IS NOT NULL) FROM {FEED}"
    )
    assert blank, "FAIL: no row in the feed arrives without a currency, so there is nothing to fill"
    assert filled, "FAIL: every row in the feed states its currency, so nothing checks the fill"

    spread = one(
        f"SELECT count(DISTINCT currency_code) FROM ({TRUTH}) WHERE was_blank"
    )[0]
    assert spread > 1, (
        "FAIL: every row that arrives blank belongs to one currency, so reading "
        "the blanks as dollars would grade the same as working them out"
    )

    disagree = one(f"""
        SELECT count(*) FROM (
            SELECT h.currency, a.currency_code
            FROM {FEED} h
            JOIN ({ACCOUNT_CURRENCY}) a ON a.merchant_acct = h.merchant_acct
            WHERE h.currency IS NOT NULL
              AND upper(h.currency) <> a.currency_code
        )
    """)[0]
    assert not disagree, (
        f"FAIL: {disagree} row(s) that state a currency disagree with the currency "
        "their merchant account bills, so the market record is no longer the route "
        "home and this file is grading the wrong answer"
    )

    unmapped = one(f"""
        SELECT count(*) FROM {FEED} h
        LEFT JOIN ({ACCOUNT_CURRENCY}) a ON a.merchant_acct = h.merchant_acct
        WHERE a.merchant_acct IS NULL
    """)[0]
    assert not unmapped, (
        f"FAIL: {unmapped} settlement(s) sit on a merchant account the market "
        "record cannot reach, so no fill can cover the feed"
    )

    decimals = one(
        f"SELECT count(*) FROM {FEED} WHERE amount NOT LIKE '%.00'"
    )[0]
    assert decimals, (
        "FAIL: every amount in the feed is a whole number of units, so reading "
        "the text as dollars would grade the same as parsing it to cents"
    )


def test_every_settlement_in_the_feed_has_a_row():
    """The ticket's population: the whole feed, whatever state a row ended in
    and whatever date it carries. A backfill scoped to the blank rows, to the
    days after the international launch, or to the settled ones leaves finance
    converting the rest by hand."""
    missing, total = one(f"""
        SELECT count(*) FILTER (WHERE l.txn_id IS NULL), count(*)
        FROM {FEED} h
        LEFT JOIN {LANDED} l ON l.txn_id = h.txn_id
    """)
    if missing:
        sample = [row[0] for row in query(f"""
            SELECT h.txn_id FROM {FEED} h
            LEFT JOIN {LANDED} l ON l.txn_id = h.txn_id
            WHERE l.txn_id IS NULL ORDER BY h.txn_id LIMIT 5
        """)]
        pytest.fail(
            f"FAIL: {missing} of {total} settlement(s) have no row, first few: {sample}"
        )


def test_one_row_per_settlement():
    """The grain. Two rows for one settlement is a join that fanned out, and
    finance sums this table."""
    rows, keys = one(f"SELECT count(*), count(DISTINCT txn_id) FROM {LANDED}")
    assert rows == keys, f"FAIL: {rows} rows over {keys} settlements"


def test_no_row_carries_a_null_currency():
    """The ticket says `currency_code` is never null. A null is how an amount
    reaches the conversion with nothing to convert it at."""
    empty = one(
        f"SELECT count(*) FROM {LANDED} WHERE currency_code IS NULL "
        "OR trim(currency_code) = ''"
    )[0]
    assert not empty, f"FAIL: {empty} row(s) landed with no currency"


def test_the_rows_the_feed_filled_keep_the_currency_the_feed_sent():
    """Two thirds of the feed states its currency and that statement stands. A
    fill that overwrites it with something else has replaced what the processor
    said with what the pipeline guessed."""
    wrong = query(f"""
        SELECT t.txn_id, l.currency_code, t.currency_code
        FROM ({TRUTH}) t JOIN ({LANDED_ROWS}) l USING (txn_id)
        WHERE NOT t.was_blank AND l.currency_code <> t.currency_code
        LIMIT 5
    """)
    count = one(f"""
        SELECT count(*) FROM ({TRUTH}) t JOIN ({LANDED_ROWS}) l USING (txn_id)
        WHERE NOT t.was_blank AND l.currency_code <> t.currency_code
    """)[0]
    assert not count, (
        f"FAIL: {count} settlement(s) that arrived with a currency landed under "
        f"another one, first few: {wrong}"
    )


def test_the_blank_rows_take_the_currency_their_merchant_account_bills():
    """The substance of the ticket.

    The row itself says nothing, so the currency comes off the merchant account:
    the account's region, and the currency the market record says that region
    bills. Reading the blanks as dollars is right for every row before the
    international launch and wrong for every pound and euro after it; taking the
    currency off the market's country instead is wrong for a market that bills
    in a currency that is not its own.
    """
    blank = one(f"SELECT count(*) FROM ({TRUTH}) WHERE was_blank")[0]
    bad = f"""
        SELECT count(*) FROM ({TRUTH}) t JOIN ({LANDED_ROWS}) l USING (txn_id)
        WHERE t.was_blank AND l.currency_code <> t.currency_code
    """
    count = one(bad)[0]
    if count:
        sample = query(f"""
            SELECT t.merchant_acct, l.currency_code, t.currency_code, count(*) AS n
            FROM ({TRUTH}) t JOIN ({LANDED_ROWS}) l USING (txn_id)
            WHERE t.was_blank AND l.currency_code <> t.currency_code
            GROUP BY 1, 2, 3 ORDER BY n DESC LIMIT 5
        """)
        pytest.fail(
            f"FAIL: {count} of {blank} settlement(s) that arrived without a currency "
            f"landed under the wrong one — account, landed, billed, rows: {sample}"
        )


def test_a_merchant_account_lands_one_currency():
    """The rule the ticket states in as many words: what is filled in has to be
    what that account's own filled rows already say.

    An account that comes out holding two currencies has had its blank rows
    filled from one source and its filled rows read from another, and the two
    do not agree. This is the check a person could run on their own answer.
    """
    split = query(f"""
        SELECT t.merchant_acct,
               string_agg(DISTINCT l.currency_code, ', ' ORDER BY l.currency_code) AS held
        FROM ({TRUTH}) t JOIN ({LANDED_ROWS}) l USING (txn_id)
        GROUP BY 1
        HAVING count(DISTINCT l.currency_code) > 1
        ORDER BY 1
    """)
    assert not split, (
        f"FAIL: {len(split)} merchant account(s) came out holding more than one "
        f"currency: {split[:5]}"
    )


def test_the_amount_is_the_feeds_amount_in_integer_cents():
    """`amount` is a two-decimal string — the one money column in the estate
    that lands as text. `CONVENTIONS.md` says money is an integer number of
    cents, so the string is parsed, not carried and not read as a number: read
    as a number it is a hundred times too small."""
    count = one(f"""
        SELECT count(*) FROM ({TRUTH}) t JOIN ({LANDED_ROWS}) l USING (txn_id)
        WHERE l.amount_cents IS DISTINCT FROM t.amount_cents
    """)[0]
    if count:
        sample = query(f"""
            SELECT t.txn_id, l.amount_cents, t.amount_cents
            FROM ({TRUTH}) t JOIN ({LANDED_ROWS}) l USING (txn_id)
            WHERE l.amount_cents IS DISTINCT FROM t.amount_cents
            ORDER BY t.txn_id LIMIT 5
        """)
        pytest.fail(
            f"FAIL: {count} settlement(s) landed the wrong amount — "
            f"txn, landed, the feed's amount in cents: {sample}"
        )


def test_the_settlement_date_is_the_day_the_row_settled():
    """`settled_on` is the day the money settled. The day the file reached us
    and the merchant's own local timestamp are both on the row and are neither
    of them that day."""
    count = one(f"""
        SELECT count(*) FROM ({TRUTH}) t JOIN ({LANDED_ROWS}) l USING (txn_id)
        WHERE l.settled_on IS DISTINCT FROM t.settled_on
    """)[0]
    if count:
        sample = query(f"""
            SELECT t.txn_id, l.settled_on, t.settled_on
            FROM ({TRUTH}) t JOIN ({LANDED_ROWS}) l USING (txn_id)
            WHERE l.settled_on IS DISTINCT FROM t.settled_on
            ORDER BY t.txn_id LIMIT 5
        """)
        pytest.fail(
            f"FAIL: {count} settlement(s) landed on the wrong day — "
            f"txn, landed, settled: {sample}"
        )
