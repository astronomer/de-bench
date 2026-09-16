"""TRD-318 — does the patched statement build read both shapes of the export?

Never ships to the agent. It runs beside the scored tree, with the tree root on
`PYTHONPATH`, so it imports the patched module the same way the DAG would.

**Why a verifier rather than a DAG run.** `marts.trade_statement` is derived,
so a fresh warehouse does not carry it and the build makes it. Nothing else in
the world reads it. A `dag_runs` check would grade one period and cost a
scheduler; calling the build directly grades four periods in seconds. It also
removes the second reason a run would say nothing: a trial's patch never
carries anything under `include/data`, so a period an agent published by hand
is a period no check can read.

**No authored numbers.** Every expected result is read out of the parquet files
in `landing/orders_export/` in the same session, and every period boundary out
of `raw.fiscal_calendar`. A fix is graded against the world's own export, never
against a figure typed here.

**Four periods, on purpose.** Each is named by the day the DAG would fire, the
morning after the period closed.

    2025-11-30   FY2025-P10, 2025-11-02 to 2025-11-29. One shape throughout,
                 and the shape the shipped code was written for: the control
                 that a fix has not moved the periods that already worked.
    2026-01-04   FY2025-P11, 2025-11-30 to 2026-01-03. The export changed
                 shape inside it, on the thirty-third of its thirty-five days.
    2026-02-01   FY2025-P12, 2026-01-04 to 2026-01-31. The first whole period
                 in the new shape.
    2026-05-03   FY2026-P3, 2026-04-05 to 2026-05-02. Four periods later, and
                 named nowhere.

The ticket names none of the four. All four sit outside the world's reserved
windows.
"""

from __future__ import annotations

import datetime as dt

import duckdb
import pytest

# The scored tree is on PYTHONPATH (scoring.build_score_env), so the house
# library resolves the warehouse and the landing tree the same way every DAG in
# the world does, whatever the working directory happens to be.
from include.lib import landing_dir, warehouse

DB = str(warehouse.warehouse_path())
TABLE = "marts.trade_statement"

# Qualified onto the live warehouse, for the reason `include/lib/warehouse.py`
# gives: the frozen `nwv` copy sits first on the search path and carries a
# `raw.fiscal_calendar` of its own.
CALENDAR = warehouse.qualify("raw.fiscal_calendar")
MART = warehouse.qualify(TABLE)

#: Every export file on disk, the same glob the job reads.
EXPORT = str(landing_dir("orders_export") / "dt=*" / "*.parquet")

#: The run dates the fix is measured on. See the module docstring.
BEFORE = "2025-11-30"
ACROSS = "2026-01-04"
AFTER = "2026-02-01"
RECENT = "2026-05-03"
GRADED = (BEFORE, ACROSS, AFTER, RECENT)

#: The statement grain, and what the account was billed, read straight out of
#: the export. This is the expected result and there is nothing behind it.
#:
#: `union_by_name` puts both spellings of the account in scope whichever side
#: of the change the period sits on. A period the export did not carry the
#: promotion figure through carries none: every row in a file that has the
#: column carries a figure, nought included, so a NULL means a day the OMS
#: published nothing at all.
TRUTH = """
WITH period AS (
    SELECT coalesce(customer_id, customer_ref) AS account_id,
           currency_code,
           gross_cents,
           net_cents,
           promo_allocation_cents
    FROM read_parquet(?, union_by_name = true, hive_partitioning = true)
    WHERE channel = 'trade'
      AND dt BETWEEN ? AND ?
), carried AS (
    SELECT count(*) = count(promo_allocation_cents) AS whole_period
    FROM period
)
SELECT account_id,
       currency_code,
       count(*)::BIGINT         AS orders,
       sum(gross_cents)::BIGINT AS gross_cents,
       sum(net_cents)::BIGINT   AS net_cents,
       CASE WHEN (SELECT whole_period FROM carried)
            THEN sum(promo_allocation_cents)::BIGINT END
                                AS promo_allocation_cents
FROM period
GROUP BY 1, 2
ORDER BY 1, 2
"""

#: How much of a period each spelling of the account covers, and how much of it
#: the promotion column reaches. The fixture's own guard reads this.
SHAPES = """
SELECT count(*)::BIGINT                       AS rows_in_period,
       count(customer_id)::BIGINT             AS under_the_new_name,
       count(customer_ref)::BIGINT            AS under_the_old_name,
       count(promo_allocation_cents)::BIGINT  AS carrying_a_promotion
FROM read_parquet(?, union_by_name = true, hive_partitioning = true)
WHERE channel = 'trade'
  AND dt BETWEEN ? AND ?
"""


def query(sql: str, params: list | None = None) -> list[tuple]:
    """One read, on its own connection. DuckDB takes one writer and the build
    under test opens its own, so nothing here may hold the file open."""
    con = duckdb.connect(DB)
    try:
        return con.execute(sql, params or []).fetchall()
    finally:
        con.close()


def period_of(ds: str) -> tuple[str, dt.date, dt.date]:
    """`(fiscal_month, first day, last day)` for the period `ds - 1` falls in.

    The DAG's own convention, read from the shipped 4-5-4 calendar. Nothing
    here computes a fiscal date.
    """
    last_day = dt.date.fromisoformat(ds) - dt.timedelta(days=1)
    rows = query(
        f"""WITH here AS (
                SELECT fiscal_year, fiscal_period FROM {CALENDAR}
                WHERE cal_date = ?
            )
            SELECT c.fiscal_year || '-P' || lpad(c.fiscal_period::VARCHAR, 2, '0'),
                   min(c.cal_date),
                   max(c.cal_date)
            FROM {CALENDAR} c
            JOIN here h USING (fiscal_year, fiscal_period)
            GROUP BY 1""",
        [last_day],
    )
    assert rows, f"FAIL: {ds}: raw.fiscal_calendar has no row for {last_day}"
    return str(rows[0][0]), rows[0][1], rows[0][2]


def expected(ds: str) -> list[tuple]:
    """The period's statement, from the export files and the calendar."""
    month, first_day, last_day = period_of(ds)
    rolled = query(TRUTH, [EXPORT, first_day, last_day])
    return [(last_day, month, *row) for row in rolled]


def build(ds: str) -> None:
    """Run the patched build for one period, the way the DAG's task does."""
    from projects.finance.lib import trade_statement

    trade_statement.build_statement(ds)


def built(ds: str) -> list[tuple]:
    _month, _first_day, last_day = period_of(ds)
    columns = {row[0] for row in query(
        f"SELECT column_name FROM duckdb_columns() "
        f"WHERE database_name = ? AND schema_name = 'marts' AND table_name = ?",
        [warehouse.warehouse_path().stem, TABLE.split(".")[-1]],
    )}
    missing = sorted({"period_end", "fiscal_month", "account_id", "currency_code",
                      "orders", "gross_cents", "net_cents",
                      "promo_allocation_cents"} - columns)
    assert not missing, (
        f"FAIL: {TABLE} carries no {', '.join(missing)} column, so the desk has "
        "no line to read it off"
    )
    return query(
        f"""SELECT period_end, fiscal_month, account_id, currency_code, orders,
                   gross_cents, net_cents, promo_allocation_cents
            FROM {MART} WHERE period_end = ?
            ORDER BY account_id, currency_code""",
        [last_day],
    )


#: What each graded period came out as, built once and held. A build reads the
#: whole export tree and writes a few thousand rows, and four of the tests
#: below want the same four periods.
_STATEMENTS: dict[str, list[tuple]] = {}


def statement(ds: str) -> list[tuple]:
    """The period as the patched build leaves it. Built once per session."""
    if ds not in _STATEMENTS:
        build(ds)
        _STATEMENTS[ds] = built(ds)
    return _STATEMENTS[ds]


def check_period(ds: str) -> None:
    want = expected(ds)
    month, first_day, last_day = period_of(ds)
    assert want, f"FAIL: {ds}: {month} holds no trade orders, so nothing is graded"
    got = statement(ds)
    assert len(got) == len(want), (
        f"FAIL: {ds}: {month} ({first_day} to {last_day}) came out with "
        f"{len(got)} account line(s) and the export holds {len(want)}"
    )
    wrong = [(a, b) for a, b in zip(got, want) if a != b]
    assert not wrong, (
        f"FAIL: {ds}: {month} has {len(wrong)} line(s) that disagree with the "
        f"export; the first is {wrong[0][0]} against {wrong[0][1]}"
    )


def test_the_graded_periods_can_tell_a_wrong_answer_from_a_right_one():
    """The fixture's own guard. If the export ever stops changing shape inside
    FY2025-P11, or starts carrying the promotion before it, these periods grade
    nothing and this says so rather than passing quietly."""
    across = query(SHAPES, [EXPORT, *period_of(ACROSS)[1:]])[0]
    total, new_name, old_name, promo = across
    assert 0 < new_name < total and 0 < old_name < total, (
        "FAIL: the period the export changed shape inside no longer holds both "
        f"spellings of the account: {new_name} new, {old_name} old, {total} rows"
    )
    assert 0 < promo < total, (
        "FAIL: the promotion column no longer reaches part of that period: "
        f"{promo} of {total} rows carry it"
    )
    for ds, name in ((BEFORE, "before the change"), (AFTER, "after it"),
                     (RECENT, "four periods later")):
        total, new_name, old_name, promo = query(
            SHAPES, [EXPORT, *period_of(ds)[1:]])[0]
        assert total, f"FAIL: {ds}: the period {name} holds no trade orders"
        assert promo in (0, total), (
            f"FAIL: {ds}: the period {name} is no longer one shape throughout"
        )


def test_the_period_before_the_export_changed_shape():
    """The control: a period the shipped code already got right stays right."""
    check_period(BEFORE)


def test_the_period_the_export_changed_shape_inside():
    """The one that carries both shapes, and the one the desk is missing."""
    check_period(ACROSS)


def test_the_first_whole_period_after_the_change():
    check_period(AFTER)


def test_a_period_the_ticket_never_names():
    check_period(RECENT)


def test_no_line_goes_out_under_a_blank_account():
    """Every trade order in the export names an account. A statement line with
    no account on it is the period's money filed under nobody."""
    for ds in GRADED:
        blank = [row for row in statement(ds) if not row[2]]
        assert not blank, (
            f"FAIL: {ds}: {len(blank)} line(s) carry no account, the first for "
            f"{blank[0][6]} cents"
        )


def test_the_promotion_is_empty_where_the_export_did_not_carry_it_throughout():
    """The desk nets this figure off the account, so a zero and an empty cell
    are different claims. A period the OMS did not publish it right through
    carries no figure; a period it did carries the export's own total."""
    for ds in GRADED:
        month, first_day, last_day = period_of(ds)
        total, _new, _old, promo = query(SHAPES, [EXPORT, first_day, last_day])[0]
        published = [row[7] for row in statement(ds)]
        if promo == total:
            assert None not in published, (
                f"FAIL: {ds}: the export carried a promotion for every day of "
                f"{month}, and the statement leaves it empty"
            )
            assert sum(published) == query(
                "SELECT sum(promo_allocation_cents)::BIGINT FROM read_parquet("
                "?, union_by_name = true, hive_partitioning = true) "
                "WHERE channel = 'trade' AND dt BETWEEN ? AND ?",
                [EXPORT, first_day, last_day],
            )[0][0], f"FAIL: {ds}: {month}'s promotion does not tie to the export"
        else:
            offending = [value for value in published if value is not None]
            assert not offending, (
                f"FAIL: {ds}: the export carried a promotion on {promo} of "
                f"{total} rows in {month}, so no line may report one — "
                f"{len(offending)} do, the first at {offending[0]} cents"
            )


def test_rebuilding_a_period_leaves_one_copy_and_touches_no_other():
    """A repair that has to be run twice must not double a period, and a period
    is the partition: building the next one leaves this one alone."""
    once = statement(ACROSS)
    build(ACROSS)
    assert built(ACROSS) == once, f"FAIL: {ACROSS}: the second build changed the period"
    build(AFTER)
    assert built(ACROSS) == once, (
        f"FAIL: building {AFTER} moved the period that closed before it"
    )
