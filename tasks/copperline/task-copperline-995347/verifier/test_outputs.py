"""CUS-509 — is the money on a Compass feature row the money that was true on
that row's own date?

Never ships to the agent. It runs beside the scored tree, with the tree root on
`PYTHONPATH`, so it resolves the warehouse the way every DAG in the world does.

**The defect.** `marts.feature_customer_daily` sums `net_sales_cents` off
`int_orders_enriched`, and that column is net of every return ever matched to
the order, whenever the customer asked for it. `docs/semantic-definitions.md`
defines it that way on purpose and four teams read it that way. Summing it into
a row dated before the customer asked puts a refund into a feature that could
not have been known on the row's date, which is what
`contracts/feature-store.md` §FS-1 forbids and the only thing this table exists
to avoid. Nothing fails: the grain holds, the contract check passes, and
`tie_feature_table_has_no_future` only guards the order date.

**No authored numbers.** Every expected result is computed in the same session.
The counts and `booked_cents_30d` come from `raw.orders` with the two filters
staging applies. The money comes from `int.int_net_sales_lines` for the line's
discounted value and `stg.stg_sales__returns` for the refund and the date the
customer asked for it — the two relations the world already keeps that pair in,
both under `dbt/copperline_analytics/models/shared/`, which the task holds shut.
The line's discounted value is a largest-remainder allocation of the order-level
discount; reproducing that allocation here would be copying a hundred lines of
somebody else's model in order to grade a date bound. Reading it is the honest
way, and no answer can edit the relation it is read from.

**What the oracle applies, and what it does not.** It applies one rule: a
refund is off the line from the day it was *initiated*, per
`int_net_sales_lines`'s own sentence, capped at what the line was worth because
a refund carries back the tax and the line is tax-exclusive. It does not copy
how the model reaches that answer. A fix that bounds the refunds at line grain,
one that bounds them off `first_return_date`, and one that rebuilds the netting
some third way all land the same figures and all pass.

**Two run dates, five graded days, and the ticket names none of them.**

    built at 2026-05-12   graded on 2026-05-12, 2026-05-04, 2026-04-28
    built at 2026-03-04   graded on 2026-03-04, 2026-02-22

Three of the five are days the run did not run on, which is the whole of the
ticket: a fix that bounds the refunds at the *run's* date rather than the
*row's* gets the top row of the spine right and every row under it wrong. The
second run date is ten weeks earlier and says the fix is not cut to a window.
All five sit outside the world's reserved windows (2026-06-01..09 and
2025-08-31..2025-10-04) and outside the never-grade tail (2026-06-10..14).

**Why dbt runs here, and why this file owns it.** The world ships the landed
half of the warehouse only (`AGENTS.md`, "The warehouse"), so
`marts.feature_customer_daily` does not exist until dbt has made it. A cold
warehouse holding `raw.*` and `ops.*` is all this file needs: it builds the
twenty models under the leaf itself, then rebuilds the leaf once per run date
and takes a copy of the graded days before the next build replaces the table.

The build is not run through a `prepare:` step, because a prepare failure is
tolerated and would leave every test below grading relations that were never
made — a build starved of the single DuckDB writer, or of room for the order
book, would read as a wrong answer and is not one. It is retried instead, the
later goes on the profile's larger `memory_limit`, and a build that still will
not go fails as a build with dbt's own words. Nothing may hold the warehouse
open while dbt has it: DuckDB takes one writer.
"""

from __future__ import annotations

import os
import subprocess
import time

import duckdb
import pytest

from include.lib import warehouse, workspace_root

DB = str(warehouse.warehouse_path())

# Every name is qualified onto the live warehouse, for the reason
# `include/lib/warehouse.py` gives: the frozen `nwv` copy sits first on the
# search path and carries twelve tables under Copperline's own names. `int` is
# a reserved word, so the schema is quoted before it is qualified.
ORDERS = warehouse.qualify("raw.orders")
INT_SCHEMA = warehouse.qualify('"int"')
STG_SCHEMA = warehouse.qualify("stg")
LINES = f"{INT_SCHEMA}.int_net_sales_lines"
RETURNS = f"{STG_SCHEMA}.stg_sales__returns"
DIM = warehouse.qualify("marts.dim_customer")
FEATURES = warehouse.qualify("marts.feature_customer_daily")

#: The pinned dbt. `dbt/README.md` says a bare `dbt` is either not on the PATH
#: or is not the pinned one. The override exists so this file can be exercised
#: against a tree on a laptop; nothing in a trial or in scoring sets it.
DBT = os.environ.get("DE_BENCH_DBT", "/opt/dbt-venv/bin/dbt")
PROJECT = str(workspace_root() / "dbt" / "copperline_analytics")

#: Run date -> the days of that run's spine this verifier grades. See the
#: module docstring for why three of the five are not the run's own date.
GRADED: dict[str, tuple[str, ...]] = {
    "2026-05-12": ("2026-05-12", "2026-05-04", "2026-04-28"),
    "2026-03-04": ("2026-03-04", "2026-02-22"),
}
DAYS = [(run, day) for run, days in GRADED.items() for day in days]

#: The columns graded, in the order the oracle returns them.
MONEY = ("net_sales_cents", "net_sales_cents_30d")
COUNTS = ("orders_30d", "orders_90d", "booked_cents_30d")

#: What a customer's money was, as of the row's own date and as the table has
#: it today. `pit` nets the refunds the customer had asked for by `day`;
#: `all_time` nets every refund ever matched to the line, which is what the
#: shipped model sums. The lookback is the one the model's own order window
#: uses — orders after the run date less 120 days — because the ticket says no
#: window moves.
MONEY_SQL = f"""
WITH lines AS (
    SELECT l.order_id, l.order_line_id, l.customer_ref AS customer_id,
           l.order_date, l.discounted_line_cents
    FROM {LINES} l
    WHERE l.customer_ref IS NOT NULL
      AND l.order_date > CAST(? AS DATE) - INTERVAL 120 DAY
      AND l.order_date <= CAST(? AS DATE)
),
asked_by_day AS (
    SELECT order_id, order_line_id, sum(refund_cents) AS refund_cents
    FROM {RETURNS}
    WHERE initiated_date <= CAST(? AS DATE)
    GROUP BY 1, 2
),
asked_ever AS (
    SELECT order_id, order_line_id, sum(refund_cents) AS refund_cents
    FROM {RETURNS}
    GROUP BY 1, 2
),
per_line AS (
    SELECT l.customer_id, l.order_date,
           l.discounted_line_cents
               - least(coalesce(b.refund_cents, 0), l.discounted_line_cents) AS pit,
           l.discounted_line_cents
               - least(coalesce(e.refund_cents, 0), l.discounted_line_cents) AS all_time
    FROM lines l
    LEFT JOIN asked_by_day b
           ON b.order_id = l.order_id AND b.order_line_id = l.order_line_id
    LEFT JOIN asked_ever e
           ON e.order_id = l.order_id AND e.order_line_id = l.order_line_id
)
SELECT customer_id,
       sum(pit)::HUGEINT                                                    AS net_sales_cents,
       coalesce(sum(pit) FILTER (
           order_date BETWEEN CAST(? AS DATE) - 29 AND CAST(? AS DATE)), 0)::HUGEINT
                                                                            AS net_sales_cents_30d,
       sum(all_time)::HUGEINT                                               AS leaked_net_sales_cents,
       coalesce(sum(all_time) FILTER (
           order_date BETWEEN CAST(? AS DATE) - 29 AND CAST(? AS DATE)), 0)::HUGEINT
                                                                            AS leaked_net_sales_cents_30d
FROM per_line
GROUP BY 1
"""

#: The three columns the ticket says do not move, straight off the landed
#: orders. Test orders and rows the OMS deleted are dropped in staging and
#: nowhere else, which is why they are here.
COUNTS_SQL = f"""
SELECT o.customer_ref                                                       AS customer_id,
       count(*) FILTER (
           o.local_order_date BETWEEN CAST(? AS DATE) - 29 AND CAST(? AS DATE))::BIGINT
                                                                            AS orders_30d,
       count(*) FILTER (
           o.local_order_date BETWEEN CAST(? AS DATE) - 89 AND CAST(? AS DATE))::BIGINT
                                                                            AS orders_90d,
       coalesce(sum(o.subtotal_cents) FILTER (
           o.local_order_date BETWEEN CAST(? AS DATE) - 29 AND CAST(? AS DATE)), 0)::HUGEINT
                                                                            AS booked_cents_30d
FROM {ORDERS} o
WHERE o.customer_ref IS NOT NULL
  AND NOT coalesce(o.is_test, false)
  AND o.deleted_at IS NULL
  AND o.local_order_date > CAST(? AS DATE) - INTERVAL 120 DAY
  AND o.local_order_date <= CAST(? AS DATE)
GROUP BY 1
"""


def query(sql: str, params: list | None = None) -> list[tuple]:
    """One read, on its own connection. Nothing holds the file open: dbt wants
    the single writer and this runs between its invocations."""
    con = duckdb.connect(DB)
    try:
        return con.execute(sql, params or []).fetchall()
    finally:
        con.close()


def dbt(*args: str, target: str = "dev", timeout: int = 420) -> subprocess.CompletedProcess:
    """Run the pinned dbt in the project directory, profile alongside.

    `prod` is the same warehouse file with a larger `memory_limit` — the
    profile says so in as many words — and it is the second thing this file
    tries when a build dies with room to blame.
    """
    return subprocess.run(
        [DBT, *args, "--profiles-dir", ".", "--target", target],
        cwd=PROJECT, capture_output=True, text=True, timeout=timeout, check=False,
    )


def dbt_until_it_builds(*args: str, tries: int = 3) -> str:
    """Run dbt until it succeeds, and say why it did not if it never does.

    The build is the one thing here that is not a property of the answer: it
    wants the single DuckDB writer and it wants room for the order book, and a
    container running many trials at once can deny it either. A tolerated
    failure would leave the tests grading relations that were never made, which
    reads as a wrong answer and is not one. So it is retried — the second and
    third goes take the profile's larger `memory_limit` — and a build that
    still will not go is reported as a build failure with dbt's own words.
    """
    trouble = ""
    for attempt in range(tries):
        result = dbt(*args, target="dev" if attempt == 0 else "prod")
        if result.returncode == 0:
            return ""
        trouble = (
            f"attempt {attempt + 1} of {tries} exited {result.returncode}\n"
            + ((result.stdout or "") + (result.stderr or ""))[-1500:]
        )
        time.sleep(5 * (attempt + 1))
    return trouble


def money(run: str, day: str) -> dict[str, tuple]:
    return {row[0]: row[1:] for row in query(MONEY_SQL, [run, day, day, day, day, day, day])}


def counts(run: str, day: str) -> dict[str, tuple]:
    return {row[0]: row[1:] for row in query(COUNTS_SQL, [day, day, day, day, day, day, run, day])}


def relation_exists(name: str) -> bool:
    try:
        query(f"SELECT 1 FROM {name} LIMIT 1")
    except duckdb.Error:
        return False
    return True


#: (run, day) -> {customer_id: row}, and (run, day) -> rows on that day. Filled
#: once, by the fixture, because each run replaces the table whole.
BUILT: dict[tuple[str, str], dict[str, tuple]] = {}
ROWS: dict[tuple[str, str], int] = {}
FAILURES: dict[str, str] = {}

#: Why the warehouse could not be stood up at all, if it could not. Every test
#: reads it first, so a build that never ran fails as a build rather than as an
#: answer.
COLD: str = ""


@pytest.fixture(scope="session", autouse=True)
def rebuilt():
    """Stand the warehouse up, then build the leaf once per run date.

    Nothing before this runs. The world ships `raw.*` and `ops.*` only, so the
    twenty models under `feature_customer_daily` are built here rather than by
    a step whose failure would be tolerated. Then the leaf is rebuilt at each
    run date: it is a table and every run replaces it whole, so a day has to be
    read before the next run date is built. The row count is kept beside the
    rows so that the grain can be graded on a day the warehouse no longer
    holds — a dictionary keyed on the account would swallow a duplicate.
    """
    global COLD

    if not relation_exists(ORDERS):
        COLD = f"the warehouse at {DB} holds no {ORDERS}, so there is nothing to build from"
        return

    COLD = dbt_until_it_builds("run", "--select", "+feature_customer_daily")
    if COLD:
        return
    absent = [name for name in (LINES, RETURNS, DIM, FEATURES) if not relation_exists(name)]
    if absent:
        COLD = f"dbt reported success and {', '.join(absent)} still does not exist"
        return

    columns = ", ".join(MONEY + COUNTS)
    for run, days in GRADED.items():
        trouble = dbt_until_it_builds(
            "run", "--select", "feature_customer_daily",
            "--vars", '{"ds": "%s"}' % run, tries=2)
        if trouble:
            FAILURES[run] = trouble
            continue
        for day in days:
            try:
                rows = query(
                    f"SELECT customer_id, {columns} FROM {FEATURES} WHERE ds = CAST(? AS DATE)",
                    [day],
                )
            except duckdb.Error as problem:
                # A renamed or dropped column lands here. It is a contract break
                # in its own right and the blame is better said than raised.
                FAILURES[run] = (
                    f"the feature table no longer answers for "
                    f"{', '.join(MONEY + COUNTS)}: {problem}"
                )
                break
            BUILT[(run, day)] = {row[0]: row[1:] for row in rows}
            ROWS[(run, day)] = len(rows)


def warehouse_stood_up() -> None:
    """Every test's first line. A cold warehouse is a build failure, not a
    wrong answer, and it says which of the two it is."""
    assert not COLD, f"FAIL: the warehouse could not be stood up:\n{COLD}"


def built(run: str, day: str) -> dict[str, tuple]:
    warehouse_stood_up()
    assert run not in FAILURES, (
        f"FAIL: the feature table does not build at ds={run}, so there is "
        f"nothing to grade:\n{FAILURES[run]}"
    )
    rows = BUILT.get((run, day))
    assert rows, f"FAIL: {day}: the feature table holds no row for that day after a run at ds={run}"
    return rows


# ------------------------------------------------------- the oracle's guards

def test_the_world_still_records_when_a_customer_asked_for_a_refund():
    """The oracle checks itself against the world before it grades anything.

    Two properties hold this file up: `stg_sales__returns` carries the date the
    customer asked, and the returns reach orders that name an account. If a
    rebuild of the world moves either, this says so rather than letting the
    tests below grade against a feed that has changed shape.
    """
    warehouse_stood_up()
    total, undated = query(
        f"SELECT count(*), count(*) FILTER (initiated_date IS NULL) FROM {RETURNS}")[0]
    assert total, "FAIL: the return feed is empty, so nothing below grades anything"
    assert undated == 0, f"FAIL: {undated} returns carry no initiated date"

    reachable = query(f"""
        SELECT count(*) FROM {RETURNS} r
        JOIN {LINES} l ON l.order_id = r.order_id AND l.order_line_id = r.order_line_id
        WHERE l.customer_ref IS NOT NULL
    """)[0][0]
    assert reachable, (
        "FAIL: no return in the world lands on an order that names an account, "
        "so the feature table cannot carry one either way"
    )


@pytest.mark.parametrize("run,day", DAYS)
def test_each_graded_day_can_tell_the_two_answers_apart(run, day):
    """A day only grades the fix if the two readings disagree on it. Each one
    has to hold at least one account whose money moves once the refunds the
    customer had not yet asked for come off it."""
    warehouse_stood_up()
    expected = money(run, day)
    moved = [
        customer for customer, row in expected.items()
        if (row[0], row[1]) != (row[2], row[3])
    ]
    assert moved, (
        f"FAIL: {day}: no account's money moves between the as-of answer and "
        "the all-time one, so this day cannot grade the fix"
    )


# -------------------------------------------------------------- the substance

@pytest.mark.parametrize("run,day", DAYS)
def test_the_money_on_a_row_is_the_money_that_was_true_on_its_date(run, day):
    """The whole of the ticket. Both net-sales columns on every graded row hold
    what the account had once the refunds it had asked for by that date are off
    it, and no others."""
    rows = built(run, day)
    expected = money(run, day)
    wrong = []
    for customer, want in expected.items():
        got = rows.get(customer)
        if got is None:
            continue
        for index, column in enumerate(MONEY):
            if got[index] != want[index]:
                wrong.append(
                    f"{customer} {column}={got[index]}, as of {day} it was "
                    f"{want[index]} (netting every refund ever gives {want[index + 2]})"
                )
    assert not wrong, f"FAIL: {day}: " + "; ".join(wrong[:8]) + f" [{len(wrong)} in all]"


@pytest.mark.parametrize("run,day", DAYS)
def test_the_counts_and_the_booked_money_did_not_move(run, day):
    """The ticket's third rule. An order counts on its order date and
    `booked_cents` never moves again, so both were already right, and a fix that
    reaches them has changed something it was told to leave alone."""
    rows = built(run, day)
    expected = counts(run, day)
    wrong = []
    for customer, want in expected.items():
        got = rows.get(customer)
        if got is None:
            continue
        for index, column in enumerate(COUNTS):
            if got[len(MONEY) + index] != want[index]:
                wrong.append(
                    f"{customer} {column}={got[len(MONEY) + index]}, the order "
                    f"book says {want[index]}"
                )
    assert not wrong, f"FAIL: {day}: " + "; ".join(wrong[:8]) + f" [{len(wrong)} in all]"


@pytest.mark.parametrize("run,day", DAYS)
def test_every_account_that_traded_still_has_its_row(run, day):
    """FS-2's grain, from one side. An account that ordered inside the window
    and lost its row is a hole in the training set that nothing complains about,
    and dropping the rows that moved is not a fix."""
    rows = built(run, day)
    ordered = set(counts(run, day))
    book = {row[0] for row in query(
        f"SELECT customer_id FROM {DIM} WHERE source_id_shape = 'current'")}
    missing = sorted((ordered & book) - set(rows))
    assert not missing, (
        f"FAIL: {day}: {len(missing)} account(s) that ordered inside the window "
        f"have no row, e.g. {', '.join(missing[:5])}"
    )


@pytest.mark.parametrize("run,day", DAYS)
def test_the_grain_is_still_one_row_per_account_per_day(run, day):
    """FS-2's grain, from the other side. A fix that reaches line grain and
    forgets to collapse it fans the table out, and every reader of the parquet
    then counts an account as many times as it has order lines."""
    rows = built(run, day)
    assert ROWS[(run, day)] == len(rows), (
        f"FAIL: {day}: the day holds {ROWS[(run, day)]} rows for {len(rows)} "
        "accounts; FS-2 pins one row per account per day"
    )
