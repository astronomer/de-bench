"""CUS-517 — is the Compass feature file the whole trade-account book?

Never ships to the agent. It runs beside the scored tree, with the tree root on
`PYTHONPATH`, so it resolves the warehouse the way every DAG in the world does.

**The defect.** `marts.feature_customer_daily` takes its population from
`marts.dim_customer` under `where source_id_shape = 'current'`.
`source_id_shape` says which format an account's id arrived in — `current` for
a `C-######`, `northwave` for an `NWA-#####` that the merge review has not
folded onto a Copperline account. It is not a version, and the dimension is not
versioned: it carries no `valid_from` and no `valid_to` and holds one row per
account. So the filter is not the current-version cut that
`contracts/feature-store.md` §FS-1 forbids, and it is not FS-1 compliance
either. It drops the acquired book. The world ships it that way; nothing here
is planted.

It is silent by construction. FS-2's grain holds over a smaller population,
`check_contract` passes over a smaller population, and `check_as_of` compares
each row it finds against the dimension and never asks which rows are absent.

**What the fix has to be.** The dimension already resolves both books to one
row per account: a decided merge folds the Northwave account onto the
Copperline key and the Copperline row survives, so the `northwave` rows that
are left are exactly the accounts nobody has decided are somebody else. Reading
the dimension whole is the population and keeps the grain by construction.
Rebuilding the acquired half from `raw.nwv_accounts` instead puts the decided
merges back on the file as second rows, which is what
`test_the_day_is_the_account_book_exactly` convicts.

**No authored numbers.** Every expected result is computed in the same session.
The book comes from `marts.dim_customer`, which the task holds shut, and is
checked against `raw.customers`, `raw.nwv_accounts` and `ops.merge_candidates`
before anything is graded. The counts and the booked money come from
`raw.orders` with the two filters staging applies. The acquired half's
behavioural figures are not authored as zero: a guard reads the order book for
those ids first and the tests below only claim what it found.

**What this file does not grade, and why.** `net_sales_cents` and
`net_sales_cents_30d` on an account that has traded belong to CUS-509
(`task-copperline-995347`), which is open on the same model and is about the
date a refund comes off a line. Nothing here reads either column on a trading
account, so the two answers compose: this one adds rows, that one moves money
on the rows that were already there, and each passes over the other's tree.

**Two run dates, four graded days, and the ticket names none of them.**

    built at 2026-04-21   graded on 2026-04-21, 2026-04-08
    built at 2026-02-10   graded on 2026-02-10, 2026-01-28

Two of the four are days the run did not run on, and the second run date is ten
weeks earlier, so a population repaired for the top row of the spine or cut to
one window shows. The population itself does not vary by day — the dimension
has no validity window — so the second run date is insurance rather than a
second reading. All four sit outside the world's reserved windows
(2026-06-01..09 and 2025-08-31..2025-10-04) and outside the never-grade tail
(2026-06-10..14), and so does every window and lookback read for them.

**Why dbt runs here, and why this file owns it.** The world ships the landed
half of the warehouse only (`AGENTS.md`, "The warehouse"), so
`marts.feature_customer_daily` does not exist until dbt has made it. A cold
warehouse holding `raw.*` and `ops.*` is all this file needs: it builds the
models under the leaf itself, then rebuilds the leaf once per run date and
takes a copy of the graded days before the next build replaces the table.

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
# search path and carries twelve tables under Copperline's own names.
ORDERS = warehouse.qualify("raw.orders")
CUSTOMERS = warehouse.qualify("raw.customers")
NWV_ACCOUNTS = warehouse.qualify("raw.nwv_accounts")
MERGES = warehouse.qualify("ops.merge_candidates")
DIM = warehouse.qualify("marts.dim_customer")
FEATURES = warehouse.qualify("marts.feature_customer_daily")

#: The pinned dbt. `dbt/README.md` says a bare `dbt` is either not on the PATH
#: or is not the pinned one. The override exists so this file can be exercised
#: against a tree on a laptop; nothing in a trial or in scoring sets it.
DBT = os.environ.get("DE_BENCH_DBT", "/opt/dbt-venv/bin/dbt")
PROJECT = str(workspace_root() / "dbt" / "copperline_analytics")

#: Run date -> the days of that run's spine this verifier grades. The model's
#: spine is the fourteen days to the run date, so the second day of each pair
#: is a day the run did not run on.
GRADED: dict[str, tuple[str, ...]] = {
    "2026-04-21": ("2026-04-21", "2026-04-08"),
    "2026-02-10": ("2026-02-10", "2026-01-28"),
}
DAYS = [(run, day) for run, days in GRADED.items() for day in days]

#: The attributes graded, and where each comes from on the dimension. The
#: feature table coalesces the region the way the shipped model does; the
#: dimension has already coalesced it, so the two agree either way.
ATTRIBUTES = ("region_as_of", "status_as_of", "source_book")

#: The behavioural columns that must not move for an account that was already
#: on the file, and the trailing window each one covers. `net_sales_cents` and
#: `net_sales_cents_30d` are deliberately absent — see the module docstring.
COUNTS = ("orders_7d", "orders_30d", "orders_90d", "booked_cents_30d")

#: Every column read off a graded day, in one select.
COLUMNS = ATTRIBUTES + COUNTS + ("net_sales_cents", "net_sales_cents_30d",
                                 "last_order_date")

#: The account book as the customer master resolves it: one row per account
#: over both books, the acquired half being the accounts no decided merge folds
#: onto a Copperline one. The columns come back in `ATTRIBUTES` order behind
#: the shape, so a row here is (shape, region, status, book).
BOOK_SQL = f"""
SELECT customer_id, source_id_shape, coalesce(region_code, 'unknown'), status, source_book
FROM {DIM}
"""

#: The same population from the landed books, so the dimension is checked
#: rather than trusted. A privacy deletion takes a Copperline account out of
#: the master, which is what staging's `deleted_at` filter does; a decided
#: merge is one with a person's name against it.
RAW_BOOK_SQL = f"""
WITH decided AS (
    SELECT nwv_account_id, customer_id
    FROM {MERGES}
    WHERE decided_by IS NOT NULL AND customer_id IS NOT NULL
)
SELECT customer_id FROM {CUSTOMERS} WHERE deleted_at IS NULL
UNION
SELECT coalesce(d.customer_id, n.nwv_account_id)
FROM {NWV_ACCOUNTS} n
LEFT JOIN decided d ON d.nwv_account_id = n.nwv_account_id
"""

#: The three counts and the booked money, straight off the landed orders. Test
#: orders and rows the OMS deleted are dropped in staging and nowhere else,
#: which is why they are here. The lookback is the model's own order window.
COUNTS_SQL = f"""
SELECT o.customer_ref                                                       AS customer_id,
       count(*) FILTER (
           o.local_order_date BETWEEN CAST(? AS DATE) - 6 AND CAST(? AS DATE))::BIGINT
                                                                            AS orders_7d,
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
    reads as a wrong answer and is not one.
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


def counts(run: str, day: str) -> dict[str, tuple]:
    return {row[0]: row[1:] for row in query(COUNTS_SQL, [day, day, day, day, day, day, day, day, run, day])}


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

#: The account book, keyed on the account: shape, region, status, book. Read
#: once, after the dimension is built and before the leaf is rebuilt.
BOOK: dict[str, tuple] = {}

#: Why the warehouse could not be stood up at all, if it could not. Every test
#: reads it first, so a build that never ran fails as a build rather than as an
#: answer.
COLD: str = ""


@pytest.fixture(scope="session", autouse=True)
def rebuilt():
    """Stand the warehouse up, then build the leaf once per run date.

    Nothing before this runs. The world ships `raw.*` and `ops.*` only, so the
    models under `feature_customer_daily` are built here rather than by a step
    whose failure would be tolerated. Then the leaf is rebuilt at each run
    date: it is a table and every run replaces it whole, so a day has to be
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
    absent = [name for name in (DIM, FEATURES) if not relation_exists(name)]
    if absent:
        COLD = f"dbt reported success and {', '.join(absent)} still does not exist"
        return

    try:
        BOOK.update({row[0]: row[1:] for row in query(BOOK_SQL)})
    except duckdb.Error as problem:
        COLD = f"the customer master no longer answers for the account book: {problem}"
        return

    columns = ", ".join(COLUMNS)
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
                    f"{', '.join(COLUMNS)}: {problem}"
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
        f"FAIL: nothing could be read off the feature table at ds={run}, so "
        f"there is nothing to grade:\n{FAILURES[run]}"
    )
    rows = BUILT.get((run, day))
    assert rows, f"FAIL: {day}: the feature table holds no row for that day after a run at ds={run}"
    return rows


def acquired() -> set[str]:
    """The accounts the shipped model drops: on the master, on the acquired
    book, and folded onto no Copperline account."""
    return {account for account, row in BOOK.items() if row[0] == "northwave"}


def at(row: tuple, column: str):
    return row[COLUMNS.index(column)]


# ------------------------------------------------------- the oracle's guards

def test_the_master_resolves_both_books_to_one_row_an_account():
    """The oracle checks itself against the landed books before it grades
    anything.

    Three properties hold this file up: the master is one row per account, its
    population is the two books with the decided merges folded together, and
    the acquired half it leaves is not empty. If a rebuild of the world moves
    any of them, this says so rather than letting the tests below grade against
    a book that has changed shape.
    """
    warehouse_stood_up()
    assert BOOK, "FAIL: the customer master is empty, so nothing below grades anything"

    duplicates = query(
        f"SELECT customer_id, count(*) FROM {DIM} GROUP BY 1 HAVING count(*) > 1 LIMIT 5")
    assert not duplicates, (
        f"FAIL: the customer master carries {len(duplicates)}+ accounts twice, "
        f"e.g. {duplicates[:3]}; the population below is not well defined"
    )

    from_raw = {row[0] for row in query(RAW_BOOK_SQL)}
    assert from_raw == set(BOOK), (
        f"FAIL: the master and the landed books disagree on the population: "
        f"{len(from_raw - set(BOOK))} account(s) on the books and not on the "
        f"master, {len(set(BOOK) - from_raw)} the other way"
    )

    left = acquired()
    assert left, (
        "FAIL: no account on the master carries the acquired book's id shape, "
        "so there is nothing for this ticket to put on the file"
    )


def test_the_acquired_accounts_have_never_traded_in_the_copperline_order_book():
    """What the acquired half's behavioural figures are is read, not assumed.

    Northwave's own trade froze into the `nwv` database at the acquisition and
    never landed in `raw.orders`. So an acquired account's trailing windows are
    empty, and that is what the test below claims — but it claims it because
    this ran first, not because a zero was typed into this file.
    """
    warehouse_stood_up()
    traded = query(f"""
        SELECT DISTINCT o.customer_ref
        FROM {ORDERS} o
        JOIN {DIM} d ON d.customer_id = o.customer_ref
        WHERE d.source_id_shape = 'northwave'
        LIMIT 5
    """)
    assert not traded, (
        "FAIL: the order book now carries orders against the acquired ids "
        f"({[row[0] for row in traded]}), so their trailing windows are no "
        "longer empty and this file grades the wrong figures"
    )


# -------------------------------------------------------------- the substance

@pytest.mark.parametrize("run,day", DAYS)
def test_the_day_is_the_account_book_exactly(run, day):
    """The whole of the ticket. Every account the customer master holds is on
    the day, over both books, and no id the master does not hold is.

    Missing convicts the filter left standing, the filter inverted, and an
    acquired account re-keyed onto a Copperline id. Extra convicts an acquired
    half rebuilt from `raw.nwv_accounts`, which puts the decided merges back as
    second rows under their Northwave ids.
    """
    rows = built(run, day)
    missing = sorted(set(BOOK) - set(rows))
    extra = sorted(set(rows) - set(BOOK))
    trouble = []
    if missing:
        trouble.append(
            f"{len(missing)} account(s) on the master have no row, e.g. "
            f"{', '.join(missing[:5])}")
    if extra:
        trouble.append(
            f"{len(extra)} row(s) name an account the master does not hold, "
            f"e.g. {', '.join(extra[:5])}")
    assert not trouble, f"FAIL: {day}: " + "; ".join(trouble)


@pytest.mark.parametrize("run,day", DAYS)
def test_the_grain_is_still_one_row_per_account_per_day(run, day):
    """FS-2's grain. An acquired half unioned on at the wrong grain, or joined
    through the merge table without collapsing, fans the table out, and every
    reader of the parquet then counts an account twice."""
    rows = built(run, day)
    assert ROWS[(run, day)] == len(rows), (
        f"FAIL: {day}: the day holds {ROWS[(run, day)]} rows for {len(rows)} "
        "accounts; FS-2 pins one row per account per day"
    )


@pytest.mark.parametrize("run,day", DAYS)
def test_every_row_carries_the_attributes_the_master_holds(run, day):
    """The ticket's fourth rule. The attributes are the master's, on both
    books, so an acquired half rebuilt from the landed account book — which
    carries the source system's own status vocabulary rather than the three
    values `contracts/customer_360.yml` pins — is convicted here as well."""
    rows = built(run, day)
    wrong = []
    for account, want in BOOK.items():
        got = rows.get(account)
        if got is None:
            continue
        for index, column in enumerate(ATTRIBUTES):
            if at(got, column) != want[index + 1]:
                wrong.append(
                    f"{account} {column}={at(got, column)!r}, the master says "
                    f"{want[index + 1]!r}")
    assert not wrong, f"FAIL: {day}: " + "; ".join(wrong[:8]) + f" [{len(wrong)} in all]"


@pytest.mark.parametrize("run,day", DAYS)
def test_the_acquired_accounts_carry_empty_windows(run, day):
    """An account that has not traded with us has traded nothing.

    The figure is the one the guard above read off the order book, not a zero
    typed here. A row invented with a made-up history, or one filled from
    Northwave's own frozen order book, is convicted; so is a row left with a
    null where the table's own `coalesce` puts a zero.
    """
    rows = built(run, day)
    wrong = []
    for account in sorted(acquired()):
        got = rows.get(account)
        if got is None:
            continue
        for column in COUNTS + ("net_sales_cents", "net_sales_cents_30d"):
            if at(got, column) != 0:
                wrong.append(f"{account} {column}={at(got, column)}, it has no order here")
        if at(got, "last_order_date") is not None:
            wrong.append(
                f"{account} last_order_date={at(got, 'last_order_date')}, it has no order here")
    assert not wrong, f"FAIL: {day}: " + "; ".join(wrong[:8]) + f" [{len(wrong)} in all]"


@pytest.mark.parametrize("run,day", DAYS)
def test_the_accounts_already_on_the_file_did_not_move(run, day):
    """The ticket's fifth rule. Adding rows is not licence to rebuild the
    trailing windows: the counts and the booked money on an account that was
    already on the file come out as the order book has them.

    `net_sales_cents` is not here. It belongs to CUS-509, which is open on the
    same model, and a tree that has both fixes and a tree that has only this
    one both pass this test.
    """
    rows = built(run, day)
    expected = counts(run, day)
    wrong = []
    for account, want in expected.items():
        got = rows.get(account)
        if got is None:
            continue
        for index, column in enumerate(COUNTS):
            if at(got, column) != want[index]:
                wrong.append(
                    f"{account} {column}={at(got, column)}, the order book says {want[index]}")
    assert not wrong, f"FAIL: {day}: " + "; ".join(wrong[:8]) + f" [{len(wrong)} in all]"
