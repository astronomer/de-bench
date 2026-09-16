"""FIN-472 — what the board pack's revenue page holds after the change.

Never ships to the agent. It runs beside the scored tree, with the tree root on
`PYTHONPATH`, so it imports the patched DAG module the same way the scheduler
would and drives the pack's own tasks.

**Why a fixture rather than a DAG run.** The world ships the landed half of the
warehouse only — `AGENTS.md`, "The warehouse": `staging.*` and `marts.*` are
derived and neither is on disk until the 04:00 dbt build has made them. The two
relations the revenue page reads are built here instead, out of `raw.invoices`
and `raw.fiscal_calendar`:

    marts.revenue_recognized_monthly   one row per fiscal month per entity,
                                       with the month's cents and whether it
                                       has closed. The contract for it is
                                       `contracts/revenue_recognized_monthly.yml`
                                       and this is its grain and its columns.
    marts.board_revenue_weekly         the fiscal-week table the ticket points
                                       at. It is here so that a page moved onto
                                       it answers, and is then read for what it
                                       is: fiscal weeks where the contract asks
                                       for fiscal months.

`is_closed` is the world's own rule reduced to one date. `docs/finance-policy.md`
§REV-8 closes a period on the fifth business day of the next one; FY2026-P04 ran
to 2026-05-30 and closed 2026-06-05, and the world's date is 2026-06-15
(`worlds/copperline/world.yaml`, `WORLD_TODAY`). So every fiscal month that ended
before 2026-05-31 is closed at the world's date and the month in progress is not.

**No authored figures.** Every expected result is read out of the fixture in the
same session: which months are on the page, how many rows the page holds, and
each row's cents. The only authored number is `MONTHS_ASKED`, which is what the
ticket asks for and is not derivable from anything.
"""

from __future__ import annotations

import csv
import importlib
from pathlib import Path

import duckdb
import pytest

# The scored tree is on PYTHONPATH (scoring.build_score_env), so the house
# library resolves the warehouse the same way every DAG in the world does.
from include.lib import warehouse, workspace_root

DB = str(warehouse.warehouse_path())

# Every name is qualified onto the live warehouse, for the reason
# `include/lib/warehouse.py` gives: the frozen `nwv` copy sits first on the
# search path and carries `raw.*` under the same names.
MARTS = warehouse.qualify("marts")
MONTHLY = warehouse.qualify("marts.revenue_recognized_monthly")
WEEKLY = warehouse.qualify("marts.board_revenue_weekly")
INVOICES = warehouse.qualify("raw.invoices")
CALENDAR = warehouse.qualify("raw.fiscal_calendar")

#: The ticket's number, and the one thing on this page the world does not say.
MONTHS_ASKED = 13

#: A Monday the pack would run on, and the Sunday it dates itself to. Both sit
#: outside the world's reserved windows (2026-06-01..09, FY2025 P8) and outside
#: the never-grade tail. The revenue step reads neither — it selects on
#: `is_closed` — so they only have to be a real pair.
DS = "2026-05-18"
TARGET_DS = "2026-05-17"

#: The close boundary at the world's date. See the module docstring.
CLOSED_BEFORE = "2026-05-31"

EXPORT_ROOT = workspace_root() / "exports" / "board_pack"

MONTH_EXPR = "fiscal_year || '-P' || lpad(cast(fiscal_period as varchar), 2, '0')"

FIXTURE_MONTHLY = f"""
CREATE OR REPLACE TABLE {MONTHLY} AS
WITH months AS (
    SELECT {MONTH_EXPR} AS fiscal_month,
           max(cal_date) AS month_end
    FROM {CALENDAR}
    GROUP BY 1
)
SELECT i.posted_period                          AS fiscal_month,
       i.entity_code                            AS entity,
       sum(i.net_cents)::BIGINT                 AS recognized_cents,
       (m.month_end < DATE '{CLOSED_BEFORE}')   AS is_closed
FROM {INVOICES} i
JOIN months m ON m.fiscal_month = i.posted_period
GROUP BY 1, 2, 4
"""

FIXTURE_WEEKLY = f"""
CREATE OR REPLACE TABLE {WEEKLY} AS
WITH weeks AS (
    SELECT week_start,
           fiscal_year,
           min(cast(fiscal_week as integer))     AS fiscal_week,
           {MONTH_EXPR}                          AS fiscal_month
    FROM {CALENDAR}
    GROUP BY week_start, fiscal_year, fiscal_period
)
SELECT w.week_start,
       w.fiscal_year,
       w.fiscal_week,
       w.fiscal_month,
       'US'                                              AS region,
       0::BIGINT                                         AS comp_sales_cents,
       0::BIGINT                                         AS comp_sales_cents_ly,
       0                                                 AS comp_store_count,
       0::BIGINT                                         AS comp_change_cents,
       coalesce(r.recognized_cents, 0)::BIGINT           AS month_reported_cents,
       coalesce(r.recognized_cents, 0)::BIGINT           AS month_recognized_cents,
       0                                                 AS month_active_accounts,
       coalesce(r.is_closed, false)                      AS month_is_closed
FROM weeks w
LEFT JOIN {MONTHLY} r
       ON r.fiscal_month = w.fiscal_month AND r.entity = 'CL-US'
"""

#: The months the page holds, and nothing else decides it: the closed months the
#: table carries, newest first, as many of them as the ticket asked for.
PAGE_MONTHS = f"""
SELECT fiscal_month
FROM {MONTHLY}
WHERE is_closed
GROUP BY 1
ORDER BY fiscal_month DESC
LIMIT {MONTHS_ASKED}
"""

#: The page itself: one row per fiscal month per entity, the month's own cents.
TRUTH = f"""
SELECT fiscal_month, entity, sum(recognized_cents)::BIGINT
FROM {MONTHLY}
WHERE fiscal_month IN ({PAGE_MONTHS})
GROUP BY 1, 2
ORDER BY 1, 2
"""


def query(sql: str, params: list | None = None) -> list[tuple]:
    """One read, on its own connection, and read-only.

    DuckDB takes one writer and the pack opens its own. Read-only also leaves
    the file's timestamp alone, which matters: `warehouse.connect(read_only=True)`
    re-copies the whole warehouse to its snapshot whenever the live file is
    newer, and a read that dirtied the file would pay for that copy on every
    call the pack makes.
    """
    con = duckdb.connect(DB, read_only=True)
    try:
        return con.execute(sql, params or []).fetchall()
    finally:
        con.close()


@pytest.fixture(scope="session", autouse=True)
def warehouse_ready():
    """The two derived relations the revenue page reads."""
    con = duckdb.connect(DB)
    try:
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {MARTS}")
        con.execute(FIXTURE_MONTHLY)
        con.execute(FIXTURE_WEEKLY)
    finally:
        con.close()


def pack():
    """The patched DAG module, imported the way the scheduler imports it."""
    return importlib.import_module("projects.finance.dags.fin_board_pack_weekly")


def run_task(task_id: str):
    """Run one of the pack's steps, with its own arguments.

    The arguments come off the task rather than out of this file, so a step
    whose function or parameter was renamed still runs: whatever the task
    passes is what it gets. A templated argument is the run's date and is
    rendered to the Sunday the pack dates itself to.
    """
    dag = getattr(pack(), "dag", None)
    assert dag is not None, "FAIL: fin_board_pack_weekly.py no longer defines a DAG"
    task = dag.get_task(task_id)
    function = getattr(task, "python_callable", None)
    assert function is not None, f"FAIL: the {task_id} step no longer calls a python function"
    kwargs = dict(getattr(task, "op_kwargs", None) or {})
    for name, value in list(kwargs.items()):
        if isinstance(value, str) and "{{" in value:
            kwargs[name] = TARGET_DS
    try:
        return function(**kwargs)
    except TypeError:
        # A step rewritten to take the date positionally, or to take nothing at
        # all, still runs. Only a step that wants something this file cannot
        # give it fails here.
        for attempt in ((TARGET_DS,), ()):
            try:
                return function(*attempt)
            except TypeError:
                continue
        raise


def written_page(revenue: list) -> list[tuple] | None:
    """The revenue rows the pack writes to the deck's own file, or None when the
    write step cannot be driven from here.

    Driving the write step matters because a page trimmed on the way out is a
    page trimmed. The step takes three of its four arguments off other steps,
    so they are bound by position; the companion file and the account count are
    other tickets and go in empty.
    """
    dag = getattr(pack(), "dag", None)
    if dag is None:
        return None
    try:
        function = dag.get_task("write_pack").python_callable
        path = EXPORT_ROOT / f"{TARGET_DS}.csv"
        if path.exists():
            path.unlink()
        function(revenue, [], 0, TARGET_DS)
        if not path.exists():
            return None
        with path.open(encoding="utf-8-sig", newline="") as handle:
            table = [row for row in csv.reader(handle) if any(cell.strip() for cell in row)]
        rows = []
        for row in table[1:]:
            if len(row) < 3:
                return None
            rows.append((row[0].strip(), row[1].strip(), int(row[2])))
        return sorted(rows)
    except Exception:
        return None


def page() -> list[tuple]:
    """The revenue rows that reach the deck.

    Read off the file the pack writes when the write step can be driven, and
    off the revenue step's own return when it cannot. Either way it is the
    page, so a trim in the revenue step and a trim on the way out both read the
    same here.
    """
    revenue = run_task("company_revenue")
    assert revenue is not None, "FAIL: the revenue step returned nothing"
    written = written_page(revenue)
    if written is not None:
        return written
    short = [row for row in revenue if len(row) < 3]
    assert not short, (
        f"FAIL: the revenue step returns {len(short[0])} value(s) a row and the page needs "
        "a fiscal month, an entity and the month's cents"
    )
    return sorted((str(row[0]), str(row[1]), int(row[2])) for row in revenue)


def truth() -> list[tuple]:
    return sorted((str(m), str(e), int(c)) for m, e, c in query(TRUTH))


def test_the_month_table_holds_more_closed_months_than_the_page_asks_for():
    """The fixture's own guard. If the world's invoice book ever stops carrying
    more than thirteen closed fiscal months, a page that was never trimmed and a
    page that was trimmed correctly are the same page, and every test below
    grades nothing. This says so rather than passing quietly."""
    closed = query(f"SELECT count(DISTINCT fiscal_month) FROM {MONTHLY} WHERE is_closed")[0][0]
    assert closed > MONTHS_ASKED, (
        f"FAIL: the month table holds {closed} closed fiscal month(s) and the page asks "
        f"for {MONTHS_ASKED}, so nothing here can tell a trimmed page from an untrimmed one"
    )


def test_the_weekly_table_is_a_different_shape_from_the_monthly_one():
    """The other guard. The ticket points at `marts.board_revenue_weekly`, and a
    page moved onto it has to come out visibly different or the move is not
    measurable."""
    weeks = query(f"SELECT count(DISTINCT week_start) FROM {WEEKLY}")[0][0]
    months = query(f"SELECT count(DISTINCT fiscal_month) FROM {MONTHLY}")[0][0]
    assert weeks > months, (
        f"FAIL: {weeks} fiscal week(s) against {months} fiscal month(s); the weekly table "
        "is not distinguishable from the monthly one"
    )


def test_the_page_still_reads_in_fiscal_months():
    """§BP-1: one row per fiscal month per reporting entity. Every key on the
    page has to be a fiscal month the month table holds. A week start, an ISO
    week label or a fiscal-week number is not one, and that is the half of this
    ticket the contract does not give."""
    known = {str(row[0]) for row in query(f"SELECT DISTINCT fiscal_month FROM {MONTHLY}")}
    stray = sorted({row[0] for row in page()} - known)
    assert not stray, (
        f"FAIL: the page keys on {', '.join(stray[:6])}, which the month table does not "
        "spell as a fiscal month"
    )


def test_the_page_holds_one_row_per_fiscal_month_and_entity():
    """§BP-1 again, from the other side: no key twice. A page joined to a finer
    grain prints the same month more than once with different totals."""
    keys = [(row[0], row[1]) for row in page()]
    doubled = sorted({key for key in keys if keys.count(key) > 1})
    assert not doubled, f"FAIL: the page carries {doubled[:6]} more than once"


def test_the_page_is_the_last_thirteen_closed_fiscal_months():
    """The half of the ticket the contract does give. The range is not in the
    contract — its own open items call it unwritten — so trimming it is the
    pack's to do."""
    expected = {str(row[0]) for row in query(PAGE_MONTHS)}
    found = {row[0] for row in page()}
    assert found == expected, (
        f"FAIL: the page holds {len(found)} fiscal month(s) "
        f"({', '.join(sorted(found)[:4])}…) and the last {MONTHS_ASKED} closed months are "
        f"{', '.join(sorted(expected))}"
    )


def test_every_row_carries_the_months_own_figure_for_its_entity():
    """A shorter page is a shorter page and nothing else. Dropping entities, or
    rolling months together to make the row count fall, moves the numbers the
    meeting reads."""
    found = page()
    expected = truth()
    assert found == expected, (
        f"FAIL: the page holds {len(found)} row(s) against {len(expected)}; first "
        f"disagreement {next((f'{a} vs {b}' for a, b in zip(found, expected) if a != b), 'in the tail')}"
    )
