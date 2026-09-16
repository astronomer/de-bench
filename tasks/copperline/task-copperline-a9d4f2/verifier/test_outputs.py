"""FIN-457 — which measure the board pack's revenue page carries after the change.

Never ships to the agent. It runs beside the scored tree, with the tree root on
`PYTHONPATH`, so it imports the patched DAG module the same way the scheduler
would and drives the pack's own steps.

**Why a fixture rather than a DAG run.** The world ships the landed half of the
warehouse only — `AGENTS.md`, "The warehouse": `staging.*` and `marts.*` are
derived and neither is on disk until the 04:00 dbt build has made them. The
three relations the pack reads are built here instead, out of `raw.invoices`,
`raw.credit_memos`, `raw.customers` and `raw.fiscal_calendar`:

    marts.revenue_recognized_monthly   one row per fiscal month per entity with
                                       the month's recognised cents, per
                                       `contracts/revenue_recognized_monthly.yml`.
                                       Built exactly as FIN-472's fixture builds
                                       it, so the two tasks agree about the same
                                       relation.
    marts.account_rollup               one row per fiscal month, entity and
                                       account, per `contracts/account_rollup.yml`.
                                       It carries both money columns:
                                       `recognized_cents`, and `reported_cents`
                                       as `docs/semantic-definitions.md` §SD-2
                                       defines it — recognised, net of refunds,
                                       with `billing_era = 'legacy'` removed for
                                       §BP-2.
    marts.dim_customer                 the account names the companion page
                                       prints.

The rollup sums to the month table on `recognized_cents` exactly, so the only
thing that separates the two relations is the measure — which is what this task
grades. A guard test asserts that tie rather than assuming it.

`is_closed` is the world's own rule reduced to one date. `docs/finance-policy.md`
§REV-8 closes a period on the fifth business day of the next one; FY2026-P05 ran
to 2026-05-30 and the world's date is 2026-06-15 (`worlds/copperline/world.yaml`,
`WORLD_TODAY`). So every fiscal month that ended before 2026-05-31 is closed at
the world's date and the month in progress is not.

**No authored figures.** Every expected result is read out of the fixture in the
same session: which months and entities the page holds, each row's cents, and
which twenty accounts the companion page ranks. Nothing here is typed from a
table an author read once.
"""

from __future__ import annotations

import csv
import importlib
from collections import Counter
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
ROLLUP = warehouse.qualify("marts.account_rollup")
CUSTOMERS = warehouse.qualify("marts.dim_customer")
INVOICES = warehouse.qualify("raw.invoices")
MEMOS = warehouse.qualify("raw.credit_memos")
ACCOUNTS = warehouse.qualify("raw.customers")
CALENDAR = warehouse.qualify("raw.fiscal_calendar")

#: §BP-1, quoted: "Columns: `fiscal_month`, `entity`, `reported_cents`,
#: `active_accounts`". The one authored list in this file, and it is the
#: contract's own, not the author's.
PACK_COLUMNS = ["fiscal_month", "entity", "reported_cents", "active_accounts"]

#: §BP-1 again: twenty rows in the companion file.
TOP_N = 20

#: A Monday the pack would run on, and the Sunday it dates itself to. Both sit
#: outside the world's reserved windows (2026-06-01..09, FY2025 P8) and outside
#: the never-grade tail. No step reads either — the pages select on the marts —
#: so they only have to be a real pair.
DS = "2026-05-18"
TARGET_DS = "2026-05-17"

#: The close boundary at the world's date. See the module docstring.
CLOSED_BEFORE = "2026-05-31"

EXPORT_ROOT = workspace_root() / "exports" / "board_pack"

MONTH_EXPR = "fiscal_year || '-P' || lpad(cast(fiscal_period as varchar), 2, '0')"

MONTHS_CTE = f"""
months AS (
    SELECT {MONTH_EXPR} AS fiscal_month,
           max(cal_date) AS month_end
    FROM {CALENDAR}
    GROUP BY 1
)
"""

FIXTURE_MONTHLY = f"""
CREATE OR REPLACE TABLE {MONTHLY} AS
WITH {MONTHS_CTE}
SELECT i.posted_period                          AS fiscal_month,
       i.entity_code                            AS entity,
       sum(i.net_cents)::BIGINT                 AS recognized_cents,
       (m.month_end < DATE '{CLOSED_BEFORE}')   AS is_closed
FROM {INVOICES} i
JOIN months m ON m.fiscal_month = i.posted_period
GROUP BY 1, 2, 4
"""

#: The rollup, at the grain `contracts/account_rollup.yml` pins. `reported_cents`
#: is §SD-2's definition applied to the same invoices the month table sums:
#: recognised, less the credit memos raised against the account for the month,
#: with the legacy-era book left out for §BP-2.
FIXTURE_ROLLUP = f"""
CREATE OR REPLACE TABLE {ROLLUP} AS
WITH {MONTHS_CTE},
accounts AS (
    SELECT i.posted_period                       AS fiscal_month,
           i.entity_code                         AS entity,
           coalesce(i.customer_ref, 'unattributed') AS customer_id,
           sum(i.net_cents)::BIGINT              AS recognized_cents,
           coalesce(sum(i.net_cents) FILTER (WHERE i.billing_era <> 'legacy'), 0)::BIGINT
                                                 AS current_era_cents
    FROM {INVOICES} i
    GROUP BY 1, 2, 3
),
credits AS (
    SELECT cm.applies_to_period                  AS fiscal_month,
           i.entity_code                         AS entity,
           coalesce(i.customer_ref, 'unattributed') AS customer_id,
           sum(cm.amount_cents)::BIGINT          AS credited_cents
    FROM {MEMOS} cm
    JOIN {INVOICES} i ON i.invoice_id = cm.invoice_id
    GROUP BY 1, 2, 3
)
SELECT a.fiscal_month,
       a.entity,
       a.customer_id,
       a.recognized_cents,
       coalesce(c.credited_cents, 0)::BIGINT     AS credited_cents,
       (a.current_era_cents - coalesce(c.credited_cents, 0))::BIGINT AS reported_cents,
       (count(*) OVER (PARTITION BY a.fiscal_month, a.entity))::INTEGER AS active_accounts,
       (m.month_end < DATE '{CLOSED_BEFORE}')    AS is_closed
FROM accounts a
JOIN months m ON m.fiscal_month = a.fiscal_month
LEFT JOIN credits c
       ON c.fiscal_month = a.fiscal_month
      AND c.entity = a.entity
      AND c.customer_id = a.customer_id
"""

FIXTURE_CUSTOMERS = f"""
CREATE OR REPLACE TABLE {CUSTOMERS} AS
SELECT customer_id,
       account_name,
       status,
       status AS account_status
FROM {ACCOUNTS}
"""

#: The month and entity keys the page covers: everything up to the newest closed
#: fiscal month, which is the range the pack shipped with and the range this
#: ticket does not ask anyone to move.
PAGE_KEYS = f"""
SELECT fiscal_month, entity
FROM {ROLLUP}
WHERE fiscal_month <= (SELECT max(fiscal_month) FROM {ROLLUP} WHERE is_closed)
GROUP BY 1, 2
ORDER BY 1, 2
"""

#: The page itself: §SD-2's `reported_cents`, summed to the grain §BP-1 names.
TRUTH = f"""
SELECT fiscal_month, entity, sum(reported_cents)::BIGINT
FROM {ROLLUP}
WHERE fiscal_month <= (SELECT max(fiscal_month) FROM {ROLLUP} WHERE is_closed)
GROUP BY 1, 2
ORDER BY 1, 2
"""

#: What the pack shipped carrying: the month table's `recognized_cents` under
#: the `reported_cents` header. Kept here to prove the two answers are
#: distinguishable on every key the page holds.
SHIPPED = f"""
SELECT fiscal_month, entity, sum(recognized_cents)::BIGINT
FROM {MONTHLY}
WHERE fiscal_month <= (SELECT max(fiscal_month) FROM {MONTHLY} WHERE is_closed)
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
    """The three derived relations the pack reads."""
    con = duckdb.connect(DB)
    try:
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {MARTS}")
        con.execute(FIXTURE_MONTHLY)
        con.execute(FIXTURE_ROLLUP)
        con.execute(FIXTURE_CUSTOMERS)
    finally:
        con.close()


def pack_module():
    """The patched DAG module, imported the way the scheduler imports it."""
    return importlib.import_module("projects.finance.dags.fin_board_pack_weekly")


def dag():
    found = getattr(pack_module(), "dag", None)
    assert found is not None, "FAIL: fin_board_pack_weekly.py no longer defines a DAG"
    return found


def run_task(task_id: str, upstream: dict | None = None):
    """Run one of the pack's steps, with its own arguments.

    The arguments come off the task rather than out of this file, so a step
    whose function or parameter was renamed still runs: whatever the task
    passes is what it gets. An argument that is another step's output is filled
    from `upstream`, keyed on the task id that produces it; a templated argument
    is the run's date and is rendered to the Sunday the pack dates itself to.
    """
    task = dag().get_task(task_id)
    function = getattr(task, "python_callable", None)
    assert function is not None, f"FAIL: the {task_id} step no longer calls a python function"
    kwargs = dict(getattr(task, "op_kwargs", None) or {})
    for name, value in list(kwargs.items()):
        source = getattr(getattr(value, "operator", None), "task_id", None)
        if source is not None:
            kwargs[name] = (upstream or {}).get(source)
        elif isinstance(value, str) and "{{" in value:
            kwargs[name] = TARGET_DS
    try:
        return function(**kwargs)
    except TypeError:
        # A step rewritten to take its arguments positionally still runs. Only a
        # step that wants something this file cannot give it fails here.
        return function(*kwargs.values())


def read_csv(path: Path) -> list[list[str]]:
    with path.open(encoding="utf-8-sig", newline="") as handle:
        return [row for row in csv.reader(handle) if any(cell.strip() for cell in row)]


_deck: dict = {}


def deck() -> dict:
    """Drive the pack once and keep what it produced.

    `revenue` and `accounts` are the two steps' own returns. `pack` and
    `companion` are the two files the write step lays down, header row and all,
    or None when the write step cannot be driven from here — a page trimmed or
    relabelled on the way out is still a page, so the files are what the deck
    really shows. The account count is NW-214's ticket and goes in as zero.
    """
    if _deck:
        return _deck
    revenue = run_task("company_revenue")
    assert revenue is not None, "FAIL: the revenue step returned nothing"
    accounts = run_task("top_accounts")
    assert accounts is not None, "FAIL: the top-accounts step returned nothing"
    _deck.update({"revenue": revenue, "accounts": accounts, "pack": None, "companion": None})
    pack_path = EXPORT_ROOT / f"{TARGET_DS}.csv"
    companion_path = EXPORT_ROOT / f"{TARGET_DS}_accounts.csv"
    for path in (pack_path, companion_path):
        if path.exists():
            path.unlink()
    try:
        run_task("write_pack", {
            "company_revenue": revenue,
            "top_accounts": accounts,
            "active_accounts": 0,
        })
    except Exception:
        return _deck
    if pack_path.exists():
        _deck["pack"] = read_csv(pack_path)
    if companion_path.exists():
        _deck["companion"] = read_csv(companion_path)
    return _deck


def page() -> list[tuple]:
    """The revenue rows that reach the deck.

    Read off the file the pack writes when the write step can be driven, and
    off the revenue step's own return when it cannot. Either way it is the
    page, so a change made in the revenue step and one made on the way out both
    read the same here.
    """
    written = deck()["pack"]
    rows = written[1:] if written else deck()["revenue"]
    short = [row for row in rows if len(row) < 3]
    assert not short, (
        f"FAIL: the revenue page carries {len(short[0])} value(s) a row and it needs a "
        "fiscal month, an entity and the month's cents"
    )
    return sorted((str(row[0]).strip(), str(row[1]).strip(), int(row[2])) for row in rows)


def account_page() -> list[tuple]:
    """The companion page: (fiscal_month, entity, customer_id, cents), in the
    order the file prints them."""
    written = deck()["companion"]
    rows = written[1:] if written else deck()["accounts"]
    assert rows, "FAIL: the companion accounts page is empty"
    short = [row for row in rows if len(row) < 5]
    assert not short, (
        f"FAIL: the companion page carries {len(short[0])} value(s) a row and §BP-1 asks "
        "for a fiscal month, an entity, an account id, an account name and its cents"
    )
    return [(str(r[0]).strip(), str(r[1]).strip(), str(r[2]).strip(), int(r[4])) for r in rows]


def truth() -> list[tuple]:
    return sorted((str(m), str(e), int(c)) for m, e, c in query(TRUTH))


def test_the_two_marts_agree_about_recognized_revenue():
    """The fixture's own guard, and the reason this task is about a measure and
    not about a mart. Summed to fiscal month and entity, the rollup's
    `recognized_cents` is the month table's `recognized_cents` to the cent, so
    moving the page from one relation to the other changes nothing on its own.
    What changes the number is which column the page sums."""
    mismatch = query(f"""
        SELECT m.fiscal_month, m.entity, m.recognized_cents, r.recognized_cents
        FROM {MONTHLY} m
        JOIN (SELECT fiscal_month, entity, sum(recognized_cents)::BIGINT AS recognized_cents
              FROM {ROLLUP} GROUP BY 1, 2) r
          ON r.fiscal_month = m.fiscal_month AND r.entity = m.entity
        WHERE m.recognized_cents <> r.recognized_cents
    """)
    assert not mismatch, (
        f"FAIL: the two fixture relations disagree about recognised revenue on "
        f"{len(mismatch)} key(s), first {mismatch[0]}"
    )


def test_the_two_measures_differ_on_every_key_the_page_holds():
    """The other guard. Reported and recognised have to be visibly different on
    every month and entity the page carries, or a page that was never fixed and
    a page that was fixed read the same and every test below grades nothing."""
    reported = {(m, e): c for m, e, c in query(TRUTH)}
    recognized = {(m, e): c for m, e, c in query(SHIPPED)}
    assert set(reported) == set(recognized), (
        "FAIL: the two measures do not cover the same month and entity keys, so the "
        "fixture cannot tell one page from the other"
    )
    same = sorted(key for key, cents in reported.items() if cents == recognized[key])
    assert not same, (
        f"FAIL: reported and recognised revenue are equal on {len(same)} key(s), first "
        f"{same[0]}; those keys grade nothing"
    )


def test_the_revenue_page_still_reads_in_fiscal_months():
    """§BP-1: one row per fiscal month per reporting entity. Every key on the
    page has to be a fiscal month the rollup holds. Changing the measure is not
    licence to change the grain."""
    known = {str(row[0]) for row in query(f"SELECT DISTINCT fiscal_month FROM {ROLLUP}")}
    stray = sorted({row[0] for row in page()} - known)
    assert not stray, (
        f"FAIL: the revenue page keys on {', '.join(stray[:6])}, which the rollup does "
        "not spell as a fiscal month"
    )


def test_the_revenue_page_holds_one_row_per_fiscal_month_and_entity():
    """§BP-1 from the other side: no key twice. A page joined to the rollup at
    account grain and never summed back prints the same month once an account."""
    # Counted rather than scanned: a page that was joined to the rollup and
    # never summed back is a row an account, which is hundreds of thousands of
    # them, and this test has to fail on it rather than sit there.
    counts = Counter((row[0], row[1]) for row in page())
    doubled = sorted(key for key, seen in counts.items() if seen > 1)
    assert not doubled, (
        f"FAIL: the revenue page carries {len(doubled)} key(s) more than once, first "
        f"{doubled[:6]}"
    )


def test_the_revenue_page_still_covers_every_closed_month_and_entity():
    """The page's range is not what this ticket moves. Dropping the entities or
    the months where the two measures disagree is the cheapest way to make a
    tie-out tie, and it is not a fix."""
    expected = {(str(m), str(e)) for m, e in query(PAGE_KEYS)}
    found = {(row[0], row[1]) for row in page()}
    missing = sorted(expected - found)
    extra = sorted(found - expected)
    assert not missing and not extra, (
        f"FAIL: the revenue page is missing {len(missing)} key(s) "
        f"({', '.join('/'.join(k) for k in missing[:4])}) and carries {len(extra)} it "
        f"should not ({', '.join('/'.join(k) for k in extra[:4])})"
    )


def test_the_revenue_page_carries_reported_revenue():
    """The ticket. Every row is §SD-2's `reported_cents` for that month and
    entity — recognised, net of refunds, with the legacy-era book out for §BP-2
    — which is the measure the column has always named and the measure the
    companion page under it is made of."""
    found = page()
    expected = truth()
    shipped = sorted((str(m), str(e), int(c)) for m, e, c in query(SHIPPED))
    if found == shipped:
        raise AssertionError(
            "FAIL: the revenue page still carries recognised revenue under the "
            "`reported_cents` header, which is the disagreement the ticket is about"
        )
    assert found == expected, (
        f"FAIL: the revenue page holds {len(found)} row(s) against {len(expected)}; first "
        f"disagreement {next((f'{a} vs {b}' for a, b in zip(found, expected) if a != b), 'in the tail')}"
    )


def test_the_pack_keeps_the_columns_its_contract_names():
    """§BP-1 pins the header: `fiscal_month`, `entity`, `reported_cents`,
    `active_accounts`. Renaming the column so that the shipped query becomes
    honest is the other way to close this ticket, and it is not the pack's to
    do — finance owns C-1 and an amendment comes before the code, per
    `docs/change-management.md` §CM-1."""
    written = deck()["pack"]
    assert written, (
        f"FAIL: the pack wrote no file at {EXPORT_ROOT / f'{TARGET_DS}.csv'}, so the "
        "deck the board reads cannot be examined"
    )
    header = [cell.strip() for cell in written[0]]
    assert header == PACK_COLUMNS, (
        f"FAIL: the pack's columns are {header} and §BP-1 names {PACK_COLUMNS}"
    )


def test_the_account_page_is_still_valued_on_reported_revenue():
    """The other way to make a deck agree with itself: move the accounts page
    onto recognised revenue. §BP-1 and the contract's companion-file section
    both put that page on `reported_cents`, and it is the page that was right
    all along."""
    rows = account_page()
    header = [cell.strip() for cell in (deck()["companion"] or [[]])[0]]
    if header:
        assert "reported_cents" in header, (
            f"FAIL: the companion page's columns are {header} and the contract's "
            "companion-file section ranks it on `reported_cents`"
        )
    known = {
        (str(m), str(e), str(c)): (int(rep), int(rec))
        for m, e, c, rep, rec in query(
            f"SELECT fiscal_month, entity, customer_id, reported_cents, recognized_cents "
            f"FROM {ROLLUP}"
        )
    }
    wrong = []
    for month, entity, customer, cents in rows:
        expected = known.get((month, entity, customer))
        if expected is None or cents != expected[0]:
            wrong.append((month, entity, customer, cents, expected))
    assert not wrong, (
        f"FAIL: {len(wrong)} of the companion page's {len(rows)} rows do not carry the "
        f"rollup's reported cents for their account; first {wrong[0]}"
    )


def test_the_account_page_is_still_the_top_twenty_of_its_month():
    """§BP-1: twenty rows, ranked. The ranking is graded against whichever month
    the page shows, so a pack that also moves the ranking to the newest CLOSED
    month — which `contracts/board-pack.md` lists as an open item — is read on
    its own terms rather than against the month the code happened to pick."""
    rows = account_page()
    months = sorted({row[0] for row in rows})
    assert len(months) == 1, (
        f"FAIL: the companion page mixes {len(months)} fiscal month(s) ({months[:4]}) and "
        "the contract ranks one month"
    )
    assert len(rows) == TOP_N, f"FAIL: the companion page holds {len(rows)} rows and §BP-1 says {TOP_N}"
    cents = [row[3] for row in rows]
    assert cents == sorted(cents, reverse=True), (
        "FAIL: the companion page is not ranked from the largest account down"
    )
    expected = [
        int(row[0])
        for row in query(
            f"SELECT reported_cents FROM {ROLLUP} WHERE fiscal_month = ? "
            f"ORDER BY reported_cents DESC, customer_id ASC LIMIT {TOP_N}",
            [months[0]],
        )
    ]
    assert cents == expected, (
        f"FAIL: the companion page's twenty accounts come to {cents[:4]}… and the "
        f"rollup's top {TOP_N} by reported cents for {months[0]} come to {expected[:4]}…"
    )
