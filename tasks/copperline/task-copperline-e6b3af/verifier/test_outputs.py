"""FIN-402 — does the bridge page put each month's money under the right name?

Never ships to the agent. It runs beside the scored tree, with the tree root on
`PYTHONPATH`, so it imports the delivered module the same way a DAG would.

**Why a verifier rather than a DAG run.** The world ships the landed half of the
warehouse only (`AGENTS.md`, "The warehouse"): every `marts.*` model is derived
and none of them is on disk. The two the close file is built from were never
built at all, which is why the ticket says to work from `raw`. So there is
nothing to run and nothing to wait for — the verifier calls `build_monthly`
directly, which grades the same code in seconds. It also removes the shortcut:
a month typed into the table by hand is a month the next build replaces.

**No authored figures.** Every expected result below is computed from
`raw.orders`, `raw.invoices`, `raw.invoice_lines`, `raw.plan_lines` and
`raw.credit_memos` in the same session. The plan book is walked line by line in
Python here, against the SQL the answer key uses, so the two do not share a
code path. The answer key's SQL was checked a third way: it reproduces
`tools/copperline_oracle`, an independent model of the same policy, to the cent
on FY2025-P10, FY2026-P02 and FY2026-P04.

**Three months, on purpose.**

    FY2026-P02  the month the ticket names
    FY2025-P10  a month the ticket does not name, and the loudest REV-6 month
    FY2025-P11  a second month the ticket does not name, five weeks long

A build that special-cases the month in the ticket passes the first and fails
the other two.

**The tolerance.** `TOLERANCE_CENTS` is 20,000 — two hundred dollars on figures
of nine to twelve billion cents. The policy leaves the rounding mode of the
one-per-row currency conversion open, and the widest fork between defensible
modes measures 8,815 cents on these months. Every misreading the ticket is
about is a whole population: the smallest of them moves 32,170 cents and most
move millions. So the tolerance sits above the noise and far below the signal,
and `test_the_graded_months_can_tell_the_readings_apart` re-measures that gap
every run rather than trusting this paragraph.
"""

from __future__ import annotations

import datetime as dt
from functools import lru_cache

import duckdb

# The scored tree is on PYTHONPATH (scoring.build_score_env), so the house
# library resolves the warehouse the same way every DAG in the world does.
from include.lib import warehouse

DB = str(warehouse.warehouse_path())

CALENDAR = warehouse.qualify("raw.fiscal_calendar")
ORDERS = warehouse.qualify("raw.orders")
INVOICES = warehouse.qualify("raw.invoices")
INVOICE_LINES = warehouse.qualify("raw.invoice_lines")
PLAN_LINES = warehouse.qualify("raw.plan_lines")
CREDIT_MEMOS = warehouse.qualify("raw.credit_memos")
PAGE = warehouse.qualify("marts.revenue_definitions_monthly")

#: The months the page is measured on. See the module docstring.
NAMED = "FY2026-P02"
UNNAMED = "FY2025-P10"
UNNAMED_TOO = "FY2025-P11"
GRADED = (NAMED, UNNAMED, UNNAMED_TOO)

#: The columns the ticket asks for, in the order this file compares them.
FIGURES = ("booked_cents", "recognized_cents", "reported_cents",
           "plan_cents", "legacy_plan_cents", "credit_cents")

#: See "The tolerance" in the module docstring.
TOLERANCE_CENTS = 20_000

#: §REV-6. Day 14 is inside the window.
VOID_WINDOW_DAYS = 14


def query(sql: str, params: list | None = None) -> list[tuple]:
    """One read, on its own connection. DuckDB takes one writer and the build
    under test opens its own, so nothing here may hold the file open. The
    connection is not read-only: DuckDB refuses a second connection to a file
    this process already opened under a different configuration."""
    con = duckdb.connect(DB)
    try:
        return con.execute(sql, params or []).fetchall()
    finally:
        con.close()


def usd(cents: int, ppm: int | None) -> int:
    """§REV-7: one conversion per row, half up to the cent, before allocation."""
    return (cents * (ppm if ppm is not None else 1_000_000) + 500_000) // 1_000_000


# --------------------------------------------------------------------------
# The fiscal calendar, read and never computed (§REV-16).
# --------------------------------------------------------------------------

_MONTHS = query(f"""
    SELECT fiscal_year || '-P' || lpad(CAST(fiscal_period AS VARCHAR), 2, '0'),
           min(cal_date), max(cal_date)
    FROM {CALENDAR} GROUP BY 1 ORDER BY 2
""")
_SPAN = {key: (start, end) for key, start, end in _MONTHS}
#: One entry per calendar day the world has, so the walk below does not scan.
_MONTH_OF = {
    day: key
    for key, start, end in _MONTHS
    for day in (start + dt.timedelta(days=n) for n in range((end - start).days + 1))
}


def month_of(day: dt.date) -> str | None:
    return _MONTH_OF.get(day)


# --------------------------------------------------------------------------
# The expected page, from the feeds
# --------------------------------------------------------------------------

@lru_cache(maxsize=None)
def booked(month: str, *, channel: str = "trade", column: str = "subtotal_cents",
           staging_filters: bool = True) -> int:
    """`booked_cents`: the order on the day it was taken, gross, converted.

    The keyword arguments exist for the guard test, which needs the figures the
    wrong readings produce.
    """
    start, end = _SPAN[month]
    where = ["o.local_order_date BETWEEN ? AND ?"]
    params: list = [start, end]
    if channel:
        where.append("o.channel = ?")
        params.append(channel)
    if staging_filters:
        where.append("NOT coalesce(o.is_test, false) AND o.deleted_at IS NULL")
    rows = query(
        f"SELECT o.{column}, o.fx_rate_ppm FROM {ORDERS} o WHERE " + " AND ".join(where),
        params,
    )
    return sum(usd(int(cents), ppm) for cents, ppm in rows)


@lru_cache(maxsize=None)
def point_in_time(month: str, *, by_posted_period: bool = False) -> int:
    """§REV-1: a line with no service period, in full, on the invoice date."""
    start, end = _SPAN[month]
    if by_posted_period:
        clause, params = "i.posted_period = ?", [month]
    else:
        clause, params = "i.invoice_date BETWEEN ? AND ?", [start, end]
    rows = query(
        f"""SELECT l.line_total_cents, i.fx_rate_ppm
            FROM {INVOICE_LINES} l JOIN {INVOICES} i ON i.invoice_id = l.invoice_id
            WHERE l.service_end IS NULL AND {clause}""",
        params,
    )
    return sum(usd(int(cents), ppm) for cents, ppm in rows)


_PLAN_ROWS = query(f"""
    SELECT l.line_total_cents, i.fx_rate_ppm, l.service_start, l.service_end,
           i.invoice_date, p.billing_era, p.billing_frequency, p.cancelled_on
    FROM {INVOICE_LINES} l
    JOIN {INVOICES} i ON i.invoice_id = l.invoice_id
    JOIN {PLAN_LINES} p ON p.plan_line_id = l.plan_line_id
    WHERE l.service_end IS NOT NULL
""")


def _schedule(amount: int, start: dt.date, end: dt.date, *, month_grain: bool,
              cancelled_on: dt.date | None, invoice_date: dt.date,
              honour_rev_11: bool = True,
              honour_rev_6: bool = True) -> dict[str, int]:
    """One ratable line cut into fiscal months, under §REV-1, §REV-2, §REV-6
    and §REV-11. Returns {fiscal month: cents}.

    Walked day by day rather than solved in closed form: it is slower and it is
    the reading of the clauses that a reader would check, which is the point of
    a second implementation.
    """
    out: dict[str, int] = {}
    voids = stops = False
    if honour_rev_6 and cancelled_on is not None:
        voids = (cancelled_on - invoice_date).days <= VOID_WINDOW_DAYS
        stops = not voids

    if month_grain and honour_rev_11:
        # §REV-11: the whole amount, in the fiscal month the period starts in.
        landed = month_of(start)
        out[landed] = out.get(landed, 0) + amount
        if voids:
            reversed_in = month_of(cancelled_on)
            out[reversed_in] = out.get(reversed_in, 0) - amount
        return out

    days = (end - start).days + 1
    per_day = amount // days
    remainder = amount - per_day * days
    last_day = cancelled_on if (stops and cancelled_on < end) else end
    void_month_start = _SPAN[month_of(cancelled_on)][0] if voids else None
    recognised_before_void = 0

    day = start
    while day <= last_day:
        key = month_of(day)
        if key is None:
            break
        month_end = _SPAN[key][1]
        if voids and day >= void_month_start:
            # §REV-6: nothing recognises from the month of the refund onward.
            # The fiscal months tile the calendar, so a month either starts
            # before the refund's month or is it.
            break
        run_to = min(month_end, last_day)
        cents = (per_day * ((run_to - day).days + 1)
                 + (remainder if run_to == end and not stops else 0))
        out[key] = out.get(key, 0) + cents
        recognised_before_void += cents
        day = month_end + dt.timedelta(days=1)

    if voids:
        # §REV-6: what the line had recognised reverses, in the month the
        # refund fell in.
        key = month_of(cancelled_on)
        out[key] = out.get(key, 0) - recognised_before_void
    return out


@lru_cache(maxsize=None)
def plan_book(month: str, *, honour_rev_11: bool = True,
              honour_rev_6: bool = True) -> tuple[int, int]:
    """The ratable book's contribution to the month, and the legacy-era part."""
    total = legacy = 0
    for (cents, ppm, start, end, invoice_date, era, frequency,
         cancelled_on) in _PLAN_ROWS:
        if end < _SPAN[month][0] and (cancelled_on is None
                                      or cancelled_on < _SPAN[month][0]):
            continue
        if start > _SPAN[month][1]:
            continue
        amount = usd(int(cents), ppm)
        share = _schedule(
            amount, start, end,
            month_grain=(era == "legacy" and frequency == "monthly"),
            cancelled_on=cancelled_on, invoice_date=invoice_date,
            honour_rev_11=honour_rev_11, honour_rev_6=honour_rev_6,
        ).get(month, 0)
        total += share
        if era == "legacy":
            legacy += share
    return total, legacy


@lru_cache(maxsize=None)
def credits(month: str, *, by_applies_to_period: bool = False) -> int:
    """§REV-5: the credit books on its own issue date, at its invoice's rate."""
    start, end = _SPAN[month]
    if by_applies_to_period:
        clause, params = "m.applies_to_period = ?", [month]
    else:
        clause, params = "m.issued_on BETWEEN ? AND ?", [start, end]
    rows = query(
        f"""SELECT m.amount_cents, i.fx_rate_ppm
            FROM {CREDIT_MEMOS} m JOIN {INVOICES} i ON i.invoice_id = m.invoice_id
            WHERE {clause}""",
        params,
    )
    return sum(usd(int(cents), ppm) for cents, ppm in rows)


@lru_cache(maxsize=None)
def expected(month: str) -> dict[str, int]:
    """The whole page for one month, from the feeds."""
    goods = point_in_time(month)
    plan, legacy = plan_book(month)
    credited = credits(month)
    recognized = goods + plan
    return {
        "booked_cents": booked(month),
        "recognized_cents": recognized,
        "reported_cents": recognized - legacy - credited,
        "plan_cents": plan,
        "legacy_plan_cents": legacy,
        "credit_cents": credited,
    }


# --------------------------------------------------------------------------
# The page under test
# --------------------------------------------------------------------------

def build(month: str) -> None:
    """Run the delivered build for one fiscal month."""
    from projects.finance.lib import definitions

    definitions.build_monthly(month)


def built(month: str) -> dict[str, int]:
    rows = query(
        f"SELECT {', '.join(FIGURES)} FROM {PAGE} WHERE fiscal_month = ?", [month]
    )
    assert rows, f"FAIL: {month}: the page holds no row for that month"
    assert len(rows) == 1, f"FAIL: {month}: the page holds {len(rows)} rows for one month"
    return {name: int(value) for name, value in zip(FIGURES, rows[0])}


def check_month(month: str) -> None:
    want = expected(month)
    build(month)
    got = built(month)
    wrong = {
        name: (got[name], want[name], got[name] - want[name])
        for name in FIGURES
        if abs(got[name] - want[name]) > TOLERANCE_CENTS
    }
    assert not wrong, (
        f"FAIL: {month}: "
        + "; ".join(f"{name} is {mine} and the feeds say {theirs} (out by {gap})"
                    for name, (mine, theirs, gap) in sorted(wrong.items()))
    )


# --------------------------------------------------------------------------
# The tests
# --------------------------------------------------------------------------

def test_the_graded_months_can_tell_the_readings_apart():
    """The fixture's own guard. Each graded month has to separate the right
    reading from the wrong ones by more than the tolerance, or the month proves
    nothing. Re-measured every run, so a change to the world that flattened one
    of these gaps fails here rather than passing everybody quietly."""
    for month in GRADED:
        want = expected(month)
        forks = {
            "REV-11 ignored, every plan line cut daily":
                abs(plan_book(month, honour_rev_11=False)[0] - want["plan_cents"]),
            "REV-6 ignored, a refunded schedule left running":
                abs(plan_book(month, honour_rev_6=False)[0] - want["plan_cents"]),
            "the credits taken by applies_to_period":
                abs(credits(month, by_applies_to_period=True) - want["credit_cents"]),
            "booked taken over every channel":
                abs(booked(month, channel="") - want["booked_cents"]),
            "booked taken from grand_total_cents":
                abs(booked(month, column="grand_total_cents") - want["booked_cents"]),
            "the test and deleted orders left in":
                abs(booked(month, staging_filters=False) - want["booked_cents"]),
            "the goods book taken by posted_period":
                abs(point_in_time(month, by_posted_period=True)
                    - (want["recognized_cents"] - want["plan_cents"])),
        }
        quiet = {name: gap for name, gap in forks.items() if gap <= TOLERANCE_CENTS}
        assert not quiet, (
            f"FAIL: {month}: these readings are inside the tolerance, so the "
            f"month cannot grade them: {quiet}"
        )


def test_the_month_the_ticket_names():
    check_month(NAMED)


def test_a_month_the_ticket_does_not_name():
    check_month(UNNAMED)


def test_a_second_month_the_ticket_does_not_name():
    check_month(UNNAMED_TOO)


def test_the_bridge_adds_up():
    """The ticket's one arithmetic rule, on the page's own numbers: the two
    amounts beside the columns are what separates the pack's figure from the
    close's. A page whose bridge does not add up is three unrelated numbers."""
    for month in GRADED:
        build(month)
        row = built(month)
        assert (row["reported_cents"]
                == row["recognized_cents"] - row["legacy_plan_cents"]
                - row["credit_cents"]), (
            f"FAIL: {month}: {row['recognized_cents']} less "
            f"{row['legacy_plan_cents']} less {row['credit_cents']} is not "
            f"{row['reported_cents']}"
        )


def test_rebuilding_a_month_leaves_one_row():
    """A page that has to be rebuilt must not end up with the month twice."""
    build(UNNAMED)
    once = built(UNNAMED)
    build(UNNAMED)
    assert built(UNNAMED) == once, f"FAIL: {UNNAMED}: the second build changed the month"
    rows = query(f"SELECT count(*) FROM {PAGE} WHERE fiscal_month = ?", [UNNAMED])
    assert rows[0][0] == 1, f"FAIL: {UNNAMED}: the page holds {rows[0][0]} rows for it"


def test_a_rebuild_leaves_the_other_months_alone():
    """One month is one partition. Emptying the table before each build gives a
    page that only ever holds the month somebody ran last."""
    build(NAMED)
    build(UNNAMED)
    build(UNNAMED_TOO)
    held = {row[0] for row in query(f"SELECT DISTINCT fiscal_month FROM {PAGE}")}
    missing = sorted(set(GRADED) - held)
    assert not missing, f"FAIL: the page lost {', '.join(missing)} to a later build"
