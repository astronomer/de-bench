"""`tools/copperline_oracle` reads the policy, and the numbers hold by hand.

The oracle is the third reading of `docs/finance-policy.md`: it was written
from the policy text and the raw tables, without the generator, the dbt
project or any task. This file is what keeps it honest.

Two kinds of test are here. The first kind is structural — the model runs
against a real small-profile build, it is deterministic, every figure is an
integer number of cents, the sub-lines add up, and every entity and closed
month in the data gets a row. The second kind is arithmetic that a person can
follow: each one names the raw rows it comes from and shows the multiplication
in a comment, so a reader can check the oracle against the policy without
running anything. That is the property the small profile exists for.

The world is built through the real CLI, once per session. The generator is
deterministic — two builds of the small profile produce the same model output
to the cent — so the hand-checked numbers below are written out in full.
"""

import csv
import io
import subprocess
import sys
from datetime import date
from pathlib import Path

import duckdb
import pytest

from de_bench.tasks import repo_root

sys.path.insert(0, str(repo_root() / "tools"))

from copperline_oracle.recognition import (  # noqa: E402
    BALANCES,
    COLUMNS,
    COMPONENTS,
    DAILY,
    LEGACY_MONTH,
    LEGACY_PREPAY,
    Calendar,
    Rates,
    daily_schedule,
    main,
    periodise,
    recognize,
)

WORLD = repo_root() / "worlds" / "copperline"

#: The last month the small profile closes. The policy's worked month says
#: "May 2026 is the most recent closed month. It closed on 2026-06-05, the 5th
#: business day of June", and the model agrees.
LAST_CLOSED = "FY2026-P04"
LAST_CLOSE_DATE = date(2026, 6, 5)


@pytest.fixture(scope="session")
def world(tmp_path_factory) -> Path:
    """A small-profile build, made by the CLI the world is built with.

    The hand-authored fixtures land afterwards, the way they do at bake time.
    `raw.entities` carries the entity coverage REV-9 turns on and
    `raw.gift_card_jurisdictions` carries the escheat column REV-12 reads, so
    the model has nothing to run against without them.
    """
    out = tmp_path_factory.mktemp("copperline-oracle")
    db = out / "copperline.duckdb"
    env = {"PATH": "/usr/bin:/bin", "PYTHONDONTWRITEBYTECODE": "1",
           "PYTHONPATH": str(repo_root() / "tools")}
    proc = subprocess.run(
        [sys.executable, "-m", "gen_copperline",
         "--timeline", str(WORLD / "timeline.yaml"),
         "--planted", str(WORLD / "planted.yaml"),
         "--db", str(db),
         "--landing", str(out / "landing"),
         "--profile", "small"],
        cwd=repo_root() / "tools", capture_output=True, text=True, env=env,
    )
    assert proc.returncode == 0, proc.stderr[-2000:]
    proc = subprocess.run(
        [sys.executable,
         str(repo_root() / "tools" / "gen_copperline_load_fixtures.py"),
         "--fixtures", str(WORLD / "workspace" / "fixtures"),
         "--db", str(db), "--landing", str(out / "landing")],
        capture_output=True, text=True, env=env,
    )
    assert proc.returncode == 0, proc.stderr[-2000:]
    return db


@pytest.fixture(scope="session")
def rows(world) -> list[dict]:
    return recognize(world)


@pytest.fixture(scope="session")
def cell(rows):
    """One (entity, month) row, by key."""
    index = {(r["entity_code"], r["fiscal_month"]): r for r in rows}
    return index


@pytest.fixture(scope="session")
def con(world):
    connection = duckdb.connect(str(world), read_only=True)
    yield connection
    connection.close()


@pytest.fixture(scope="session")
def calendar(con) -> Calendar:
    return Calendar(con.execute(
        "SELECT cal_date, fiscal_year, fiscal_period "
        "FROM raw.fiscal_calendar ORDER BY cal_date").fetchall())


# ---------------------------------------------------------------------------
# It runs, and it runs the same way twice
# ---------------------------------------------------------------------------

def test_model_produces_rows(rows):
    assert rows, "the small profile has closed months and should produce rows"
    for row in rows:
        assert tuple(row) == COLUMNS


def test_deterministic(world, rows):
    """Two runs over one world return the same table, to the cent."""
    again = recognize(world)
    assert again == rows


def test_every_figure_is_integer_cents(rows):
    """The policy's money section: every rule counts in integer cents, and
    "a money column carrying a float is a defect, not a rounding style"."""
    for row in rows:
        for column in (*COMPONENTS, *BALANCES, "recognized_cents"):
            value = row[column]
            assert type(value) is int, f"{column} is {type(value)}"


def test_components_sum_to_recognized(rows):
    """The sub-lines defend the number: they add up to it exactly."""
    for row in rows:
        assert sum(row[c] for c in COMPONENTS) == row["recognized_cents"], row


def test_sorted_by_entity_then_month(rows):
    keys = [(r["entity_code"], r["fiscal_month"]) for r in rows]
    assert keys == sorted(keys)
    assert len(keys) == len(set(keys)), "one row per entity and month"


def test_main_writes_the_same_table_as_csv(world, rows, capsys):
    assert main([str(world)]) == 0
    printed = list(csv.DictReader(io.StringIO(capsys.readouterr().out)))
    assert len(printed) == len(rows)
    assert [{k: int(v) if k not in ("entity_code", "fiscal_month") else v
             for k, v in r.items()} for r in printed] == rows


# ---------------------------------------------------------------------------
# REV-8 and REV-9: closed months, and only closed months
# ---------------------------------------------------------------------------

def test_only_closed_months_are_published(rows, calendar):
    """REV-9: the open month has no ledger row and no tie."""
    months = {r["fiscal_month"] for r in rows}
    assert max(months) == LAST_CLOSED
    closed = {m.key for m in calendar.months if m.close is not None
              and m.close <= LAST_CLOSE_DATE}
    assert months <= closed


def test_the_close_date_matches_the_policys_worked_month(calendar):
    """REV-8, checked against the one close date the policy writes down.

    FY2026 P5 opens on Sunday 2026-05-31, so the 5th business day runs
    1, 2, 3, 4, 5 June: May 2026 closes on 2026-06-05.
    """
    p4 = next(m for m in calendar.months if m.key == LAST_CLOSED)
    assert p4.end == date(2026, 5, 30)
    assert p4.close == LAST_CLOSE_DATE


def test_every_entity_and_closed_month_in_the_data_has_a_row(con, cell, rows):
    """Coverage: every entity that billed in a closed month is published.

    The pairs come from the invoices, joined to the shipped calendar — the
    calendar is read, never computed (CAL-2).
    """
    pairs = con.execute("""
        SELECT DISTINCT i.entity_code,
               f.fiscal_year || '-P' || lpad(CAST(f.fiscal_period AS VARCHAR), 2, '0')
        FROM raw.invoices i
        JOIN raw.fiscal_calendar f ON f.cal_date = i.invoice_date
    """).fetchall()
    published = set(cell)
    for entity, month in pairs:
        if month > LAST_CLOSED:
            continue          # an open month has no ledger row
        assert (entity, month) in published, f"{entity} {month} is missing"

    # And each entity's months run without a gap, so a month with no activity
    # still carries its closing balances.
    for entity in {r["entity_code"] for r in rows}:
        months = [r["fiscal_month"] for r in rows if r["entity_code"] == entity]
        assert months == sorted(months)
        assert months[-1] == LAST_CLOSED


# ---------------------------------------------------------------------------
# REV-1 and REV-2, by hand
# ---------------------------------------------------------------------------

def test_rev2_worked_example():
    """The example REV-2 writes out itself.

    "A 4,097-cent line over a 31-day period therefore recognizes 132 cents a
    day for the first 30 days and 137 cents on the 31st."

        floor(4097 / 31) = 132        132 * 31 = 4092
        remainder        = 4097 - 4092 = 5      132 + 5 = 137
    """
    schedule = daily_schedule(4097, date(2026, 3, 1), date(2026, 3, 31))
    assert len(schedule) == 31, "REV-1: both end days are inside the period"
    assert [cents for _day, cents in schedule[:30]] == [132] * 30
    assert schedule[-1] == (date(2026, 3, 31), 137)
    assert sum(cents for _day, cents in schedule) == 4097


def test_periodise_agrees_with_the_day_by_day_schedule(calendar):
    """The fast path and the readable path are the same arithmetic.

    Three shapes: the policy's own 31-day line, a line that divides exactly
    (6,180 over 30 days is 206 a day and no remainder), and a full year that
    crosses thirteen fiscal months. Both allocated grains are checked;
    `LEGACY_MONTH` is not allocated at all, which is REV-11's whole point.
    """
    cases = [
        (4097, date(2026, 3, 1), date(2026, 3, 31)),
        (6180, date(2025, 4, 1), date(2025, 4, 30)),
        (1_200_000, date(2025, 3, 2), date(2026, 3, 1)),
    ]
    for cents, start, end in cases:
        by_day = daily_schedule(cents, start, end)
        wanted: dict[str, int] = {}
        for day, amount in by_day:
            key = calendar.key_of(day)
            wanted[key] = wanted.get(key, 0) + amount
        for grain in (DAILY, LEGACY_PREPAY):
            got: dict[str, int] = {}
            for month, _on, amount in periodise(
                    calendar, cents, start, end, grain=grain):
                got[month.key] = got.get(month.key, 0) + amount
            assert got == wanted, (cents, start, end, grain)


def test_legacy_lands_on_the_first_day_of_the_fiscal_month(calendar, con):
    """REV-11, on one row of `raw.plan_lines`.

    `PL-0000108`, invoiced on `INV-0003144`: 13,582 cents, service
    2024-02-11 to 2024-03-10, `billing_era = 'legacy'`,
    `billing_frequency = 'monthly'`.

    The service period is a calendar month and it straddles two fiscal ones,
    FY2024 P01 (2024-02-04 .. 2024-03-02) and FY2024 P02. REV-11: "the whole
    of its amount lands on the first day of the fiscal month its service
    period starts in... nothing of it reaches the next one." So the whole
    13,582 lands on 2024-02-04 and P02 gets nothing.

    A current-era line of the same shape is cut on the day instead:

        days      = 29                       (2024-02-11 .. 2024-03-10)
        daily     = floor(13582 / 29) = 468  468 * 29 = 13,572
        remainder = 13582 - 13572     =  10  on the final day, 2024-03-10

        P01: 21 days (02-11..03-02) 21 * 468        =  9,828
        P02:  8 days (03-03..03-10)  8 * 468 + 10   =  3,754
                                                       ------
                                                       13,582

    That is the difference REV-11 is about, and it is why a legacy line cut to
    daily grain is wrong at the month and not only at the day.
    """
    cents, start, end = con.execute("""
        SELECT l.line_total_cents, l.service_start, l.service_end
        FROM raw.invoice_lines l
        WHERE l.plan_line_id = 'PL-0000108'
    """).fetchone()
    assert (cents, start, end) == (13582, date(2024, 2, 11), date(2024, 3, 10))

    legacy = periodise(calendar, cents, start, end, grain=LEGACY_MONTH)
    assert [(m.key, on, amount) for m, on, amount in legacy] == [
        ("FY2024-P01", date(2024, 2, 4), 13582),
    ]

    current = periodise(calendar, cents, start, end, grain=DAILY)
    assert [(m.key, on, amount) for m, on, amount in current] == [
        ("FY2024-P01", date(2024, 3, 2), 9828),
        ("FY2024-P02", date(2024, 3, 10), 3754),
    ]


def test_rev6_void_reverses_everything_recognized(calendar, con):
    """REV-6's 14-day boundary, on one row of `raw.plan_lines`.

    `PL-0032406` on `INV-0026354`: 12,855 cents, invoiced 2025-09-29, service
    2025-09-29 to 2025-10-28, cancelled 2025-10-13. That is day 14, and "day
    14 is inside the window", so the refund voids the schedule.

        days      = 30
        daily     = floor(12855 / 30) = 428  428 * 30 = 12,840
        remainder = 12855 - 12840     =  15  on 2025-10-28

        FY2025-P08 (.. 2025-10-04):  6 days * 428  =  2,568
        FY2025-P09 (2025-10-05 ..):  9 days * 428  =  3,852
                                                      ------
                                    recognized to date  6,420

    Recognition stops at the refund, so the remainder never books and the
    schedule stops nine days into P09. The 6,420 that did recognize reverses
    in FY2025-P09, the month the refund was requested in, and the line comes
    out at nothing. Nothing restates FY2025-P08: REV-8 says a closed month is
    final.
    """
    cents, invoice_date, start, end, cancelled = con.execute("""
        SELECT l.line_total_cents, i.invoice_date, l.service_start,
               l.service_end, p.cancelled_on
        FROM raw.invoice_lines l
        JOIN raw.invoices i USING (invoice_id)
        JOIN raw.plan_lines p USING (plan_line_id)
        WHERE l.plan_line_id = 'PL-0032406'
    """).fetchone()
    assert (cents, invoice_date, cancelled) == (
        12855, date(2025, 9, 29), date(2025, 10, 13))
    assert (cancelled - invoice_date).days == 14

    schedule = periodise(calendar, cents, start, end,
                         grain=DAILY, through=cancelled)
    assert [(m.key, amount) for m, _on, amount in schedule] == [
        ("FY2025-P08", 2568),
        ("FY2025-P09", 3852),
    ]
    assert sum(amount for _m, _on, amount in schedule) == 6420


def test_rev6_day_fifteen_cancels_forward(calendar, con):
    """The other side of the boundary: "a refund on day 15 cancels."

    `PL-0022602` on `INV-0013734`: 12,676 cents, invoiced 2024-12-12, service
    2024-12-12 to 2025-01-06, cancelled 2024-12-27, which is day 15.
    "Recognition to date stands and nothing further recognizes", so the
    schedule stops on 2024-12-27 and nothing reverses.

        days      = 26
        daily     = floor(12676 / 26) = 487  487 * 26 = 12,662
        remainder = 12676 - 12662     =  14  on 2025-01-06, which never books

        FY2024-P11: 16 days (12-12..12-27) * 487 = 7,792

    The line recognizes 7,792 of its 12,676 and stops. The rest is refunded
    and never was revenue.
    """
    cents, invoice_date, start, end, cancelled = con.execute("""
        SELECT l.line_total_cents, i.invoice_date, l.service_start,
               l.service_end, p.cancelled_on
        FROM raw.invoice_lines l
        JOIN raw.invoices i USING (invoice_id)
        JOIN raw.plan_lines p USING (plan_line_id)
        WHERE l.plan_line_id = 'PL-0022602'
    """).fetchone()
    assert (cancelled - invoice_date).days == 15
    cut = periodise(calendar, cents, start, end, grain=DAILY, through=cancelled)
    full = periodise(calendar, cents, start, end, grain=DAILY)
    assert [(m.key, amount) for m, _on, amount in cut] == [("FY2024-P11", 7792)]
    assert sum(a for _m, _on, a in full) == cents
    assert sum(a for _m, _on, a in cut) < cents


def test_a_cancellation_lands_inside_the_service_period(con):
    """What REV-6's forward cut has to bite on.

    A `cancelled_on` is the day the customer asked for the money back, and it
    sits inside the term the line bills. A line that ran to its term end was
    not cancelled at all and carries no date. So the forward cut always
    removes something, and both branches of REV-6 have a population.
    """
    inside, total = con.execute("""
        SELECT SUM(CASE WHEN p.cancelled_on BETWEEN l.service_start
                                             AND l.service_end - 1
                        THEN 1 ELSE 0 END),
               COUNT(*)
        FROM raw.plan_lines p
        JOIN raw.invoice_lines l USING (plan_line_id)
        WHERE p.cancelled_on IS NOT NULL
    """).fetchone()
    assert total > 0 and inside == total


def test_the_void_reversals_add_up(rows, con):
    """REV-6 across the world: a line refunded inside the 14-day window comes
    out at nothing.

    The reversal is the recognized-to-date amount, not the whole line: the
    part after the refund never recognized, so there is nothing there to
    reverse. That amount depends on the grain. A current-era line recognized
    day by day up to the refund, per REV-2. A legacy line billed monthly
    landed whole on the first day of its fiscal month, which is on or before
    the refund, so the whole of it recognized and the whole of it reverses.

    Both are recomputed here in SQL, straight from the clauses.
    """
    current, legacy = con.execute("""
        WITH v AS (
            SELECT i.billing_era,
                   CAST(ROUND(
                       CAST(l.line_total_cents AS DECIMAL(38, 6))
                       * COALESCE(f.rate_to_usd_ppm, 1000000) / 1000000, 0)
                   AS BIGINT) AS usd_cents,
                   l.service_end - l.service_start + 1 AS days,
                   p.cancelled_on - l.service_start + 1 AS covered
            FROM raw.plan_lines p
            JOIN raw.invoice_lines l USING (plan_line_id)
            JOIN raw.invoices i USING (invoice_id)
            LEFT JOIN raw.fx_rates f
              ON f.rate_date = i.invoice_date
             AND f.currency_code = i.currency_code
            WHERE p.cancelled_on IS NOT NULL
              AND datediff('day', i.invoice_date, p.cancelled_on) <= 14
              AND p.cancelled_on <= DATE '2026-05-30'
              AND p.billing_frequency = 'monthly'
        )
        SELECT SUM(CASE WHEN billing_era = 'current'
                        THEN (usd_cents // days) * covered ELSE 0 END),
               SUM(CASE WHEN billing_era = 'legacy' THEN usd_cents ELSE 0 END)
        FROM v
    """).fetchone()
    assert sum(r["refund_reversal_cents"] for r in rows) == -(current + legacy)


def test_rev7_converts_once_at_the_stated_rate(con):
    """One line of `INV-0033730`, converted the way REV-7 says.

    The invoice is MXN, dated 2026-04-09, and `raw.fx_rates` holds
    58,530 parts per million for MXN on that date.

        444,096 * 58,530 / 1,000,000 = 25,992.93888 -> 25,993 (half up)
    """
    ppm = con.execute(
        "SELECT rate_to_usd_ppm FROM raw.fx_rates "
        "WHERE rate_date = DATE '2026-04-09' AND currency_code = 'MXN'"
    ).fetchone()[0]
    assert ppm == 58530
    rates = Rates([(date(2026, 4, 9), "MXN", ppm)])
    assert rates.convert(444096, "MXN", date(2026, 4, 9)) == 25993
    # REV-7: a NULL currency is USD by construction, not a missing rate.
    assert rates.convert(444096, None, date(2026, 4, 9)) == 444096


def test_marketplace_month_adds_up_by_hand(cell, con):
    """REV-14 for CL-MX in FY2026-P01, order by order.

    Mexico had no invoices before March 2026, so this month is nothing but
    marketplace commission and fulfilment fees. Thirteen MXN orders confirm
    shipping in the month; each one is (commission + fee) at the rate for the
    ship-confirmation business date, half up to the cent:

        MO-00015004   4460 +  959 =  5419 * 56469 =   306.005511 ->  306
        MO-00015027   8283 +  870 =  9153 * 56235 =   514.718955 ->  515
        MO-00015028   6257 + 1238 =  7495 * 56469 =   423.235155 ->  423
        MO-00015031   1907 +  340 =  2247 * 56377 =   126.679119 ->  127
        MO-00015037   1910 +  531 =  2441 * 56372 =   137.604052 ->  138
        MO-00015161   3519 +  462 =  3981 * 56609 =   225.360429 ->  225
        MO-00015173   5130 +  892 =  6022 * 56742 =   341.700324 ->  342
        MO-00015262   1272 +  299 =  1571 * 56527 =    88.803917 ->   89
        MO-00015302  10134 + 2169 = 12303 * 56713 =   697.740039 ->  698
        MO-00015340   2556 +  594 =  3150 * 56867 =   179.131050 ->  179
        MO-00015375   6688 + 1001 =  7689 * 56713 =   436.066257 ->  436
        MO-00015399    773 +  175 =   948 * 57100 =    54.130800 ->   54
        MO-00015436  13061 + 1308 = 14369 * 56756 =   815.526964 ->  816
                                                                   -----
                                                                   4,348

    Two of the thirteen are in the month only because the day is a
    head-office day: MO-00015037 confirms at 2026-02-04 07:27 UTC and
    MO-00015436 at 2026-02-26 06:34 UTC, and both are the day before in
    America/Los_Angeles.
    """
    row = cell[("CL-MX", "FY2026-P01")]
    assert row["marketplace_cents"] == 4348
    assert row["recognized_cents"] == 4348
    assert row["point_in_time_cents"] == 0
    assert con.execute("""
        SELECT COUNT(*) FROM raw.invoices
        WHERE entity_code = 'CL-MX' AND invoice_date <= DATE '2026-02-28'
    """).fetchone()[0] == 0


def test_gift_card_redemption_uses_the_issue_date_rate(cell, con):
    """REV-12 for CL-MX in FY2026-P03, on the one card that was redeemed.

    `GC-00750007` was issued on 2026-02-23 for 2,500 MXN cents and redeemed
    on 2026-04-29. MXN was 56,671 parts per million on the issue date:

        2,500 * 56,671 / 1,000,000 = 141.6775 -> 142 (half up)

    The redemption-date rate was 59,186, which would have given 148. REV-7
    allows one conversion and forbids "re-translation on the recognition
    date", so the issue-date rate is the one used.
    """
    card = con.execute("""
        SELECT g.initial_cents, g.currency_code, CAST(g.issued_at AS DATE)
        FROM raw.gift_cards g WHERE g.card_id = 'GC-00750007'
    """).fetchone()
    assert card == (2500, "MXN", date(2026, 2, 23))
    assert cell[("CL-MX", "FY2026-P03")]["gift_card_redemption_cents"] == 142


# ---------------------------------------------------------------------------
# Whole-column cross-checks
# ---------------------------------------------------------------------------

def test_point_in_time_matches_the_invoice_lines(rows, con):
    """REV-1's last sentence, summed across the world.

    Every line with no `service_end` recognizes in full on the invoice date,
    so the column is the converted total of those lines up to the end of the
    last closed month. The check is run in DECIMAL, never in a float.
    """
    total = con.execute("""
        SELECT SUM(CAST(ROUND(
            CAST(l.line_total_cents AS DECIMAL(38, 6))
            * COALESCE(f.rate_to_usd_ppm, 1000000) / 1000000, 0) AS BIGINT))
        FROM raw.invoice_lines l
        JOIN raw.invoices i USING (invoice_id)
        LEFT JOIN raw.fx_rates f
          ON f.rate_date = i.invoice_date
         AND f.currency_code = i.currency_code
        WHERE l.service_end IS NULL
          AND i.invoice_date <= DATE '2026-05-30'
    """).fetchone()[0]
    assert sum(r["point_in_time_cents"] for r in rows) == total


def test_credit_memos_match_the_source(rows, con):
    """REV-5, summed: every memo issued up to the end of the last closed
    month, converted at the rate of the invoice it credits."""
    total = con.execute("""
        SELECT SUM(CAST(ROUND(
            CAST(cm.amount_cents AS DECIMAL(38, 6))
            * COALESCE(f.rate_to_usd_ppm, 1000000) / 1000000, 0) AS BIGINT))
        FROM raw.credit_memos cm
        JOIN raw.invoices i USING (invoice_id)
        LEFT JOIN raw.fx_rates f
          ON f.rate_date = i.invoice_date
         AND f.currency_code = COALESCE(cm.currency_code, i.currency_code)
        WHERE cm.issued_on <= DATE '2026-05-30'
    """).fetchone()[0]
    assert sum(r["credit_memo_cents"] for r in rows) == -total


def test_the_trade_book_is_a_us_entity_only(rows):
    """Only CL-US invoices carry plan lines in this world, so only CL-US has
    ratable revenue. The other entities bill goods and freight."""
    for row in rows:
        if row["entity_code"] != "CL-US":
            assert row["ratable_current_cents"] == 0, row
            assert row["ratable_legacy_cents"] == 0, row
