"""FIN-455 — does a recognition day hold what REV-12 says recognizes on it?

Never ships to the agent. It runs beside the scored tree, with the tree root on
`PYTHONPATH`, so it imports the patched module the same way the DAG does.

**Why a verifier rather than a DAG run.** The subject is what a run of days
leaves behind, and seven scheduler runs buy nothing the direct call does not.
The verifier calls `gift_cards.recognize(ds)` itself, day after day, which
grades the same code in seconds and removes the shortcut: a day typed into the
table by hand is a day the replay replaces before anything reads it.

**No authored numbers.** Every expected result is computed from
`raw.gift_card_ledger`, `raw.gift_cards`, `raw.gift_card_jurisdictions`,
`raw.fiscal_calendar` and `raw.fx_rates` in the same session. All five are
read-only landing tables, so a tree edit cannot move the answer.

**Seven days, and what each is for.**

    2026-03-30, 2026-04-03, 2026-04-05, 2026-04-30   ordinary days: redemptions
                                                     and no breakage at all
    2026-03-31                                       a CALENDAR month end inside
                                                     fiscal P02, which is not a
                                                     fiscal month end and carries
                                                     no breakage either
    2026-04-04                                       the last day of FY2026 P02
    2026-05-02                                       the last day of FY2026 P03

FY2026 is 4-5-4: P02 runs 2026-03-01 to 2026-04-04 and P03 runs 2026-04-05 to
2026-05-02, so neither fiscal month end is a calendar month end and neither
calendar month end is a fiscal one. The span sits outside both reserved
windows, and outside FY2026 P04, which the close build-up task grades.

**Two roundings, both accepted.** The base-currency columns convert per entry
and the house rule is half-up on the single division. `macros/money.sql` spells
that as an integer add of 500000 and one divide, but DuckDB returns a DOUBLE
from a decimal divide and `cast(x AS BIGINT)` then rounds an exact half to
even — so the macro's own shape and a plain `round()` disagree by up to a cent
per entry. Both are honest readings of one written rule, so both pass; the wrong
RATE moves the same columns by thousands, which neither of them can reach. The
six native-currency columns are graded exactly. See `checks.yaml`.
"""

from __future__ import annotations

import duckdb

# The scored tree is on PYTHONPATH (scoring.build_score_env), so the house
# library resolves the warehouse the way every DAG in the world does.
from include.lib import warehouse

DB = str(warehouse.warehouse_path())

# Every name is qualified onto the live warehouse, for the reason
# `include/lib/warehouse.py` gives: the frozen `nwv` copy sits first on the
# search path, so an unqualified read can answer with another book's numbers.
LEDGER = warehouse.qualify("raw.gift_card_ledger")
CARDS = warehouse.qualify("raw.gift_cards")
JURISDICTIONS = warehouse.qualify("raw.gift_card_jurisdictions")
FISCAL = warehouse.qualify("raw.fiscal_calendar")
RATES = warehouse.qualify("raw.fx_rates")
TARGET = warehouse.qualify("marts.gift_card_recognition")

#: The days the replay runs, oldest first. See the module docstring.
GRADED = ("2026-03-30", "2026-03-31", "2026-04-03", "2026-04-04",
          "2026-04-05", "2026-04-30", "2026-05-02")

#: The two fiscal month ends in the span. Nothing but these two may carry
#: breakage.
MONTH_ENDS = ("2026-04-04", "2026-05-02")

#: A recognition day months before anything the replay reaches, used once to
#: prove a run writes only the day it was given. Outside both reserved windows.
UNTOUCHED_DAY = "2025-11-05"

#: The nine columns the ticket asks for. A table carrying more than these is
#: not convicted for it; one missing a column fails to read at all.
COLUMNS = ("ds", "entity_code", "currency_code", "redeemed_cents",
           "breakage_cents", "escheated_cents", "redeemed_base_cents",
           "breakage_base_cents", "cards")

#: Where the row's key ends and its figures begin.
KEY = 3

#: The two base columns, by position in `COLUMNS`.
BASE_AT = (6, 7)

# Every entry that recognizes anything, dated and priced. The placeholders are
# what each wrong reading changes:
#
#   __BREAK_DS__   which day a breakage lands on
#   __RATE_DATE__  which date the card's rate is read at
#   __REDEEM__     whether an escheat jurisdiction's redemptions recognize
#   __BREAKAGE__   which breakage is revenue
#   __ESCHEAT__    which breakage is not
_ENTRIES = f"""
WITH period AS (
    SELECT cal_date,
           max(cal_date) OVER (PARTITION BY fiscal_year, fiscal_period)
                                                    AS period_end
    FROM {FISCAL}
),
card AS (
    SELECT g.card_id, g.currency_code, g.issued_at::DATE AS issue_date,
           j.escheat_applies
    FROM {CARDS} g
    JOIN {JURISDICTIONS} j ON j.jurisdiction_code = g.jurisdiction_code
),
entry AS (
    SELECT l.card_id, l.entry_type, l.entity_code,
           -l.amount_cents                           AS cents,
           CASE WHEN l.entry_type = 'redeem' THEN l.occurred_at::DATE
                ELSE __BREAK_DS__ END                AS ds,
           l.occurred_at
    FROM {LEDGER} l
    LEFT JOIN period p ON p.cal_date = l.occurred_at::DATE
    WHERE l.entry_type IN ('redeem', 'breakage')
)
SELECT e.ds, e.entity_code, c.currency_code, e.card_id, e.entry_type,
       e.cents, c.escheat_applies,
       coalesce(r.rate_to_usd_ppm, 1000000)          AS rate_ppm
FROM entry e
JOIN card c ON c.card_id = e.card_id
LEFT JOIN {RATES} r ON r.currency_code = c.currency_code
                   AND r.rate_date = __RATE_DATE__
WHERE e.ds IN ({{marks}})
"""

#: Half-up on the single division, in integer arithmetic — the rule as
#: `macros/money.sql` states it in words.
_HALF_UP = "((cents * rate_ppm + 500000) // 1000000)"

#: The same expression as the macro writes it, which DuckDB evaluates as a
#: DOUBLE and then rounds half to even on the cast.
_MACRO = ("cast((cast(cents AS DECIMAL(38,0)) * cast(rate_ppm AS DECIMAL(38,0))"
          " + 500000) / 1000000 AS BIGINT)")

_FIGURES = """
SELECT ds, entity_code, currency_code,
       coalesce(sum(cents) FILTER (entry_type = 'redeem' AND __REDEEM__),
                0)::BIGINT,
       coalesce(sum(cents) FILTER (entry_type = 'breakage' AND __BREAKAGE__),
                0)::BIGINT,
       coalesce(sum(cents) FILTER (entry_type = 'breakage' AND __ESCHEAT__),
                0)::BIGINT,
       coalesce(sum({base}) FILTER (entry_type = 'redeem' AND __REDEEM__),
                0)::BIGINT,
       coalesce(sum({base}) FILTER (entry_type = 'breakage' AND __BREAKAGE__),
                0)::BIGINT,
       count(DISTINCT card_id)::BIGINT
FROM ({entries})
GROUP BY 1, 2, 3
"""

#: The right answer. Each key is one sentence of REV-12. `__BREAK_DS__` and
#: `__RATE_DATE__` are read where the ledger row is still aliased `l` and the
#: card row `c`; the three filters are read off the subquery, where the columns
#: are bare.
TRUTH = {
    "__BREAK_DS__": "p.period_end",
    "__RATE_DATE__": "c.issue_date",
    "__REDEEM__": "TRUE",
    "__BREAKAGE__": "NOT escheat_applies",
    "__ESCHEAT__": "escheat_applies",
}


def query(sql: str, params: list | None = None) -> list[tuple]:
    """One read, on its own connection. DuckDB takes one writer and the build
    under test opens its own, so nothing here may hold the file open."""
    con = duckdb.connect(DB)
    try:
        return con.execute(sql, params or []).fetchall()
    finally:
        con.close()


def figures(days: tuple[str, ...], base: str = _HALF_UP, **override) -> dict:
    """The expected rows over those days, keyed by (ds, entity, currency)."""
    reading = {**TRUTH, **override}
    marks = ", ".join("?::DATE" for _ in days)
    entries = _ENTRIES.format(marks=marks)
    sql = _FIGURES.format(base=base, entries=entries)
    for name, expression in reading.items():
        sql = sql.replace(name, expression)
    rows = query(sql, list(days))
    return {tuple(str(v) for v in row[:KEY]): tuple(row[KEY:]) for row in rows}


def built(days: tuple[str, ...]) -> list[tuple]:
    """What the table holds for those days, as rows. A list rather than a dict,
    so a day appended to twice is visible."""
    marks = ", ".join("?::DATE" for _ in days)
    columns = ", ".join(COLUMNS)
    return query(f"SELECT {columns} FROM {TARGET} WHERE ds IN ({marks})",
                 list(days))


def run(ds: str) -> int:
    """One day, the way `recognize` runs in the DAG."""
    from projects.finance.lib import gift_cards

    return gift_cards.recognize(ds)


def forget(days: tuple[str, ...]) -> None:
    """Forget those days, so a replay starts where a first run would. The table
    may not exist yet on the first test that calls this."""
    marks = ", ".join("?::DATE" for _ in days)
    con = duckdb.connect(DB)
    try:
        con.execute(f"DELETE FROM {TARGET} WHERE ds IN ({marks})", list(days))
    except duckdb.Error:
        pass
    finally:
        con.close()


def compare(days: tuple[str, ...]) -> list[str]:
    """Every way the table disagrees with the feed over those days."""
    rows = built(days)
    problems: list[str] = []

    actual: dict[tuple, tuple] = {}
    for row in rows:
        key = tuple(str(v) for v in row[:KEY])
        if key in actual:
            problems.append(f"{key} carries more than one row")
        actual[key] = tuple(row[KEY:])

    half_up = figures(days)
    macro = figures(days, base=_MACRO)

    missing = sorted(set(half_up) - set(actual))
    spare = sorted(set(actual) - set(half_up))
    if missing:
        problems.append(f"{len(missing)} row(s) the feed carries and the table "
                        f"does not, first {missing[0]}")
    if spare:
        problems.append(f"{len(spare)} row(s) the feed does not support, "
                        f"first {spare[0]}")

    for key in sorted(set(half_up) & set(actual)):
        want, got = half_up[key], actual[key]
        for at in range(len(want)):
            column = COLUMNS[KEY + at]
            if at in (BASE_AT[0] - KEY, BASE_AT[1] - KEY):
                if got[at] not in (want[at], macro[key][at]):
                    problems.append(
                        f"{key} {column}: {got[at]} against {want[at]} "
                        f"(or {macro[key][at]} on the macro's own rounding)")
            elif got[at] != want[at]:
                problems.append(
                    f"{key} {column}: {got[at]} against {want[at]}")
    return problems


def test_the_graded_days_can_tell_the_answers_apart():
    """The fixture's own guard.

    The span is only worth grading if every clause of REV-12 is live on it. If
    the card book ever changes shape, or the days drift off the fiscal months
    that carry the breakage, this fails here rather than passing a task that
    measures nothing.
    """
    truth = figures(GRADED)
    assert truth, "FAIL: nothing recognizes on the graded days"

    broke = {key[0] for key, row in truth.items() if row[1] or row[2]}
    assert broke == set(MONTH_ENDS), (
        f"FAIL: breakage lands on {sorted(broke)} and the fiscal month ends in "
        f"the span are {list(MONTH_ENDS)}"
    )
    escheated = sum(row[2] for row in truth.values())
    recognized = sum(row[1] for row in truth.values())
    assert escheated and recognized, (
        f"FAIL: {recognized} cents of breakage recognize and {escheated} "
        "escheat — the span cannot grade the escheat rule"
    )

    for name, override in (
        ("breakage on its own age-out day",
         {"__BREAK_DS__": "l.occurred_at::DATE"}),
        ("breakage on the calendar month end",
         {"__BREAK_DS__": "(date_trunc('month', l.occurred_at)"
                          " + INTERVAL 1 MONTH - INTERVAL 1 DAY)::DATE"}),
        ("every breakage recognized",
         {"__BREAKAGE__": "TRUE", "__ESCHEAT__": "FALSE"}),
        ("the escheat jurisdictions dropped whole",
         {"__REDEEM__": "NOT escheat_applies"}),
        ("the rate read at the entry date",
         {"__RATE_DATE__": "e.occurred_at::DATE"}),
        ("the rate read at the recognition day",
         {"__RATE_DATE__": "e.ds"}),
    ):
        assert figures(GRADED, **override) != truth, (
            f"FAIL: {name} already agrees with the feed over the graded days, "
            "so the span cannot grade the fix"
        )


def test_the_replay_leaves_every_recognition_day_whole():
    """The substance. Seven days, and each holds what REV-12 recognizes on it.
    """
    forget(GRADED)
    for day in GRADED:
        run(day)
    problems = compare(GRADED)
    assert not problems, "FAIL: " + "; ".join(problems[:6])


def test_running_a_day_twice_leaves_the_same_table():
    """A day that is re-run replaces itself and changes nothing."""
    day = MONTH_ENDS[-1]
    run(day)
    once = sorted(built(GRADED))
    run(day)
    assert sorted(built(GRADED)) == once, (
        f"FAIL: the second run of {day} moved the table"
    )


def test_a_run_leaves_the_days_it_was_not_given_alone():
    """A run writes the day it was given and no other.

    The warehouse takes one writer and `projects/platform/README.md` forbids a
    full rebuild on it, so a build that rewrites the table's history holds the
    file while every other team queues. The row planted here belongs to a
    recognition day five months before anything the replay reaches. It is
    copied off a graded day rather than typed, so a table carrying extra
    columns is planted into as happily as one that does not.
    """
    con = duckdb.connect(DB)
    try:
        con.execute(f"DELETE FROM {TARGET} WHERE ds = ?::DATE", [UNTOUCHED_DAY])
        con.execute(
            f"INSERT INTO {TARGET} "
            f"SELECT * REPLACE (?::DATE AS ds, 424242 AS cards) "
            f"FROM {TARGET} WHERE ds = ?::DATE LIMIT 1",
            [UNTOUCHED_DAY, GRADED[0]],
        )
        planted = con.execute(
            f"SELECT count(*) FROM {TARGET} "
            "WHERE ds = ?::DATE AND cards = 424242", [UNTOUCHED_DAY],
        ).fetchone()[0]
    finally:
        con.close()
    assert planted == 1, (
        f"FAIL: the table holds no row for {GRADED[0]} to copy, so the graded "
        "days were never written"
    )

    run(GRADED[-2])

    survived = query(
        f"SELECT count(*) FROM {TARGET} WHERE ds = ?::DATE AND cards = 424242",
        [UNTOUCHED_DAY],
    )[0][0]
    con = duckdb.connect(DB)
    try:
        con.execute(f"DELETE FROM {TARGET} WHERE ds = ?::DATE", [UNTOUCHED_DAY])
    finally:
        con.close()
    assert survived == 1, (
        f"FAIL: the run of {GRADED[-2]} rewrote {UNTOUCHED_DAY}, which it was "
        "not given"
    )
