"""FIN-431 — does the restated year hold what FY2023 actually took?

Never ships to the agent. It runs beside the scored tree, with the tree root on
`PYTHONPATH`, so it imports the patched module the same way the DAG would.

**Why a verifier rather than a DAG run.** The job reads two landed tables and
writes one mart, so a run adds nothing a direct call does not have, and the
world ships no Airflow state for a manually triggered DAG to inherit. The
verifier calls `build_year` once and reads what it wrote.

**No authored numbers.** Every expected result is computed from
`raw.pos_sales_daily`, `raw.fiscal_calendar` and `raw.stores` in the same
session. The graded figures are the world's own.

**Three weeks, and the whole year.**

    FY2024 W5   2024-03-03  a plain mid-period week the ticket does not name
    FY2024 W23  2024-07-07  half a year from either boundary
    FY2024 W52  2025-01-26  the week that compares to the 53rd week of FY2023

The year-wide tests carry the rest: 52 weeks, the mapping on every one of them,
and a total that ties to the book the figures come out of.
"""

from __future__ import annotations

import duckdb
import pytest

# The scored tree is on PYTHONPATH (scoring.build_score_env), so the house
# library resolves the warehouse the same way every DAG in the world does.
from include.lib import warehouse

DB = str(warehouse.warehouse_path())

#: Every name is qualified onto the live warehouse: the frozen `nwv` copy sits
#: first on the search path and carries `pos_sales_daily`, `stores` and
#: `fiscal_calendar` under the same names, so an unqualified read here would
#: answer with Northwave's numbers.
BOOK = warehouse.qualify("raw.pos_sales_daily")
CALENDAR = warehouse.qualify("raw.fiscal_calendar")
STORES = warehouse.qualify("raw.stores")
TABLE = "marts.comp_prior_year_weekly"
MART = warehouse.qualify(TABLE)

FISCAL_YEAR = "FY2024"
PRIOR_YEAR = "FY2023"

#: See the module docstring. The ticket names none of them.
W5 = "2024-03-03"
W23 = "2024-07-07"
W52 = "2025-01-26"
GRADED = (W5, W23, W52)

#: The estate, one row per store. Which version you take does not matter — a
#: store keeps its market across every version it has, which
#: `test_a_store_has_one_market_however_many_versions_it_has` holds to.
ESTATE = f"SELECT store_id, max(market_code) AS market_code FROM {STORES} GROUP BY store_id"

#: The expected result: what the estate took in the week the calendar names, by
#: market. Nothing behind it but the store day book and `comp_date_ly`.
TRUTH = f"""
WITH days AS (
    SELECT week_start, comp_date_ly
    FROM {CALENDAR}
    WHERE fiscal_year = '{FISCAL_YEAR}' AND comp_date_ly IS NOT NULL
),
estate AS ({ESTATE})
SELECT e.market_code,
       sum(b.gross_cents - b.discount_cents)::BIGINT AS takings_cents_ly,
       count(DISTINCT b.store_id)::INTEGER           AS store_count_ly
FROM days d
JOIN {BOOK} b ON b.business_date = d.comp_date_ly
JOIN estate e ON e.store_id = b.store_id
WHERE d.week_start = ?
GROUP BY 1
ORDER BY 1
"""

#: The reading the shipped job takes: the week of the same number in the year
#: before. FY2023 held 53 weeks, so it is a week early all year and never
#: reaches the 53rd. Kept here to prove the graded weeks can tell the two apart.
SAME_WEEK = f"""
WITH this_week AS (
    SELECT DISTINCT fiscal_week FROM {CALENDAR}
    WHERE fiscal_year = '{FISCAL_YEAR}' AND week_start = ?
),
last_year AS (
    SELECT cal_date, fiscal_week FROM {CALENDAR} WHERE fiscal_year = '{PRIOR_YEAR}'
),
estate AS ({ESTATE})
SELECT e.market_code,
       sum(b.gross_cents - b.discount_cents)::BIGINT AS takings_cents_ly,
       count(DISTINCT b.store_id)::INTEGER           AS store_count_ly
FROM this_week t
JOIN last_year ly ON ly.fiscal_week = t.fiscal_week
JOIN {BOOK} b ON b.business_date = ly.cal_date
JOIN estate e ON e.store_id = b.store_id
GROUP BY 1
ORDER BY 1
"""

#: The other reading the shipped job takes: the book joined to every row of an
#: SCD2 store table, which counts a store that has a prior version twice.
FANNED_OUT = f"""
WITH days AS (
    SELECT week_start, comp_date_ly
    FROM {CALENDAR}
    WHERE fiscal_year = '{FISCAL_YEAR}' AND comp_date_ly IS NOT NULL
)
SELECT s.market_code,
       sum(b.gross_cents - b.discount_cents)::BIGINT AS takings_cents_ly,
       count(DISTINCT b.store_id)::INTEGER           AS store_count_ly
FROM days d
JOIN {BOOK} b ON b.business_date = d.comp_date_ly
JOIN {STORES} s ON s.store_id = b.store_id
WHERE d.week_start = ?
GROUP BY 1
ORDER BY 1
"""

#: The mapping the calendar names, for every week of the year: which FY2023
#: week each FY2024 week compares to, and the Sunday it opens on.
MAPPING = f"""
SELECT DISTINCT d.week_start, d.fiscal_week, ly.fiscal_week, ly.week_start
FROM {CALENDAR} d
JOIN {CALENDAR} ly ON ly.cal_date = d.comp_date_ly
WHERE d.fiscal_year = '{FISCAL_YEAR}'
ORDER BY 1
"""

#: What the whole year has to add up to: the book's takings over every FY2023
#: day some FY2024 day compares to, each store counted once.
YEAR_TOTAL = f"""
WITH days AS (
    SELECT DISTINCT comp_date_ly AS cal_date
    FROM {CALENDAR}
    WHERE fiscal_year = '{FISCAL_YEAR}' AND comp_date_ly IS NOT NULL
),
estate AS ({ESTATE})
SELECT sum(b.gross_cents - b.discount_cents)::BIGINT
FROM days d
JOIN {BOOK} b ON b.business_date = d.cal_date
JOIN estate e ON e.store_id = b.store_id
"""


def query(sql: str, params: list | None = None) -> list[tuple]:
    """One read, on its own connection. DuckDB takes one writer and the build
    under test opens its own, so nothing here may hold the file open."""
    con = duckdb.connect(DB)
    try:
        return con.execute(sql, params or []).fetchall()
    finally:
        con.close()


def build() -> None:
    """Run the patched restatement, the way the DAG's `build` task does."""
    from projects.finance.lib import comp_history

    comp_history.build_year(FISCAL_YEAR, TABLE)


@pytest.fixture(scope="session", autouse=True)
def year_built():
    """Build the year once; every test below reads what it wrote."""
    build()


def published(week_start: str) -> list[tuple]:
    return query(
        f"SELECT market_code, takings_cents_ly, store_count_ly FROM {MART} "
        "WHERE week_start = ? ORDER BY market_code",
        [week_start],
    )


def test_a_store_has_one_market_however_many_versions_it_has():
    """The fixture's own guard, and the reason the SCD2 fan-out is gradeable at
    market grain: a store keeps its market across every version it carries, so
    taking the current row and taking the row that was valid in FY2023 give the
    same answer, and only failing to take the store once is wrong."""
    split = query(f"""
        SELECT s.store_id
        FROM {STORES} s
        WHERE s.store_id IN (SELECT DISTINCT store_id FROM {BOOK})
        GROUP BY s.store_id
        HAVING count(DISTINCT s.market_code) > 1
    """)
    assert not split, f"FAIL: stores with more than one market: {split[:5]}"


def test_the_graded_weeks_can_tell_the_readings_apart():
    """Each graded week has to separate the calendar's answer from both of the
    shipped job's, or it proves nothing."""
    for week in GRADED:
        truth = query(TRUTH, [week])
        assert truth, f"FAIL: {week}: the book holds nothing for the comparable week"
        assert query(SAME_WEEK, [week]) != truth, (
            f"FAIL: {week}: the same-week-number reading already agrees with the "
            "calendar, so this week cannot grade the mapping"
        )
        assert query(FANNED_OUT, [week]) != truth, (
            f"FAIL: {week}: the fanned-out reading already agrees with the "
            "calendar, so this week cannot grade the store join"
        )


@pytest.mark.parametrize("week", GRADED)
def test_a_graded_week_matches_the_store_day_book(week):
    """The week's takings by market, against the book and the calendar."""
    expected = query(TRUTH, [week])
    actual = published(week)
    assert actual == expected, (
        f"FAIL: {week}: the published week is {actual}, and the store day book "
        f"says {expected}"
    )


def test_every_week_compares_to_the_week_the_calendar_names():
    """The whole year's mapping, not just the graded weeks. FY2023 held 53
    weeks, so FY2024's 52 weeks cover FY2023 weeks 2 to 53."""
    expected = {
        (row[0], row[1], row[2], row[3]) for row in query(MAPPING)
    }
    actual = {
        tuple(row)
        for row in query(
            f"SELECT DISTINCT week_start, fiscal_week, comp_week_ly, "
            f"comp_week_start_ly FROM {MART} WHERE fiscal_year = ?",
            [FISCAL_YEAR],
        )
    }
    wrong = sorted(actual - expected)[:3]
    missing = sorted(expected - actual)[:3]
    assert actual == expected, (
        f"FAIL: the year maps {len(actual)} weeks and {len(wrong)} of them "
        f"disagree with the calendar: published {wrong}, calendar says {missing}"
    )


def test_the_year_ties_to_the_store_day_book():
    """The prior-year column, summed over the year, is the book's own takings
    for the days the comparison covers — no store twice, no week missed."""
    expected = query(YEAR_TOTAL)[0][0]
    total = query(
        f"SELECT coalesce(sum(takings_cents_ly), 0) FROM {MART} WHERE fiscal_year = ?",
        [FISCAL_YEAR],
    )[0][0]
    assert total == expected, (
        f"FAIL: the year published {total} cents against the book's {expected}"
    )


def test_every_market_lands_a_row():
    """Five markets traded through FY2023 and every one of them is in the book.
    A market dropped to make a total tie is not a fix."""
    markets = {row[0] for row in published(W23)}
    expected = {row[0] for row in query(TRUTH, [W23])}
    assert markets == expected, f"FAIL: {W23}: published {sorted(markets)}"


def test_rebuilding_the_year_leaves_one_copy():
    """A restatement gets run more than once. The second run must not double
    the year or lose a week of it."""
    before = query(f"SELECT * FROM {MART} ORDER BY week_start, market_code")
    build()
    after = query(f"SELECT * FROM {MART} ORDER BY week_start, market_code")
    assert after == before, (
        f"FAIL: the second run changed the year: {len(before)} rows became {len(after)}"
    )
