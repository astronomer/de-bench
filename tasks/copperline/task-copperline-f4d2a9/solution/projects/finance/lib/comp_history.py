"""The FY2024 prior-year comparatives, built out of the store day book.

`marts.comp_sales_daily` has no prior year for FY2024. The order detail starts
on 2024-02-04, the day FY2024 opens, and FY2023 was cut back to store-day
summary under `docs/retention-policy.md` RET-2 long before anyone wanted it
again. What is left of that year is `raw.pos_sales_daily`: one row per store per
day, the whole of FY2023 and nothing either side of it.

One row per fiscal week per market. Takings are net of discount and before tax
on both sides of the book, which is `gross_cents - discount_cents` here.

**Which FY2023 week a FY2024 week compares to is a lookup, not a count.**
FY2023 held 53 weeks, so FY2024 week W compares to FY2023 week W+1 and the
year's 52 weeks cover FY2023 weeks 2 to 53. `raw.fiscal_calendar.comp_date_ly`
holds the comparable day for every date and this reads it: joining the calendar
to itself on the week number lines FY2024 up against FY2023 weeks 1 to 52
instead, which is a week out on every row and drops the 53rd week from the
comparison altogether. `docs/retail-calendar.md` CAL-3 and
`docs/finance-policy.md` REV-16 both say so.

**A store is one store, however many rows it carries.** `raw.stores` is SCD2 and
twenty-one of the stores in the book have a prior version, so a join that does
not take the store once counts those stores twice and the year adds up to more
than FY2023 took. `docs/comp-store-policy.md` says it in a line: comparability
is a property of the store, not of the row. The market is the same on every
version of a store, so it does not matter which one you take, only that you take
one.

Every store in the book is comparable for FY2024 anyway: all of them traded the
whole of FY2023, the stores opened inside the fixture range are short of CMP-1
for the whole year, and the Northwave estate dates from the acquisition close
under CMP-4.

Owned by finance-analytics. FIN-431.
"""

from __future__ import annotations

from include.lib import warehouse

__all__ = [
    "FISCAL_YEAR",
    "TABLE",
    "COLUMNS",
    "ensure_table",
    "build_year",
    "assert_year_complete",
]

#: The year being restated, and the table it lands in.
FISCAL_YEAR = "FY2024"
TABLE = "marts.comp_prior_year_weekly"

BOOK = "raw.pos_sales_daily"
CALENDAR = "raw.fiscal_calendar"
STORES = "raw.stores"

#: The published shape. `week_start` is the partition: the Sunday that opens the
#: FY2024 week, so a rerun of the year replaces it week by week.
COLUMNS = [
    "week_start",
    "fiscal_year",
    "fiscal_week",
    "comp_week_ly",
    "comp_week_start_ly",
    "market_code",
    "takings_cents_ly",
    "store_count_ly",
]


def ensure_table(table: str, con) -> None:
    """The mart, if nothing has made it yet. Nothing else writes this table."""
    qualified = warehouse.qualify(table)
    schema = qualified.rpartition(".")[0]
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {qualified} (
            week_start DATE NOT NULL,
            fiscal_year VARCHAR NOT NULL,
            fiscal_week INTEGER NOT NULL,
            comp_week_ly INTEGER NOT NULL,
            comp_week_start_ly DATE NOT NULL,
            market_code VARCHAR NOT NULL,
            takings_cents_ly BIGINT NOT NULL,
            store_count_ly INTEGER NOT NULL
        )
    """)


#: One FY2024 week. Each of its seven dates carries the day it compares to, and
#: those seven days are one FY2023 week: the comparison is read off the calendar
#: rather than counted back through the week numbers.
WEEK_SQL = f"""
WITH days AS (
    SELECT cal_date, week_start, fiscal_year, fiscal_week, comp_date_ly
    FROM {{calendar}}
    WHERE fiscal_year = ?
      AND week_start = ?
      AND comp_date_ly IS NOT NULL
),
estate AS (
    SELECT store_id, market_code
    FROM {{stores}}
    WHERE is_current
)
SELECT d.week_start,
       d.fiscal_year,
       d.fiscal_week,
       ly.fiscal_week                               AS comp_week_ly,
       ly.week_start                                AS comp_week_start_ly,
       e.market_code,
       sum(b.gross_cents - b.discount_cents)::BIGINT AS takings_cents_ly,
       count(DISTINCT b.store_id)::INTEGER          AS store_count_ly
FROM days d
JOIN {{book}} b ON b.business_date = d.comp_date_ly
JOIN {{calendar}} ly ON ly.cal_date = d.comp_date_ly
JOIN estate e ON e.store_id = b.store_id
GROUP BY 1, 2, 3, 4, 5, 6
ORDER BY 6
"""


def build_year(fiscal_year: str, table: str) -> int:
    """Rebuild every week of `fiscal_year` in `table`. Returns the rows written.

    A week at a time, delete-insert on `week_start`, so running the job twice
    leaves one copy of the year and a single week can be replaced on its own.
    """
    sql = WEEK_SQL.format(
        calendar=warehouse.qualify(CALENDAR),
        stores=warehouse.qualify(STORES),
        book=warehouse.qualify(BOOK),
    )
    written = 0
    with warehouse.connect() as con:
        ensure_table(table, con)
        weeks = [
            row[0]
            for row in con.execute(
                f"SELECT DISTINCT week_start FROM {warehouse.qualify(CALENDAR)} "
                "WHERE fiscal_year = ? ORDER BY 1",
                [fiscal_year],
            ).fetchall()
        ]
        for week_start in weeks:
            rows = con.execute(sql, [fiscal_year, week_start]).fetchall()
            written += warehouse.delete_insert(
                table, "week_start", week_start, rows, columns=COLUMNS, con=con
            )
    return written


def assert_year_complete(fiscal_year: str, table: str) -> int:
    """Every fiscal week of the year landed a row. Returns the weeks found.

    A week with nothing behind it is the failure that reads as a flat year
    rather than as a missing one, so it is worth saying out loud before the
    board pack picks the table up.
    """
    with warehouse.connect(read_only=True) as con:
        expected = {
            row[0]
            for row in con.execute(
                f"SELECT DISTINCT week_start FROM {warehouse.qualify(CALENDAR)} "
                "WHERE fiscal_year = ?",
                [fiscal_year],
            ).fetchall()
        }
        found = {
            row[0]
            for row in con.execute(
                f"SELECT DISTINCT week_start FROM {warehouse.qualify(table)} "
                "WHERE fiscal_year = ?",
                [fiscal_year],
            ).fetchall()
        }
    missing = sorted(expected - found)
    if missing:
        raise ValueError(
            f"{table}: no {fiscal_year} rows for "
            + ", ".join(str(day) for day in missing[:5])
        )
    return len(found)
