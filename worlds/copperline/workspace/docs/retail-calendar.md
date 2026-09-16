# The retail calendar

Maintained by finance-eng. Last reviewed 2026-02-09, at the FY2026 year start. Shipped table: `raw.fiscal_calendar`.

Copperline reports on a retail calendar, not on calendar months. Every fiscal figure in the warehouse — periods, quarters, weeks, year-on-year comparisons — comes from the shipped calendar table. Nothing derives a fiscal date with arithmetic.

## CAL-1 The year-end rule and the pattern

The fiscal year ends on the Saturday nearest 31 January. A fiscal quarter is 13 weeks in the 4-5-4 pattern: a four-week period, a five-week period, a four-week period. A fiscal week runs Sunday through Saturday. A fiscal period is a fiscal month of four or five weeks and is written `P1` to `P12`.

## CAL-2 The shipped calendar

`raw.fiscal_calendar` holds one row per calendar date from 2022-01-30 to 2027-01-30. Columns: `cal_date`, `fiscal_year`, `fiscal_quarter`, `fiscal_period`, `fiscal_week`, `week_start`, `week_end`, `day_of_fiscal_week`, `comp_date_ly`, `comp_week_ly`, `is_53rd_week`. It is the calendar. A model that needs a fiscal attribute joins to it, through `dim_date`, and does not compute one.

## CAL-3 The year after a 53-week year

In a year that follows a 53-week year, the prior-year comparable is the restated week, not the same-numbered week and not a date offset. `comp_date_ly` holds the answer. `ds - interval '364 days'` is right in ordinary years and wrong for every date in the year after a 53-week year.

## The years in the table

| Fiscal year | Starts | Ends | Weeks |
|---|---|---|---|
| FY2023 | 2023-01-29 | 2024-02-03 | **53** |
| FY2024 | 2024-02-04 | 2025-02-01 | 52 |
| FY2025 | 2025-02-02 | 2026-01-31 | 52 |
| FY2026 | 2026-02-01 | 2027-01-30 | 52 |

FY2023 is the 53-week year. Under the Saturday-nearest rule a fifty-third week falls roughly every five or six years; the next one is FY2028.

## What the extra week does to a comparison

FY2024 week 1 does not compare to FY2023 week 1. It compares to FY2023 week 2, because FY2023 held an extra week at the front of the comparison. The same shift runs the whole of FY2024: week W compares to week W+1 of the year before.

This is the one place where the obvious answer and the right answer differ by a whole week across a whole year, and where the obvious answer looks fine. A 364-day offset lands on the correct weekday, returns a full week of data, and produces a comp figure that is plausible and wrong. `comp_date_ly` and `comp_week_ly` are the answer, and they are on every row of the calendar table.

`docs/finance-policy.md` §REV-16 states the same rule from the finance side and points at the same column. The two say the same thing on purpose; they do not disagree.

## Fiscal detail nobody remembers

- Fiscal periods are not calendar months and never line up with one. P1 of FY2026 runs 2026-02-01 to 2026-02-28, which is a coincidence and not a rule.
- `is_53rd_week` is true on seven rows in the whole table, all in FY2023.
- The pre-2024 rows exist so that FY2023 has a calendar behind it. Detail-grain facts do not go back that far; the store-day summary does.
- Quarters are 13 weeks, so a quarter is 91 days except in the 53-week year, where Q4 is 98.

## Open items

- The table stops at 2027-01-30. TODO: extend it before the FY2027 close, or every year-end date arithmetic starts returning NULL and nothing will fail loudly.
- Nobody has decided how the second-wave markets report their first partial period. It has not mattered yet because they are in no mart.
