# Contract: daily flash

Schema: `contracts/comp_sales_daily.yml`. Consumer C-5 in `docs/report-registry.md`. Owner: finance. Publisher: `fin_daily_flash`. Agreed 2021-04-26, last amended 2026-02-09.

Comp sales by region and marketplace GMV, out by 06:00 every morning. The most-read number in the company.

## DF-1 Comparability follows the policy

Comp figures in the flash are computed under `docs/comp-store-policy.md` and nothing else. All four clauses apply — the thirteen-month opening test, closures restating both years, remodels over 21 days, and acquired stores dating from the acquisition close. The flash does not hold a comparability rule of its own and never has.

`comp_sales_cents` is defined in `docs/semantic-definitions.md` and means what it says there.

## DF-2 Prior year comes from the calendar

The prior-year comparable week is read from `raw.fiscal_calendar.comp_date_ly`. Never a calendar-date offset, and never a same-numbered week. `docs/retail-calendar.md` §CAL-3 and `docs/finance-policy.md` §REV-16 both state the rule; this contract binds the flash to it.

A flash figure computed from a 364-day offset is wrong for every date in a year that follows a 53-week year, and looks right.

## DF-3 Grain, columns and the day it covers

One row per region per business date for the comp figures, and one row per business date for marketplace GMV. Columns: `ds`, `region`, `comp_sales_cents`, `gmv_cents`, and the prior-year comparable of each.

The flash covers the day before yesterday, not yesterday, per `docs/late-data-policy.md` §LD-3. A flash that reports the current day is reporting an incomplete day.

## Timing

Due 06:00. It runs at 05:45 off marts the 04:00 build already landed, which is why it is fifteen minutes and not an hour. Late is worse than approximate here; the executive team read it before the market opens.

## Notes

- `gmv_cents` is the marketplace's gross order value and is not Copperline revenue. It is in the flash because the executive team asked for it, not because finance recognises it.
- Regional totals do not sum to a company total, because stores outside comp are in neither.
- A holiday in every market produces a legitimately empty day. The market calendar has those, and the publish path skips regions that are not expected to report.

## Open items

- TODO: the flash has no prior-year figure for a region that did not exist a year ago, and prints an empty cell. Finance asked for a dash. Nobody has done it.
- The 06:00 commitment is to finance and the executive team. Nobody has written down what happens when it is missed.
