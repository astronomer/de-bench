# Memo: freight cost in the finance workbook, FY2026

To: analytics engineering, supply-chain analytics
From: Dana Whitfield, finance
Date: 2026-02-27

The logistics cost workbook changed shape at the fiscal year boundary and the change was not announced. Two people have now built a year-on-year freight comparison off it and got two different answers. This memo is what happened and what it means for anything that reads the workbook.

## What changed

Until FY2026 the workbook's `freight_cost` column was the carrier's charge including the fuel surcharge. From FY2026 the surcharge was pulled out into its own column, `fuel_surcharge`, so that supply could negotiate on the base rate. Nobody changed the name of `freight_cost`, and the workbook is maintained by hand, so there is no schema change to notice.

## MEMO-1 What the column means

`freight_cost` in the finance workbook is gross of the fuel surcharge before the FY2026 boundary and net of it after. Where the workbook and `docs/semantic-definitions.md` disagree about what a freight figure includes, this memo governs the workbook's column from the FY2026 boundary onward; the definitions govern the warehouse columns and are unaffected.

## MEMO-2 Prior periods are not restated

Prior periods are not restated. A year-on-year cost comparison across the boundary compares two different measures unless the reader adjusts, and the size of the gap is the fuel surcharge — between 9% and 19% of the base charge over the range. Add `fuel_surcharge` back to `freight_cost` for FY2026 rows to compare like with like, or compare base rates on both sides and say so.

## What this does not change

The warehouse is unaffected. `marts.fct_shipping_costs` rates packages from the carrier rate cards under `docs/rate-policy.md`, fuel folded into `rated_cents` the way the cards price it — the split the workbook wants has to come from the surcharge file, not the mart. This is a workbook problem, and it reaches the warehouse only through `sc_freight_accrual_workbook` and `fin_accrual_freight`.

## What we are doing about it

Nothing, for FY2026. Restating the workbook means rekeying eleven months of hand-entered rows and the value is not there. From FY2027 the workbook will be replaced by an extract from the mart, which is what should have happened in the first place.

Questions to me or to supply-chain analytics.
