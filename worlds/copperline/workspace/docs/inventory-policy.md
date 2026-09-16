# Inventory valuation policy

Maintained by finance-eng with merchandising finance. Last reviewed 2026-02-18, three weeks after the method change took effect. Source tables: `raw.inventory_snapshots`, `raw.dept_cost_complement`.

Copperline changed how it values inventory at the start of FY2026. The data shows that something changed — columns that used to be populated are NULL, and columns that used to be NULL are populated — but the data does not say what the new rule is. This document says it. There is nowhere else it is written down.

## INV-1 The method, and the date

From 2026-02-01, the first day of FY2026, Copperline values inventory under the retail inventory method. Before that date it valued inventory at weighted-average cost. The change is effective on that date and applies to every department in every market.

The data cannot supply this rule. `raw.inventory_snapshots` shows `unit_cost_cents` going NULL and `retail_value_cents` and `cost_complement_bps` starting to populate, which announces a change and names nothing. This clause is the statement of what the change was.

## INV-2 The grain

Under the retail method, cost is derived at department grain, not at SKU grain. The cost complement is a department-level ratio and there is no per-SKU cost behind it. A model that computes margin per SKU line from FY2026 data is computing something the method does not define.

`raw.dept_cost_complement` holds one row per department per fiscal period, FY2026 onward. It does not exist for earlier periods and is not back-filled.

## INV-3 Rounding

Compute the complement at basis points, apply it per department, round half up to the cent, then sum. The complement is an integer basis-point value on the row — 6,340 means 63.40% — and it is never converted to a float. Round once, per department, after applying the complement; do not round the per-department amounts to a coarser unit and do not round a total that was summed from unrounded parts.

## INV-4 No restatement

Figures for periods before 2026-02-01 are not restated. FY2025 stays on weighted-average cost, permanently. The two methods do not agree and are not meant to: applying weighted-average cost to FY2026 understates inventory value by roughly 6 to 9% at department grain, and applying the retail method to FY2025 is not defined at all, because there is no cost complement behind those periods.

A year-on-year inventory comparison across the boundary therefore compares two measures. Say so when you publish one.

## Why the change

Merchandising finance had wanted the retail method since the acquisition. The acquired estate kept its own cost basis, the two books never merged, and reconciling weighted-average cost across two costing systems was taking most of a week every period. The retail method takes the ratio at department grain and stops asking the question. The board approved it in the FY2026 plan and it took effect with the fiscal year, on purpose, so that no fiscal year straddles the change.

## What each column means, and when

| Column | Before 2026-02-01 | From 2026-02-01 |
|---|---|---|
| `unit_cost_cents` | weighted-average cost per SKU per store per day | NULL |
| `retail_value_cents` | NULL | retail value at ticket price |
| `cost_complement_bps` | NULL | department cost-to-retail ratio, integer basis points |
| `on_hand_units` | populated | populated |
| `dept_id` | populated | populated, and load-bearing from here on |

The mart is `marts.fct_inventory_valuation`. The merch dashboards read `marts.inventory_position`, which carries units and not value, so they are unaffected by the change and people keep asking whether they are.

## Open items

- The department hierarchy changed twice in FY2025 and `raw.dept_cost_complement` uses the current one. A comparison across a department reorganization is not sound and nothing stops you making one.
- TODO: shrink is applied at period end from the physical count and is not in the complement. Merchandising finance wants it in. Nobody has costed the work.
- The acquired estate's departments were mapped onto Copperline's during the FY2026 plan. The mapping lives in a spreadsheet the merchandising finance team keeps, and the wiki page that used to hold it is gone.
