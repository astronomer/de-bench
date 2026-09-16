# Semantic definitions

Maintained by the data platform team, with each definition owned by the consumer that asked for it. Last reviewed 2026-05-04.

Every revenue-like column in the warehouse, named once and defined once. The list exists because "revenue" was three different numbers in three different dashboards for most of FY2024, and every one of them was defensible on its own.

A name in this table is reserved. The definition is the whole meaning of the name; the consumer column says who asked for it and is the person to ask before it changes.

## The reserved names

| Column | Grain | Definition | Consumer |
|---|---|---|---|
| `gmv_cents` | order | full marketplace order value, sellers included | daily flash |
| `net_sales_cents` | order line | gross less REV-13 discounts and returns | merch dashboards |
| `booked_cents` | order | order value on the order date, gross, no recognition | daily flash |
| `recognized_cents` | invoice line day | finance-policy REV-1..REV-11 applied | finance close |
| `reported_cents` | fiscal month | recognized, net of refunds, excluding legacy-era plans | board pack |
| `commission_cents` | marketplace order | Copperline's take under REV-14 | daily flash, finance close |
| `comp_sales_cents` | store fiscal week | net sales for stores comparable under `docs/comp-store-policy.md` | daily flash |
| `merch_margin_cents` | order line | net sales less landed cost | merch dashboards |
| `demand_units` | SKU day | units ordered, returns not deducted | replenishment feed |

## SD-1 Presentation rounding

Percentages are rounded half up to one decimal place. That is a presentation rule and applies to what a report prints, never to a stored value and never to an intermediate step. Money is never rounded to anything but a cent; `docs/finance-policy.md` §REV-2 owns that.

## SD-2 One name, one meaning

These names are reserved. A model that computes something else must not use one of these names, and a consumer that wants something else must define it in its own contract. Three columns look alike and are not: `booked_cents` counts on the order date and never moves; `recognized_cents` follows the recognition schedule; `reported_cents` is `recognized_cents` net of refunds with legacy-era plans removed, because the board pack contract excludes them.

## The three that get confused

The three money columns in SD-2's last sentence account for most of the questions this document is asked. Worked on one trade agreement:

A twelve-month trade program invoiced 1,200,000 cents on 2026-03-02, with one 50,000-cent refund on 2026-04-19.

- `booked_cents` is 1,200,000 on 2026-03-02 and nothing on any other day. It never moves again, including for the refund.
- `recognized_cents` is about 3,287 cents a day across the term, per REV-1 and REV-2, with the remainder on the final day. April's total is the sum of April's daily rows.
- `reported_cents` for FY2026 P2 is April's recognized total less the 50,000-cent refund, and would be zero for this agreement if it were a legacy-era plan, because the board pack excludes those.

Three right numbers, three different questions. The name says which question.

## Where each name lives

| Column | Model |
|---|---|
| `gmv_cents` | `marts.gmv_daily` |
| `net_sales_cents` | `marts.order_economics`, `marts.sell_through_daily` |
| `booked_cents` | `marts.order_economics` |
| `recognized_cents` | `marts.revenue_recognized_daily`, `marts.revenue_recognized_monthly` |
| `reported_cents` | `marts.account_rollup` |
| `commission_cents` | `marts.settlement_weekly`, `marts.gmv_daily` |
| `comp_sales_cents` | `marts.comp_sales_daily` |
| `merch_margin_cents` | `marts.category_margin`, `marts.order_economics` |
| `demand_units` | `marts.inventory_position` |

A name appearing in two models means the same thing in both. If it does not, one of them is a defect.

## Grain, and why it is in the table

The grain column is part of the definition, not a note about it. `net_sales_cents` is an order-line number: summing it to order grain is fine, and a model that computes it at order grain and calls it the same thing is computing something else, because the discount stacking in REV-13 applies per line and floors per line.

The same is true in the other direction. `booked_cents` is an order number and there is no line-level version of it. A model that splits it across lines has invented an allocation nobody agreed.

When a mart changes grain, every reserved name on it changes meaning. That is why `docs/change-management.md` §CM-2 asks for an amendment per consumer rather than a note in the pull request, and why `docs/lineage.md` is the list to work before the change rather than after.

## Names that were retired

- `revenue_cents` — meant three things and now means nothing. Removed FY2025 P4. Anything still using it is old.
- `gross_sales_cents` — never had a discount rule behind it. Use `net_sales_cents` and say what was netted.
- `units` — ambiguous between ordered, shipped and on hand. `demand_units` is ordered; the other two are named in the supply models.

## Adding a name

Definitions name; models compute. A model that computes something these definitions do not cover needs a new name here before it ships, not after. Where a model computes something different under one of these names, the model is wrong and is renamed — the definition does not bend to fit it. Raise it through `docs/change-management.md`; a name more than one consumer reads needs each of their owners to agree.

## Open items

- `merch_margin_cents` says "landed cost" and landed cost changed meaning at the FY2026 valuation change. `docs/inventory-policy.md` has the method; this row has not been rewritten to match. TODO.
- Nobody owns `demand_units` on the returns question. It says returns are not deducted, which is what supply wants and not what merchandising assumes.
- There is no reserved name for tax-exclusive sales. The tax team keeps its own columns and nobody has asked to conform them.
