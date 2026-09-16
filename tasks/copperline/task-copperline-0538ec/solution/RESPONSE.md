# DQ-77 — the twelve nightly failures, split

Nine of the twelve are stale. They assert a rule the quality-contract revision of 2026-04-13
replaced, and QC-3 says a test written before that memo is stale until it has been re-derived
from QC-1. Those nine are dealt with below.

Three are real. Two sources disagree about the same money in each of them and the test is right
to say so. They are still failing this morning and they will fail tonight. None of them is
fixed here.

The nightly still runs all six team selections and the project still runs its tests at
`severity: error`.

## The three real ones

**`tie_channel_daily_store_pos`** — 64,409 store-days. The order book and the till disagree
about what a store sold. `models/commerce/agg_daily_store_sales.sql` puts the two side by side
and does not pick one: the order book is the company's record of what sold, the POS batch is
the store's record of what the till took. Three causes are tangled in the gap — a store
transaction voided after the batch closed, the store's trading day ending at 23:05 store-local
against the order's local order date, and the pre-cutover era carrying no UTC stamp to line the
two up with. Settling it means separating those three, and `marts.recon_exceptions` is where
that work already starts. It is a week of its own.

**`tie_settlement_processor_overlap`** — 9,111 orders. Meridian and Halcyon both claim the same
payment. Inside the shadow quarter, 2025-07-01 to 2025-09-30, that is the migration working as
planned. Outside it, the money is counted twice. `int_payment_matched` flags an order as
`is_shadow_quarter` when both books hold it, whatever the date, so the test cannot tell the two
apart — and that is the finding. To settle it: split the flag from the window, count what falls
outside, and take the outside rows to payments as a reconciliation.

**`tie_gmv_settlement_reconstructs_order`** — 17,199 orders. The marketplace order arrives on
one feed carrying `gmv_cents`. The settlement lines arrive on another and should add back up to
it: principal, commission, fulfilment fee. Every settled order fails, which means the take rate
published in `marts.settlement_weekly` divides a numerator from one feed by a denominator from
another. That is the one of the three with a number in front of a consumer, so it goes first.
Settling it means agreeing with the marketplace which of the two feeds states GMV.

## The nine stale ones

Four on the inventory and WMS staging models:

| Test | Clause | What was done |
|---|---|---|
| `relationships_stg_inventory__snapshots_sku__sku__ref_dim_product_` | QC-2 row 4 | moved onto `int_sku_crosswalk` |
| `relationships_stg_inventory__wms_movements_sku__sku__ref_dim_product_` | QC-2 row 4 | moved onto `int_sku_crosswalk` |
| `not_null_stg_inventory__snapshots_unit_cost_cents` | QC-2 row 8 | retired |
| `not_null_stg_inventory__wms_movements_from_location` | QC-2 row 1 | retired |

The two relationships tests never had a chance: the inventory feed spells a SKU as the
merchandising code, `BLD-1024-CLA-4PK-19024`, and the catalog spells it `SKU-#####`, so no row
joined and the test failed on all of them. `int_sku_crosswalk` is the model that matches the two
spellings, and it matches about eight per cent of them. The relationship is now tested there, on
`catalog_sku`, which is the column that carries a catalog key. `unit_cost_cents` has been empty
since the FY2026 valuation change moved the WMS to `cost_complement_bps`; QC-2 row 8 names the
column. `from_location` is empty on a receipt and on both ends of an adjustment, by the shape of
the movement.

Three on the gift-card staging models:

| Test | Clause | What was done |
|---|---|---|
| `accepted_values_stg_sales__gift_cards_status__active__redeemed__void` | QC-2 row 2 | retired |
| `accepted_values_stg_sales__gift_card_ledger_entry_type__issue__redeem__reload__adjust` | QC-2 row 2 | retired |
| `not_null_stg_sales__gift_card_ledger_order_id` | QC-2 row 1 | retired |

The issuer added `expired` to the status list and sends `breakage` as an entry type, and a new
source status is not a defect. QC-2 row 2 re-places a list like this on the mart, from a
contract — but no mart publishes a card status or an entry type and no contract pins either, so
there is nothing to re-place them onto today. When one is published, the list goes with it.
`order_id` is set on a redemption and on nothing else.

Two mart ties, and these two are the ones worth reading twice, because the memo's table does not
cover them. Both sit at the mart layer, where QC-2's rows do not reach. They are stale under the
last sentence of QC-1 instead: a mart test not traceable to a contract clause is a test of
somebody's assumption.

| Test | Clause | What was done |
|---|---|---|
| `tie_order_economics_promo_allocation` | QC-1, marts | re-derived against the header promotions |
| `tie_channel_mix_daily_channel_coverage` | QC-1, marts | retired |

`tie_order_economics_promo_allocation` compared `order_discount_cents` against every promotion
applied to the order. That held while every promotion was order-level. Line-level promotions
arrived in FY2025 and they land inside `line_total_cents`, so they were on one side of the
comparison and not on the other. `stg_sales__promo_applications.is_header_discount` is the scope
the test was missing — a row with no `order_line_id` discounted the header — and with it the two
sides tie.

`tie_channel_mix_daily_channel_coverage` asserted that store, web and marketplace add up to the
day's booked total. There are four channels: `trade` came onto the order spine with the trade
book and it is about six per cent of orders and about a third of the money. Nothing states the
three-channel rule, so there is nothing to re-derive the test against, and it is retired.

## One thing this leaves open

`contracts/order_economics.yml` still pins a three-value channel list against a mart that
publishes four, so `plat_contracts_enforce` reports it every day. That is the same gap the
retired tie saw from the other side and it is a contract amendment under
`docs/change-management.md`, owned by commerce. Raised with them; it is not a test.
