# The dbt project: the answer key

Never ships. It sits beside `workspace/`, not inside it. `workspace/dbt/` is
what the agent gets and it says nothing that this file says.

## The twelve failing tests

`plat_dbt_test_nightly` runs the selection and twelve tests fail on the shipped
tree. Nine are **stale** — they encode a rule the April 2026 quality-contract
revision replaced, and `docs/memos/2026-04-quality-contract-revision.md` QC-3
says a test written before that memo is stale until it has been re-derived from
QC-1. Three are **real** and name a genuine disagreement between two sources.

DQ-1 grades whether the agent separates the nine from the three. Muting the
selection, widening a threshold or deleting a test is the wrong answer to all
twelve.

### The four on inventory and WMS staging — all stale

| Test | Rows | Why it fails | Memo clause |
|---|---:|---|---|
| `relationships_stg_inventory__snapshots_sku__sku__ref_dim_product_` | 115,240 | The inventory feed spells a SKU as the merchandising code (`BLD-1024-CLA-4PK-19024`); the catalog spells it `SKU-#####`. No row joins. | QC-2 row 4: a relationships test from staging to a dimension is retired and re-placed at `int_`. |
| `relationships_stg_inventory__wms_movements_sku__sku__ref_dim_product_` | 60,340 | Same two spellings, same total miss. | QC-2 row 4. |
| `not_null_stg_inventory__snapshots_unit_cost_cents` | 34,120 | The WMS stopped sending a unit cost at the FY2026 valuation change and started sending `cost_complement_bps` instead. | QC-2 row 8, which names this column outright. |
| `not_null_stg_inventory__wms_movements_from_location` | 15,237 | A receipt has no origin and an adjustment has neither end. The shape of the movement decides which location columns it carries. | QC-2 row 1: not-null on a staging column the source may omit. |

The right fix for the first two is to move the assertion to `int_`, where
`int_sku_crosswalk` in `models/supply/intermediate/` already states how far the
two spellings can be reconciled — about eight per cent of rows — and where the
honest test is on the crosswalk's own coverage rather than on total integrity.
The right fix for the last two is to delete them: the mart tests what it needs.

### The three on gift-card staging — all stale

| Test | Rows | Why it fails | Memo clause |
|---|---:|---|---|
| `accepted_values_stg_sales__gift_cards_status__active__redeemed__void` | 581 | The list was written when a card was `active`, `redeemed` or `void`. The issuer added `expired`. | QC-2 row 2: a new source status is not a defect. |
| `accepted_values_stg_sales__gift_card_ledger_entry_type__issue__redeem__reload__adjust` | 797 | Same shape from the other side: `breakage` is an entry type the list does not name, and `reload` and `adjust` are types the feed has never sent. | QC-2 row 2. |
| `not_null_stg_sales__gift_card_ledger_order_id` | 2,261 | Only a redemption always carries an order. A breakage never does — nobody was there — and a card handed out at a store event was never sold. | QC-2 row 1. |

The right fix is to move the two accepted-values lists to the mart, from the
contract, and to delete the not-null.

### The five commerce mart ties — three real, two stale

| Test | Rows | Real or stale | Why |
|---|---:|---|---|
| `tie_channel_daily_store_pos` | 64,409 store-days | **real** | The order book and the till disagree about store sales. The model header names three causes and only one of them carries money — see below. |
| `tie_settlement_processor_overlap` | 9,111 orders | **real** | Meridian and Halcyon both settled the same payments. Inside the 2025-07-01 to 2025-09-30 window that is the shadow quarter and expected; outside it, it is money counted twice. The test does not distinguish them and that is the finding. |
| `tie_gmv_settlement_reconstructs_order` | 17,199 orders | **real** | The settlement lines do not add back up to the marketplace order's `gmv_cents`. Every settled order fails, so the take rate published in `settlement_weekly` divides a numerator by a denominator from a different feed. |
| `tie_order_economics_promo_allocation` | 50,890 orders | **stale** | Asserts that `order_discount_cents` equals the sum of every promotion applied. True when every promotion was order-level; line promotions arrived in FY2025 and land inside `line_total_cents`, so they are on one side of the comparison and not the other. **No contract states this rule** — QC-1 says a mart test not traceable to a contract clause is a test of somebody's assumption. |
| `tie_channel_mix_daily_channel_coverage` | 862 days | **stale** | Asserts that store, web and marketplace sum to the day's booked total. There are four channels: `trade` was added when the trade book moved onto the order spine and is about six per cent of orders and a third of the money. **No contract states this rule either**, and `contracts/order_economics.yml` still pins a three-value channel list — the same gap seen from the other side. |

One correction to the overlap row, measured while DQ-93
(`task-copperline-c2b7f1`) was written: "outside the window" is true of the
order's date and of nothing else. Read by the date each settlement row carries,
every dual-fed payment falls inside the quarter. What the test cannot see is
sharper than a tail of stragglers: `raw.pay_processor_windows` moves the
authority from Halcyon to Meridian on 2025-09-01, in the middle of the quarter,
and the tie's own header assumes the quarter's end. The row counts and the rest
of what that seam holds are in `PLANTED.md`.

Neither stale mart tie should be widened. `tie_order_economics_promo_allocation`
should be rewritten against the header promotions only, which ties exactly.
`tie_channel_mix_daily_channel_coverage` should be deleted and the contract amended
under `docs/change-management.md`; the amendment is the work, not the test.

### What `tie_channel_daily_store_pos` is really made of

The header on `agg_daily_store_sales` gives three causes and is two thirds
wrong about its own model. The tie reports `till_net_cents - (booked_cents -
order_discount_cents)`; `raw.pos_sales_header.net_cents` is `gross - discount +
tax` and `booked_cents` is `subtotal_cents`, which is before tax. So the two
sides are not the same quantity. Over the whole range at the full profile, on
the 213,408 store-days that carry both sides, the gap is +7,389,678,723 cents
and it is exactly three things:

| part | cents | in the header? |
|---|---:|---|
| tax on the transactions the till kept | +8,390,741,710 | no |
| voided transactions the till drops and the order book keeps | -1,596,054,634 | yes |
| `is_test` and `deleted_at` orders `stg_sales__orders` drops | +594,991,647 | no |

The header's other two causes put in nothing. `business_date` equals
`raw.orders.local_order_date` on every till transaction that carries an order
id, so the 23:05 trading day moves nothing here — it moves plenty in STO-419,
which converts a stamp, and this tie converts none. And neither side reads
`event_time_utc`, so the E6 seam never enters the arithmetic. DQ-96
(`task-copperline-d4f60b`) grades the measurement; `worlds/copperline/PLANTED.md`
carries the derivation and the store-day counts.

The row count above is the small profile, which is what the verification at the
foot of this file ran. At the full profile the tie fails on 213,232 store-days
of 213,408 — every night the estate has.

## Where the tree departs from `04-transformations.md`

The chapter is the design. The extract layer in `03-extracts.md` lands about
fifty tables and does not land the upstream schemas the chapter assumed, so
several models named there have nothing to build from. Every departure below is
deliberate. `03-extracts.md` §19 already says purchasing and supplier catalogues
stay upstream with no feed.

### Cut for want of a source

| Named in ch. 04 | Why it is not built |
|---|---|
| `int_po_receipt_matched` | No purchase-order, goods-receipt or supplier-master extract. The three-way match has no three sides. |
| `fct_purchase_order_lines`, `fct_goods_receipts`, `fct_supplier_performance`, `agg_supplier_spend`, `marts.supplier_scorecard_weekly` | Same. Supply's procurement half has no feed behind it. |
| `fct_gl_lines`, `snap_trial_balance`, `fct_ap_invoices` | S5 lands a 74-row monthly account rollup, not a journal-line subledger, and there is no accounts-payable feed at all. |
| `int_price_compared`, `marts.price_index_daily` | No competitor price feed. |
| `marts.market_pricing` | `raw.market_config` is a rule-7 hand-authored file and is not in the built tree yet. |
| `agg_search_effectiveness` | The clickstream has no search event. Five event names ship and none of them is a search. |
| `marts.experiment_readout` | No experiment assignment feed. |
| `fct_loyalty_transactions` | No loyalty points ledger. `orders.loyalty_id` is the only trace of the programme, so `marts.loyalty_daily` reports what members bought and nothing about points. |
| `marts.consent_daily` | No consent feed. Consent is derived from email unsubscribes inside `marts.audience_segments`, and the derivation is stated on the row as `consent_source`. |
| `marts.nps_weekly` | No NPS survey. `support_tickets.csat` is a satisfaction score, not a net promoter score, and it is reported in `marts.support_sla_daily` under its own name. |
| `int_rate_applied` | `raw.carrier_rate_cards` is rule-7 hand-authored and not built. Freight cost comes from what the carrier billed on the package, and `fct_freight_billed` (renamed from `fct_shipping_costs` to end the name fight with the DAG mart) says it cannot check the bill. |
| `agg_channel_funnel_daily` | Duplicated `marts.funnel_daily`. One of the two was going to be read and it was not worth two models to find out which. |

### Built differently from the chapter

| Chapter | Built | Why |
|---|---|---|
| `int_gl_postings_unified` reads the ERP journal | It normalises the **five subledgers that exist** — AR invoices, credit memos, gift cards, marketplace settlements, card payments — to debit and credit | There is no journal feed. The subledgers are real and the load-bearing rule survives: finance reads this model and so does commerce's reconciliation. |
| `fct_gl_lines` | `fct_gl_postings` | It is not the ERP's journal and naming it `gl_lines` would say it was. |
| `int_customer_resolved` is customer-local | Promoted to `models/shared/intermediate/` | Finance's invoice book carries the same three id shapes and was resolving them with its own copy of the rule. Rule 4, applied exactly. |
| `marts.dispute_daily` under finance | Under commerce, per `docs/lineage.md` | It reads `marts.settlement_weekly`. In finance that is a rule-2 crossing; in commerce it is rule 3. The shipped lineage document already places it there. |
| `int_sku_crosswalk` (not in the chapter) | Added, supply-local | The two SKU spellings have to be reconciled somewhere and only supply crosses them. If a second team ever does, it moves to `int`. |
| `fct_inventory_valuation` (not in the chapter's supply list) | Added | `docs/lineage.md` names `models/supply/fct_inventory_valuation.sql` as a reader of `marts.inventory_position`, and the FY2026 valuation change needs somewhere to be applied. |

### Departures that are the point

Two rule-2 crossings are planted and both are named in the model headers and in
`dbt/README.md`:

- `models/finance/category_margin.sql` reads commerce's `marts.order_economics`.
- `models/growth/channel_roi_daily.sql` reads commerce's `marts.gmv_daily`.

Both appear as `ref` rows in the shipped `docs/lineage.md`, so they are
discoverable from the world rather than only from the graph. The repo-side test
`tests/test_copperline_dbt.py` allows exactly these two and fails on a third.

Two rule-5 departures are the chapter's own: `dim_product` is built by commerce
from its snapshot pair, `dim_customer` by the customer team, both under a
platform contract. Both say so in their headers.

`contracts/order_economics.yml` pins a three-value channel list and the order
book has four. That gap is live: `marts.order_economics` publishes all four
channels and `plat_contracts_enforce` will keep reporting it. There is no dbt
`accepted_values` test on that column, deliberately — adding one would make a
thirteenth failure and the contract is enforced by the DAG, not by dbt.

## Model counts

| Layer | Owner | Models |
|---|---|---:|
| `stg` | platform | 46 |
| `int` shared | platform | 11 |
| conformed dims | platform | 11 |
| ops | platform | 2 |
| **platform total** | | **70** |
| commerce | | 17 (+2 snapshots) |
| growth | | 9 |
| supply | | 16 |
| finance | | 17 |
| customer | | 12 |
| **total** | | **141** |

`05-platform.md` budgets 70 for platform and 146 in total. Platform is exact.
The five teams come to 71 against a budget of 76, and the five short are the
models above that have no source. The chapter says counts are a ceiling and that
a model not worth reading should be cut; these were cut.

Declared and not built, as the chapter asks: `dim_employee`, `int_labor_hours`,
`int_employees_scd` (no HR extract), `marts.fill_rate_daily` (A-4),
`marts.dim_location` (A-3), `marts.revenue_recognized_daily` and
`marts.revenue_recognized_monthly` (FIN-311), `marts.budget_variance_weekly`
(no budget extract).

Plus `dbt/northwave_reporting/`: 41 models — 10 staging, 11 marts, 20 under
`models/legacy/`. Nothing in the tree says which three still run nightly. The
directory name is not the answer: the live three are ordinary-looking marts, and
`models/legacy/` is where the previous team put things it had stopped caring
about rather than things that had stopped running.

## The test suite

625 tests, none of them skipped, placed by the layer of the model they sit on.

| Layer | Tests |
|---|---:|
| `stg` | 247 |
| `int` | 63 |
| conformed dims | 65 |
| ops | 7 |
| marts | 243 |
| **total** | **625** |

| Shape | Count |
|---|---:|
| `not_null` | 386 |
| `unique` | 84 |
| `relationships` | 47 |
| `dbt_utils.unique_combination_of_columns` | 45 |
| `accepted_values` | 39 |
| singular reconciliation ties | 16 |
| `dbt_utils.equal_rowcount` / `fewer_rows_than` | 5 |
| `dbt_utils.accepted_range` | 3 |

`05-platform.md` §3.2 estimates about two hundred. The built suite is three
times that and the estimate is the thing that was wrong, not the suite: every
test is one of the four shapes the placement table in `04-transformations.md`
names, and the bulk is `not_null` on the columns each source actually
guarantees, which is exactly what the April 2026 memo's QC-1 asks a staging
model to test. Nothing here was added for coverage's sake and nothing was cut to
hit a number.

Sixteen singular tests carry the reconciliation ties: eight on commerce, three
on finance, two on supply, two on customer and one on growth. Five of the
commerce eight fail — three real, two stale — and the other eleven hold. The
eleven that pass are worth as much as the five that do not: a tie nobody wrote
is a tie nobody will notice breaking.

## Rule 6, confirmed

Every published money column is `*_cents` and integer. The checks:

- No model or test file contains a float, double or real cast. The repo-side
  test asserts it.
- Every conversion goes through `macros/money.sql`. `to_cents`, `to_base_cents`
  and `apply_bps` all multiply first, divide once, and round half up to an
  integer — never banker's rounding, per `docs/finance-policy.md` REV-2.
- Every fact that converts carries `fx_rate_ppm` and `fx_rate_date` beside the
  converted amount: `int_orders_enriched`, `int_gl_postings_unified`,
  `fct_gl_postings`, `fct_order`, `fct_invoice_line`, `fct_ar_invoices`,
  `order_economics`, `channel_roi_daily`.
- Rates are parts per million as `bigint`, never a decimal fraction. Basis
  points are integers, so 2% is 200.
- `raw.pay_halcyon_settlements.amount` is the one text money column in the
  estate. `stg_payments__halcyon_settlements` casts it to cents with `to_cents`
  and keeps the original string beside it as `amount_text`.

## Sign conventions, which cost an afternoon

Three landed feeds sign their own amounts and three do not. Getting this wrong
does not fail — it produces a plausible number with the wrong sign.

| Feed | Signs itself |
|---|---|
| `raw.gift_card_ledger` | yes — issue positive, redeem and breakage negative |
| `raw.pay_meridian_settlements` | yes — capture positive, refund and chargeback negative |
| `raw.marketplace_settlements` | yes — commission and fee positive, principal and refund negative |
| `raw.credit_memos`, `raw.returns`, `raw.disputes` | no — magnitudes only |

The staging models for the first three expose `magnitude_cents` and
`is_money_out` and do **not** re-sign. Any model that states a direction states
it against the magnitude.

## One thing left for the platform pass

`dbt_packages/` is git-ignored, which is the dbt convention and is right for the
repo. It also means a trial tree has no dbt-utils in it until somebody runs
`dbt deps`, and `dbt deps` wants the network. Whoever wires the trial setup has
to pick one: vendor `dbt_packages/` into the shipped tar, or run `dbt deps` at
setup from a local package cache. Nothing in this project depends on which.

`tests/test_copperline_dbt.py` skips its parse checks when `dbt_packages/` is
absent, so it is green either way and says why it skipped.

## Verification

Against the small profile, `WORLD_TODAY=2026-06-15`:

```
dbt run   →  141 models, 0 errors
dbt test  →  625 tests, 613 pass, 12 fail, 0 skipped
```

The twelve are exactly the list above. `dbt build` is not the check: it
interleaves tests with models and skips everything downstream of a failing test,
so the four inventory failures take the supply half of the warehouse with them.
`dbt/README.md` says so in the world's own voice.
