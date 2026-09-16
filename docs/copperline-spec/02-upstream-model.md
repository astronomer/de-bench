# 02 — The upstream model

## 1. What this chapter is

This is the simulated truth of Copperline Retail Group: the application databases the company runs on, in twelve schemas and seventy-nine tables. It is not the warehouse, and no agent ever sees it. It is the internal data model of the fixture generator — the thing that has to be coherent so that everything the agent *does* see can be derived from it.

Copperline does not run one system. It runs seven, and the twelve schemas below divide along those lines:

| Schema | The system it belongs to |
|---|---|
| `core` | reference and master data, held in Ironwood (the ERP) and replicated to everything else |
| `product` | Palette (the PIM) for the catalog; price lists, promotions and coupons in the commerce platform |
| `store` | the store master in Ironwood; registers and lanes in Northgate (the POS estate) |
| `customer` | Halyard (the CRM) for identity and consent; loyalty and campaign records in the commerce platform |
| `sales` | the OMS — the order spine for every channel, with POS-originated orders pushed in from Northgate |
| `ecommerce` | the web platform and the Driftwood collector behind it |
| `inventory` | Corvid (the WMS) for warehouse stock; store stock and the cost ledger in Ironwood |
| `supply_chain` | Ironwood purchasing |
| `logistics` | Corvid shipping, plus the carriers' own tracking systems mirrored back |
| `hr` | the HR and payroll system |
| `finance` | Ironwood — the general ledger, AP, AR, budgets |
| `support` | Halyard's service desk |

The generator simulates these tables day by day and then **derives the `raw.*` extracts of chapter 03 from them**. The extracts are not copies. The delivery mess is applied at the extract boundary, and it is the only place it exists:

- **Two clocks.** Every fact that lands carries `event_time` and `loaded_at`, both UTC, with a real late tail — nothing later than day five, something always on day five, per-channel splits. Upstream carries one timestamp per event, because an OLTP table does.
- **Integer minor units.** Money lands as `*_cents BIGINT`, converted at generation with Decimal ROUND_HALF_UP, with the rate stamped on the row as `fx_rate_ppm`. Upstream money is `DEC(18,4)`, because that is what an ERP holds.
- **Era-conditional formats.** The customer re-key (E2), the local close stamp (E1), the UTC flip (E6), the valuation change (E8) — none of them exist upstream. Upstream has one identifier scheme, one clock, one cost method. The eras are applied when the extract is cut, which is why an era looks like a change in the feed rather than a change in the company.
- **Feed shapes and vocabularies.** Nightly CSV, hourly JSONL, dated parquet, a paginated API stub, a hand-typed monthly file. Column names change, statuses get renamed, the same fact arrives under two spellings. That divergence is authored at the boundary.

So the upstream model may keep its own conventions and its own `today`, and nothing about them binds the world. **Every landing-layer invariant in `day-zero-invariants.md` applies at the extract boundary, not here.** Read this chapter for what is true; read chapter 03 for what lands.

**Earn-its-place applies to the extract list, not to the model.** Every table below exists because the company would have it, and the generator keeps them all so the fiction stays consistent — a store has a manager, a PO has a buyer, a payslip charges a cost center. But a schema only grows a feed when a task needs one. Today `hr` has no extract at all; `logistics.delivery_routes` and `logistics.route_stops` have none; `ecommerce.carts`, `cart_lines` and `search_queries` have none; `finance.budgets` has none. They are simulated and unread. When a task claims one, the feed gets authored then, against the built world.

Where a schema does have a feed, this is roughly where it lands:

| Schema | Lands as |
|---|---|
| `core` | the reference sources — `raw.entities`, `raw.market_config`, `raw.fx_rates`, `raw.fiscal_calendar`, `raw.market_calendar` |
| `product` | the Palette change feed — `raw.pim_product_versions`, `raw.product_categories` |
| `store` | `raw.stores`, and the nightly POS batches in `raw.pos_sales_header` |
| `customer` | the CRM extract and the acquired book — `raw.customers`, `raw.customer_id_map`, `raw.nwv_accounts` |
| `sales` | `raw.orders`, `raw.order_lines`, `raw.order_status_history`, the returns and promotion side tables, and the trade invoicing domain |
| `ecommerce` | `raw.web_events` only — sessions, page views and searches collapse into one event stream |
| `inventory` | `raw.inventory_snapshots`, `raw.wms_movements`, `raw.dept_cost_complement` |
| `supply_chain` | the supplier master and the purchase-order extract out of Ironwood |
| `logistics` | the carrier scan and invoice feeds, and the rate cards |
| `hr` | nothing |
| `finance` | `raw.finance_ledger` — the hand-typed monthly extract that is the independent oracle |
| `support` | the ticket extract, through the same paginated stub as the CRM |

## 2. The sample storyline

Every sample value in this chapter follows one thread, so that a builder can check a join by reading down the page. Customer **Priya Raman (90412)** buys three **Aurora deck cushions, slate, standard (variant 550231)** on the web (**order 8840127**) on 2026-04-14, shipped from the **Edison NJ DC (warehouse 7)**, sourced from **Lakeshore Outdoor (supplier 118)**, and returns one unit nine days later.

Every date in the samples sits inside the fixture range and before world-today, 2026-06-15. Fiscal period 202603 is FY2026 P3, 2026-04-05 to 2026-05-02, closed.

## 3. Schema map

| # | Schema | Tables | Role |
|---|---|---|---|
| 1 | `core` | 6 | reference data every other schema points at |
| 2 | `product` | 9 | what you sell, and what it should cost and price at |
| 3 | `store` | 4 | where you sell it |
| 4 | `customer` | 6 | who buys, and how you market to them |
| 5 | `sales` | 8 | the revenue spine — orders, tender, returns |
| 6 | `ecommerce` | 5 | digital behaviour upstream of the order |
| 7 | `inventory` | 8 | what stock exists, where, and why it moved |
| 8 | `supply_chain` | 6 | how stock was bought and received |
| 9 | `logistics` | 7 | how stock physically moved |
| 10 | `hr` | 8 | who works here and what labour costs |
| 11 | `finance` | 9 | how all of it lands in the ledger |
| 12 | `support` | 3 | what went wrong afterwards |
| | **Total** | **79** | |

---

## 4. `core` — 6 tables

### core.currencies
*Grain: one currency*
**Tells you:** how many decimals money in this currency carries, so amounts round and format correctly.

| Column | Type | Key | Sample |
|---|---|---|---|
| currency_code | TEXT | PK | USD |
| name | TEXT | | US Dollar |
| symbol | TEXT | | $ |
| minor_unit | INT | | 2 |

Copperline trades in USD, GBP, EUR and MXN. `minor_unit` is what the extract's cents conversion reads.

### core.countries
*Grain: one country*
**Tells you:** the country a location sits in and its default trading currency.

| Column | Type | Key | Sample |
|---|---|---|---|
| country_id | INT | PK | 1 |
| iso2 | TEXT | | US |
| iso3 | TEXT | | USA |
| name | TEXT | | United States |
| currency_code | TEXT | FK→core.currencies | USD |

Nine rows are live by world-today: US, CA, GB, IE, DE from the start, and BR, MX, PL, ID set up in FY2026 Q1. The default currency here is the country's own, which is exactly what `raw.market_config` contradicts for CA, BR, ID and PL — a market bills in the currency its legal entity bills in, not the currency of the country it sells into.

### core.exchange_rates
*Grain: currency pair · day*
**Tells you:** what a foreign-currency amount was worth on the day it happened — the only correct way to convert historical money.

| Column | Type | Key | Sample |
|---|---|---|---|
| rate_id | BIGINT | PK | 441207 |
| from_currency | TEXT | FK→core.currencies | GBP |
| to_currency | TEXT | FK→core.currencies | USD |
| rate_date | DATE | | 2026-04-14 |
| rate | DEC(18,8) | | 1.27340000 |

⚠ Applying today's rate to last year's orders is the failure mode. At the extract this table becomes `raw.fx_rates` at integer parts-per-million, and the rate that applied is stamped on every money-carrying fact row as `fx_rate_ppm`, so the join is a check rather than the only path.

### core.regions
*Grain: one region node, recursive*
**Tells you:** the geographic rollup path — district → metro → state → country.

| Column | Type | Key | Sample |
|---|---|---|---|
| region_id | INT | PK | 3312 |
| name | TEXT | | Brooklyn South |
| level | TEXT | | district |
| parent_region_id | INT | FK→core.regions | 1204 |
| country_id | INT | FK→core.countries | 1 |

### core.addresses
*Grain: one postal address*
**Tells you:** where something physically is — reused by customers, stores, warehouses, suppliers, employees and shipments.

| Column | Type | Key | Sample |
|---|---|---|---|
| address_id | BIGINT | PK | 7712045 |
| line1 | TEXT | | 88 Rogers Ave |
| line2 | TEXT | | Apt 4C |
| city | TEXT | | Brooklyn |
| state_province | TEXT | | NY |
| postal_code | TEXT | | 11216 |
| country_id | INT | FK→core.countries | 1 |
| region_id | INT | FK→core.regions | 3312 |
| latitude | DEC(9,6) | | 40.671200 |
| longitude | DEC(9,6) | | -73.955800 |

One address row per physical place. The address *strings* that reach the warehouse are dirtier than this, because the CRM extract carries what the customer typed rather than what the address book holds.

### core.tax_rates
*Grain: jurisdiction · tax class · validity window*
**Tells you:** what tax percentage applied to a kind of product, in a place, at a time.

| Column | Type | Key | Sample |
|---|---|---|---|
| tax_rate_id | INT | PK | 204 |
| country_id | INT | FK→core.countries | 1 |
| region_id | INT | FK→core.regions | 1204 |
| tax_class | TEXT | | home_standard |
| rate_pct | DEC(6,4) | | 0.0888 |
| valid_from | DATE | | 2025-01-01 |
| valid_to | DATE | | NULL |

---

## 5. `product` — 9 tables

### product.categories
*Grain: one category node, recursive*
**Tells you:** the merchandising hierarchy — how a SKU rolls up subclass → class → department.

| Column | Type | Key | Sample |
|---|---|---|---|
| category_id | INT | PK | 318 |
| name | TEXT | | Outdoor Cushions |
| parent_category_id | INT | FK→product.categories | 302 |
| level | INT | | 4 |
| dept_code | TEXT | | OUT |

Departments: `HDW` hardware, `BLD` building materials, `GDN` garden, `OUT` outdoor living, `HOM` home. `dept_code` is the grain the retail inventory method values stock at from E8 onward, so it is the one level of this hierarchy that a graded number depends on.

### product.brands
*Grain: one brand*
**Tells you:** who owns the label, and whether it is private-label (higher margin) or national.

| Column | Type | Key | Sample |
|---|---|---|---|
| brand_id | INT | PK | 62 |
| name | TEXT | | Aurora |
| supplier_id | INT | FK→supply_chain.suppliers | 118 |
| is_private_label | BOOL | | true |
| country_id | INT | FK→core.countries | 1 |

Aurora is Copperline's own outdoor-living label.

### product.products
*Grain: one style/model*
**Tells you:** the marketable item — name, brand, category, lifecycle — independent of size and colour.

| Column | Type | Key | Sample |
|---|---|---|---|
| product_id | INT | PK | 4180 |
| sku_root | TEXT | | AUR-CUSH |
| name | TEXT | | Aurora Deck Cushion |
| description | TEXT | | Weatherproof deck seat cushion |
| brand_id | INT | FK→product.brands | 62 |
| category_id | INT | FK→product.categories | 318 |
| tax_class | TEXT | | home_standard |
| base_uom | TEXT | | EA |
| lifecycle_status | TEXT | | active |
| is_perishable | BOOL | | false |
| created_at | TS | | 2024-02-11 09:14:00 |

### product.product_variants
*Grain: one sellable SKU*
**Tells you:** the exact thing scanned, priced, counted and shipped. Every transactional table joins here.

| Column | Type | Key | Sample |
|---|---|---|---|
| variant_id | BIGINT | PK | 550231 |
| product_id | INT | FK→product.products | 4180 |
| sku | TEXT | UNIQUE | AUR-CUSH-SLT-STD |
| barcode_ean | TEXT | | 4006381233118 |
| size | TEXT | | STD |
| color | TEXT | | Slate |
| weight_g | INT | | 1800 |
| volume_ml | INT | | NULL |
| status | TEXT | | active |

Northwave's catalog does not join here. Its SKUs live in the second upstream model (§16) and share no identifier with Copperline's.

### product.price_lists
*Grain: price list version*
**Tells you:** which pricing regime applies to which channel and region over which dates.

| Column | Type | Key | Sample |
|---|---|---|---|
| price_list_id | INT | PK | 11 |
| name | TEXT | | US Web Standard FY2026 |
| currency_code | TEXT | FK→core.currencies | USD |
| channel_id | INT | FK→store.channels | 2 |
| region_id | INT | FK→core.regions | 1204 |
| valid_from | DATE | | 2026-02-01 |
| valid_to | DATE | | 2027-01-30 |
| priority | INT | | 10 |

### product.prices
*Grain: price list · variant · qty break · window*
**Tells you:** the price a SKU was *supposed* to sell at. Compare to `order_lines.unit_price` to find overrides and discounting.

| Column | Type | Key | Sample |
|---|---|---|---|
| price_id | BIGINT | PK | 880431 |
| price_list_id | INT | FK→product.price_lists | 11 |
| variant_id | BIGINT | FK→product.product_variants | 550231 |
| unit_price | DEC(18,4) | | 29.9900 |
| min_qty | INT | | 1 |
| valid_from | DATE | | 2026-02-01 |
| valid_to | DATE | | NULL |

⚠ Windows overlap, and the winner is decided by `price_lists.priority`, not by the tighter window or the later `valid_from`. Nothing in the data says so; the pricing policy document does, and that is what makes list-versus-paid a clause question rather than a guess.

### product.promotions
*Grain: one promotion*
**Tells you:** what offer ran, on which channel, over what dates, funded by whom.

| Column | Type | Key | Sample |
|---|---|---|---|
| promotion_id | INT | PK | 742 |
| name | TEXT | | Spring Basics 25% |
| mechanic | TEXT | | pct_off |
| discount_pct | DEC(6,4) | | 0.2500 |
| discount_amount | DEC(18,4) | | NULL |
| channel_id | INT | FK→store.channels | 2 |
| start_date | DATE | | 2026-04-01 |
| end_date | DATE | | 2026-04-30 |
| budget_amount | DEC(18,4) | | 45000.0000 |
| owner_employee_id | INT | FK→hr.employees | 3391 |

### product.promotion_products
*Grain: promotion · variant*
**Tells you:** which SKUs were *eligible* for a promotion — the denominator for incremental-lift maths, against what actually got discounted.

| Column | Type | Key | Sample |
|---|---|---|---|
| promotion_id | INT | (PK,FK)→product.promotions | 742 |
| variant_id | BIGINT | (PK,FK)→product.product_variants | 550231 |
| min_qty | INT | | 1 |
| reward_qty | INT | | NULL |

### product.product_reviews
*Grain: one review*
**Tells you:** post-purchase sentiment per SKU, and whether the reviewer actually bought it.

| Column | Type | Key | Sample |
|---|---|---|---|
| review_id | BIGINT | PK | 1204871 |
| variant_id | BIGINT | FK→product.product_variants | 550231 |
| customer_id | BIGINT | FK→customer.customers | 90412 |
| order_id | BIGINT | FK→sales.orders | 8840127 |
| rating | INT | | 2 |
| title | TEXT | | Smaller than listed |
| body | TEXT | | A size under what the listing says. |
| is_verified_purchase | BOOL | | true |
| created_at | TS | | 2026-04-22 18:41:00 |

---

## 6. `store` — 4 tables

### store.channels
*Grain: one selling channel*
**Tells you:** where a sale originated — the top-level split for nearly every commercial report.

| Column | Type | Key | Sample |
|---|---|---|---|
| channel_id | INT | PK | 2 |
| name | TEXT | | Web Store |
| channel_type | TEXT | | web |
| is_active | BOOL | | true |

Four rows: store, web, marketplace, trade. Trade is the wholesale book that bills on terms, which is why `finance.ar_invoices` covers a third of revenue and nothing else.

### store.stores
*Grain: one physical store*
**Tells you:** identity, size, format and region — the unit most retail P&L and productivity metrics are measured against.

| Column | Type | Key | Sample |
|---|---|---|---|
| store_id | INT | PK | 214 |
| store_code | TEXT | | BK-FLAT |
| name | TEXT | | Flatbush Express |
| store_format | TEXT | | express |
| region_id | INT | FK→core.regions | 3312 |
| address_id | BIGINT | FK→core.addresses | 7712900 |
| manager_employee_id | INT | FK→hr.employees | 3391 |
| cost_center_id | INT | FK→finance.cost_centers | 5214 |
| sqft_selling | INT | | 4200 |
| timezone | TEXT | | America/New_York |
| open_date | DATE | | 2019-03-15 |
| close_date | DATE | | NULL |
| status | TEXT | | open |

268 rows, 44 of them ex-Northwave with `open_date` predating the acquisition close. About 62% of the estate sits in Americas timezones — that share is what makes E1's local-stamp error partial and region-shaped rather than uniform. `timezone` is the IANA zone the extract needs to reconstruct a UTC close time and never used before E6.

⚠ One store's `region_id` changes mid-year, so `dim_store` has to be an SCD2 and the comp-store restatement has to follow the new district.

### store.registers
*Grain: one POS terminal*
**Tells you:** which lane rang a transaction, and whether it was staffed or self-checkout.

| Column | Type | Key | Sample |
|---|---|---|---|
| register_id | INT | PK | 2141 |
| store_id | INT | FK→store.stores | 214 |
| terminal_code | TEXT | | POS-01 |
| register_type | TEXT | | self_checkout |
| status | TEXT | | active |

### store.store_targets
*Grain: store · fiscal period*
**Tells you:** what the store was expected to deliver — without it, no actual-against-plan question is answerable.

| Column | Type | Key | Sample |
|---|---|---|---|
| target_id | INT | PK | 33108 |
| store_id | INT | FK→store.stores | 214 |
| fiscal_period_id | INT | FK→finance.fiscal_periods | 202603 |
| revenue_target | DEC(18,2) | | 412000.00 |
| margin_target_pct | DEC(6,4) | | 0.3800 |
| footfall_target | INT | | 28500 |
| currency_code | TEXT | FK→core.currencies | USD |

---

## 7. `customer` — 6 tables

### customer.customers
*Grain: one customer*
**Tells you:** who the shopper is, when acquired and through which channel — the anchor for retention, lifetime value and segmentation.

| Column | Type | Key | Sample |
|---|---|---|---|
| customer_id | BIGINT | PK | 90412 |
| customer_code | TEXT | | C-090412 |
| first_name | TEXT | | Priya |
| last_name | TEXT | | Raman |
| email | TEXT | UNIQUE | priya.raman@example.com |
| phone | TEXT | | +1-347-555-0182 |
| birth_date | DATE | | 1991-06-04 |
| gender | TEXT | | F |
| primary_address_id | BIGINT | FK→core.addresses | 7712045 |
| preferred_store_id | INT | FK→store.stores | 214 |
| acquisition_channel_id | INT | FK→store.channels | 2 |
| signup_date | DATE | | 2023-11-02 |
| status | TEXT | | active |
| marketing_opt_in | BOOL | | true |

`customer_code` carries the current scheme, `C-######`. The old `CUST####` scheme and the crosswalk between them are E2's work and exist only in the extract: upstream this column was rewritten in place on 2024-11-04 and the old value is gone, which is precisely why the warehouse needs `raw.customer_id_map` and why 60 churned accounts have no map row to find.

About 4,000 of these are named trade accounts. The rest are retail shoppers, and about 15% of orders have no customer at all — guest checkout leaves `sales.orders.customer_id` NULL. That share is a designed population, not an accident: any per-customer metric has to declare whether guests are in the denominator, and the documents pin the answer.

### customer.customer_addresses
*Grain: customer · address · usage*
**Tells you:** which saved address is used for billing against shipping, and which is default.

| Column | Type | Key | Sample |
|---|---|---|---|
| customer_id | BIGINT | (PK,FK)→customer.customers | 90412 |
| address_id | BIGINT | (PK,FK)→core.addresses | 7712045 |
| usage_type | TEXT | | shipping |
| is_default | BOOL | | true |

### customer.loyalty_accounts
*Grain: one loyalty membership*
**Tells you:** the customer's current tier and points balance — the state of the relationship.

| Column | Type | Key | Sample |
|---|---|---|---|
| loyalty_account_id | BIGINT | PK | 44018 |
| customer_id | BIGINT | FK→customer.customers | 90412 |
| card_number | TEXT | UNIQUE | 6011900044018 |
| tier | TEXT | | gold |
| points_balance | INT | | 3420 |
| lifetime_points | INT | | 18760 |
| enrolled_at | TS | | 2023-11-02 14:22:00 |
| status | TEXT | | active |

### customer.loyalty_transactions
*Grain: one points event*
**Tells you:** how the balance got there — every earn, redeem, expiry and adjustment, tied to the causing order.

| Column | Type | Key | Sample |
|---|---|---|---|
| loyalty_txn_id | BIGINT | PK | 9911204 |
| loyalty_account_id | BIGINT | FK→customer.loyalty_accounts | 44018 |
| order_id | BIGINT | FK→sales.orders | 8840127 |
| points_delta | INT | | 85 |
| txn_type | TEXT | | earn |
| created_at | TS | | 2026-04-14 20:11:00 |

⚠ Summed deltas do not tie to `points_balance` on every account. Expiries are applied as a batch job that writes the balance and sometimes not the delta, which is the shape a reconciliation task needs: two numbers that both look authoritative and one rule that says which one to trust.

### customer.marketing_campaigns
*Grain: one campaign*
**Tells you:** what was sent, at what cost, pushing which promotion — the spend side of marketing return.

| Column | Type | Key | Sample |
|---|---|---|---|
| campaign_id | INT | PK | 1180 |
| name | TEXT | | Spring Basics Email Blast |
| medium | TEXT | | email |
| promotion_id | INT | FK→product.promotions | 742 |
| start_date | DATE | | 2026-04-01 |
| end_date | DATE | | 2026-04-14 |
| budget_amount | DEC(18,2) | | 12000.00 |
| cost_center_id | INT | FK→finance.cost_centers | 5290 |
| owner_employee_id | INT | FK→hr.employees | 3391 |

This is Copperline's own record of what it meant to spend. The ad platforms (§17) report what they say was spent, and the two disagree.

### customer.campaign_responses
*Grain: campaign · customer*
**Tells you:** the funnel for one recipient — sent, opened, clicked, and whether it converted to a real order.

| Column | Type | Key | Sample |
|---|---|---|---|
| response_id | BIGINT | PK | 60228417 |
| campaign_id | INT | FK→customer.marketing_campaigns | 1180 |
| customer_id | BIGINT | FK→customer.customers | 90412 |
| sent_at | TS | | 2026-04-13 09:00:00 |
| opened_at | TS | | 2026-04-13 09:41:00 |
| clicked_at | TS | | 2026-04-14 19:52:00 |
| converted_order_id | BIGINT | FK→sales.orders | 8840127 |
| unsubscribed | BOOL | | false |

---

## 8. `sales` — 8 tables

### sales.orders
*Grain: one order header, all channels*
**Tells you:** one complete purchase event — who, where, when, how much, in what currency. The revenue spine.

| Column | Type | Key | Sample |
|---|---|---|---|
| order_id | BIGINT | PK | 8840127 |
| order_number | TEXT | UNIQUE | W-2026-8840127 |
| customer_id | BIGINT | FK→customer.customers | 90412 |
| channel_id | INT | FK→store.channels | 2 |
| store_id | INT | FK→store.stores | NULL |
| register_id | INT | FK→store.registers | NULL |
| cashier_employee_id | INT | FK→hr.employees | NULL |
| price_list_id | INT | FK→product.price_lists | 11 |
| order_datetime | TS | | 2026-04-14 19:58:00 |
| order_status | TEXT | | shipped |
| currency_code | TEXT | FK→core.currencies | USD |
| subtotal_amount | DEC(18,4) | | 89.9700 |
| order_discount_amount | DEC(18,4) | | 22.4900 |
| tax_amount | DEC(18,4) | | 6.0000 |
| shipping_amount | DEC(18,4) | | 4.9900 |
| grand_total | DEC(18,4) | | 78.4700 |
| billing_address_id | BIGINT | FK→core.addresses | 7712045 |
| shipping_address_id | BIGINT | FK→core.addresses | 7712045 |

⚠ `order_discount_amount` is header-level and **not** pushed down to lines. Line sums never tie to `grand_total` without an allocation, and the allocation rule is a policy clause, not an arithmetic identity.

Two era notes. `order_datetime` is one clock here; the extract splits it into `event_time` and `loaded_at`, and for store-originated orders before E6 it becomes a local close stamp with no offset. `currency_code` is populated on every row here; before E4 the extract drops the column entirely, and those rows are USD by construction.

### sales.order_lines
*Grain: one order line*
**Tells you:** what was bought at what price and cost — the atomic grain for revenue, units and margin.

| Column | Type | Key | Sample |
|---|---|---|---|
| order_line_id | BIGINT | PK | 19338451 |
| order_id | BIGINT | FK→sales.orders | 8840127 |
| line_no | INT | | 1 |
| variant_id | BIGINT | FK→product.product_variants | 550231 |
| qty | DEC(12,3) | | 3.000 |
| unit_price | DEC(18,4) | | 29.9900 |
| unit_cost | DEC(18,4) | | 11.2000 |
| line_discount_amount | DEC(18,4) | | 0.0000 |
| tax_rate_id | INT | FK→core.tax_rates | 204 |
| tax_amount | DEC(18,4) | | 6.0000 |
| line_total | DEC(18,4) | | 89.9700 |
| fulfillment_id | BIGINT | FK→logistics.fulfillments | 5520118 |
| line_status | TEXT | | shipped |

⚠ Joining to `payments` or `order_line_discounts` fans out this grain. Split tender and multiple promotions per line are both normal, so a line-grain fact that joins either without collapsing first double-counts revenue.

### sales.order_line_discounts
*Grain: order line · promotion*
**Tells you:** exactly which promotion or coupon took money off which line — the attribution layer for discount analysis.

| Column | Type | Key | Sample |
|---|---|---|---|
| order_line_discount_id | BIGINT | PK | 7710223 |
| order_line_id | BIGINT | FK→sales.order_lines | 19338451 |
| promotion_id | INT | FK→product.promotions | 742 |
| coupon_id | INT | FK→sales.coupons | 3018 |
| discount_amount | DEC(18,4) | | 22.4900 |

### sales.coupons
*Grain: one coupon code*
**Tells you:** the redeemable code tied to a promotion, plus usage limits and validity.

| Column | Type | Key | Sample |
|---|---|---|---|
| coupon_id | INT | PK | 3018 |
| coupon_code | TEXT | UNIQUE | SPRING25 |
| promotion_id | INT | FK→product.promotions | 742 |
| max_redemptions | INT | | 50000 |
| per_customer_limit | INT | | 1 |
| valid_from | DATE | | 2026-04-01 |
| valid_to | DATE | | 2026-04-30 |
| status | TEXT | | active |

### sales.payment_methods
*Grain: one tender type*
**Tells you:** how customers can pay, and what each method costs in fees and settlement delay.

| Column | Type | Key | Sample |
|---|---|---|---|
| payment_method_id | INT | PK | 4 |
| name | TEXT | | Visa Credit |
| method_type | TEXT | | card |
| provider | TEXT | | Meridian Pay |
| settlement_days | INT | | 2 |
| fee_pct | DEC(6,4) | | 0.0229 |

`provider` names the external system that will settle the tender. For card rows it reads `Halcyon Payments` before the processor switch and `Meridian Pay` after; for marketplace rows it reads `Copperline Marketplace`. It is a label on Copperline's side and nothing more — the settlement rows themselves live outside this model, in §17.

### sales.payments
*Grain: one tender event*
**Tells you:** money actually collected against an order, and its authorise/capture lifecycle.

| Column | Type | Key | Sample |
|---|---|---|---|
| payment_id | BIGINT | PK | 5510887 |
| order_id | BIGINT | FK→sales.orders | 8840127 |
| payment_method_id | INT | FK→sales.payment_methods | 4 |
| amount | DEC(18,4) | | 78.4700 |
| currency_code | TEXT | FK→core.currencies | USD |
| status | TEXT | | captured |
| authorized_at | TS | | 2026-04-14 19:58:12 |
| captured_at | TS | | 2026-04-15 03:10:00 |
| gateway_reference | TEXT | | mp_3PxK92Lb |

⚠ Split tender means one order can have several rows.

**This is Copperline's only internal record of a payment, and it is deliberately not enough.** `gateway_reference` is the processor's identifier for the transaction, and it is the whole of the shared identity between this table and the settlement feeds of §17 — there is no foreign key, no settled amount, no fee, no payout date on this side. Halcyon and Meridian keep those, in their own systems, on their own clocks, with their own restatements. That gap is the reconciliation seam the world is built around: one table inside, two feeds outside, and a quarter where both feeds were live.

The processors are not modelled in this chapter. They are simulated beside it.

### sales.returns
*Grain: one return authorisation*
**Tells you:** that a customer sent something back — when, why, who approved, what was refunded.

| Column | Type | Key | Sample |
|---|---|---|---|
| return_id | BIGINT | PK | 440218 |
| rma_number | TEXT | UNIQUE | RMA-440218 |
| order_id | BIGINT | FK→sales.orders | 8840127 |
| customer_id | BIGINT | FK→customer.customers | 90412 |
| store_id | INT | FK→store.stores | 214 |
| requested_at | TS | | 2026-04-23 11:20:00 |
| approved_by_employee_id | INT | FK→hr.employees | 3391 |
| reason_code | TEXT | | wrong_size |
| return_status | TEXT | | completed |
| refund_amount | DEC(18,4) | | 22.4900 |
| refund_payment_id | BIGINT | FK→sales.payments | 5510887 |

### sales.return_lines
*Grain: one returned line*
**Tells you:** which SKU came back, in what condition, and whether it re-entered sellable stock.

| Column | Type | Key | Sample |
|---|---|---|---|
| return_line_id | BIGINT | PK | 991042 |
| return_id | BIGINT | FK→sales.returns | 440218 |
| order_line_id | BIGINT | FK→sales.order_lines | 19338451 |
| variant_id | BIGINT | FK→product.product_variants | 550231 |
| qty | DEC(12,3) | | 1.000 |
| item_condition | TEXT | | new |
| restock_flag | BOOL | | true |
| restock_warehouse_id | INT | FK→inventory.warehouses | 7 |
| refund_amount | DEC(18,4) | | 22.4900 |

⚠ `qty` is **positive**. Net revenue needs an explicit subtraction, and the sign convention is stated in the documents rather than derivable from the row.

---

## 9. `ecommerce` — 5 tables

### ecommerce.web_sessions
*Grain: one session*
**Tells you:** one visit — device, source, campaign, and whether it went anywhere. The denominator for conversion rate.

| Column | Type | Key | Sample |
|---|---|---|---|
| session_id | BIGINT | PK | 77120449 |
| customer_id | BIGINT | FK→customer.customers | 90412 |
| channel_id | INT | FK→store.channels | 2 |
| started_at | TS | | 2026-04-14 19:44:00 |
| ended_at | TS | | 2026-04-14 20:02:00 |
| device_type | TEXT | | mobile |
| os | TEXT | | iOS |
| browser | TEXT | | Safari |
| utm_source | TEXT | | email |
| campaign_id | INT | FK→customer.marketing_campaigns | 1180 |
| landing_url | TEXT | | /promo/spring-basics |
| ip_country_id | INT | FK→core.countries | 1 |
| is_bounce | BOOL | | false |

Sessions are a *derived* concept upstream too: the web platform stitches them from the collector's events. The warehouse gets the events, not the sessions, and has to stitch them again — which is why the sessionisation rule is one of the definitions the world holds in one place.

### ecommerce.page_views
*Grain: one page view (highest-volume table)*
**Tells you:** the click-by-click path, including which SKU pages got attention and for how long.

| Column | Type | Key | Sample |
|---|---|---|---|
| page_view_id | BIGINT | PK | 5501339287 |
| session_id | BIGINT | FK→ecommerce.web_sessions | 77120449 |
| page_url | TEXT | | /p/aurora-deck-cushion-slate |
| page_type | TEXT | | pdp |
| variant_id | BIGINT | FK→product.product_variants | 550231 |
| category_id | INT | FK→product.categories | 318 |
| viewed_at | TS | | 2026-04-14 19:47:00 |
| dwell_seconds | INT | | 63 |

### ecommerce.carts
*Grain: one cart*
**Tells you:** intent to buy, and whether it converted or was abandoned — plus the order it became.

| Column | Type | Key | Sample |
|---|---|---|---|
| cart_id | BIGINT | PK | 3320117 |
| session_id | BIGINT | FK→ecommerce.web_sessions | 77120449 |
| customer_id | BIGINT | FK→customer.customers | 90412 |
| created_at | TS | | 2026-04-14 19:49:00 |
| updated_at | TS | | 2026-04-14 19:58:00 |
| cart_status | TEXT | | converted |
| converted_order_id | BIGINT | FK→sales.orders | 8840127 |
| currency_code | TEXT | FK→core.currencies | USD |

### ecommerce.cart_lines
*Grain: one cart line*
**Tells you:** what went into the basket and what got pulled back out — raw material for abandonment value.

| Column | Type | Key | Sample |
|---|---|---|---|
| cart_line_id | BIGINT | PK | 8810442 |
| cart_id | BIGINT | FK→ecommerce.carts | 3320117 |
| variant_id | BIGINT | FK→product.product_variants | 550231 |
| qty | DEC(12,3) | | 3.000 |
| unit_price | DEC(18,4) | | 29.9900 |
| added_at | TS | | 2026-04-14 19:49:00 |
| removed_at | TS | | NULL |

⚠ Rows with `removed_at` set must be excluded from abandonment value.

### ecommerce.search_queries
*Grain: one on-site search*
**Tells you:** what customers looked for, whether the site found it, and whether they clicked — demand signal and catalog-gap detector.

| Column | Type | Key | Sample |
|---|---|---|---|
| search_id | BIGINT | PK | 4410228 |
| session_id | BIGINT | FK→ecommerce.web_sessions | 77120449 |
| query_text | TEXT | | slate deck cushion |
| results_count | INT | | 47 |
| clicked_variant_id | BIGINT | FK→product.product_variants | 550231 |
| click_position | INT | | 2 |
| searched_at | TS | | 2026-04-14 19:46:00 |

Of these five, only the collector's event stream is extracted today, as `raw.web_events`. Carts, cart lines and searches are simulated and unread until a task claims them.

---

## 10. `inventory` — 8 tables

### inventory.warehouses
*Grain: one DC or fulfilment node*
**Tells you:** each node's type, region and capacity — the supply side of the network.

| Column | Type | Key | Sample |
|---|---|---|---|
| warehouse_id | INT | PK | 7 |
| warehouse_code | TEXT | | DC-EDI |
| name | TEXT | | Edison NJ Distribution Center |
| warehouse_type | TEXT | | dc |
| address_id | BIGINT | FK→core.addresses | 7713001 |
| region_id | INT | FK→core.regions | 1204 |
| manager_employee_id | INT | FK→hr.employees | 2870 |
| cost_center_id | INT | FK→finance.cost_centers | 5401 |
| capacity_pallets | INT | | 18000 |
| is_active | BOOL | | true |

### inventory.inventory_balances
*Grain: variant · site · as-of snapshot*
**Tells you:** how much of a SKU sits at a location right now, and how much is genuinely sellable against reserved or in transit.

| Column | Type | Key | Sample |
|---|---|---|---|
| balance_id | BIGINT | PK | 10442877 |
| variant_id | BIGINT | FK→product.product_variants | 550231 |
| warehouse_id | INT | FK→inventory.warehouses | 7 |
| store_id | INT | FK→store.stores | NULL |
| zone | TEXT | | A |
| bin | TEXT | | A-12-04 |
| qty_on_hand | DEC(14,3) | | 842.000 |
| qty_reserved | DEC(14,3) | | 36.000 |
| qty_available | DEC(14,3) | | 806.000 |
| qty_in_transit | DEC(14,3) | | 240.000 |
| as_of_datetime | TS | | 2026-04-14 23:59:00 |

⚠ Exactly one of `warehouse_id` and `store_id` is populated. And this snapshot disagrees with a replay of `inventory_movements` by design: Corvid writes the snapshot from its own count, movements post asynchronously, and neither is wrong. **Which one is authoritative is a business decision, and it is written down in the inventory policy, not in the data.** The warehouse must pick once, in one model, or two teams answer the same availability question differently and both can defend it.

From E8 this table carries `retail_value_cents` and a department-level `cost_complement_bps` at the extract instead of a per-SKU cost, because the valuation method changed on the FY2026 boundary.

### inventory.inventory_movements
*Grain: one stock movement*
**Tells you:** every reason stock changed — sales, receipts, returns, transfers, adjustments, shrink. The ledger behind the balance.

| Column | Type | Key | Sample |
|---|---|---|---|
| movement_id | BIGINT | PK | 88104220 |
| variant_id | BIGINT | FK→product.product_variants | 550231 |
| movement_type | TEXT | | sale |
| qty | DEC(14,3) | | -3.000 |
| from_warehouse_id | INT | FK→inventory.warehouses | 7 |
| to_warehouse_id | INT | FK→inventory.warehouses | NULL |
| from_store_id | INT | FK→store.stores | NULL |
| to_store_id | INT | FK→store.stores | NULL |
| reference_type | TEXT | | order_line |
| reference_id | BIGINT | polymorphic | 19338451 |
| unit_cost | DEC(18,4) | | 11.2000 |
| moved_at | TS | | 2026-04-15 06:20:00 |
| employee_id | INT | FK→hr.employees | 2870 |

⚠ `reference_type` plus `reference_id` is a polymorphic foreign key, resolving to `order_lines`, `receipt_lines`, `transfer_lines` or `count_lines`.

### inventory.stock_transfers
*Grain: one inter-site transfer*
**Tells you:** stock being repositioned across the network, and where it sits in request → ship → receive.

| Column | Type | Key | Sample |
|---|---|---|---|
| transfer_id | BIGINT | PK | 220841 |
| transfer_number | TEXT | UNIQUE | TR-220841 |
| source_warehouse_id | INT | FK→inventory.warehouses | 7 |
| dest_warehouse_id | INT | FK→inventory.warehouses | NULL |
| dest_store_id | INT | FK→store.stores | 214 |
| transfer_status | TEXT | | received |
| requested_at | TS | | 2026-04-08 10:00:00 |
| shipped_at | TS | | 2026-04-09 06:30:00 |
| received_at | TS | | 2026-04-10 08:15:00 |
| shipment_id | BIGINT | FK→logistics.shipments | 3120770 |
| requested_by_employee_id | INT | FK→hr.employees | 3391 |

### inventory.stock_transfer_lines
*Grain: one transfer line*
**Tells you:** where units go missing in transit — requested against shipped against received against damaged.

| Column | Type | Key | Sample |
|---|---|---|---|
| transfer_line_id | BIGINT | PK | 661204 |
| transfer_id | BIGINT | FK→inventory.stock_transfers | 220841 |
| variant_id | BIGINT | FK→product.product_variants | 550231 |
| qty_requested | DEC(14,3) | | 60.000 |
| qty_shipped | DEC(14,3) | | 60.000 |
| qty_received | DEC(14,3) | | 58.000 |
| qty_damaged | DEC(14,3) | | 2.000 |
| unit_cost | DEC(18,4) | | 11.2000 |

### inventory.stock_counts
*Grain: one count event*
**Tells you:** when a physical count happened, at which site, of what scope, and who did it.

| Column | Type | Key | Sample |
|---|---|---|---|
| count_id | BIGINT | PK | 14022 |
| store_id | INT | FK→store.stores | 214 |
| warehouse_id | INT | FK→inventory.warehouses | NULL |
| count_type | TEXT | | cycle |
| started_at | TS | | 2026-04-20 06:00:00 |
| completed_at | TS | | 2026-04-20 09:45:00 |
| counted_by_employee_id | INT | FK→hr.employees | 4102 |
| count_status | TEXT | | completed |

### inventory.stock_count_lines
*Grain: one counted SKU*
**Tells you:** the gap between what the system believed and what was physically there — real shrink, in units and money.

| Column | Type | Key | Sample |
|---|---|---|---|
| count_line_id | BIGINT | PK | 330118 |
| count_id | BIGINT | FK→inventory.stock_counts | 14022 |
| variant_id | BIGINT | FK→product.product_variants | 550231 |
| system_qty | DEC(14,3) | | 24.000 |
| counted_qty | DEC(14,3) | | 21.000 |
| variance_qty | DEC(14,3) | | -3.000 |
| variance_value | DEC(18,4) | | -33.6000 |
| adjustment_movement_id | BIGINT | FK→inventory.inventory_movements | 88104901 |

### inventory.reorder_policies
*Grain: variant · site*
**Tells you:** the replenishment rules for a SKU at a location — the threshold below which it should have been reordered.

| Column | Type | Key | Sample |
|---|---|---|---|
| policy_id | BIGINT | PK | 770214 |
| variant_id | BIGINT | FK→product.product_variants | 550231 |
| warehouse_id | INT | FK→inventory.warehouses | 7 |
| store_id | INT | FK→store.stores | NULL |
| min_qty | DEC(14,3) | | 120.000 |
| max_qty | DEC(14,3) | | 1200.000 |
| reorder_point | DEC(14,3) | | 300.000 |
| safety_stock | DEC(14,3) | | 90.000 |
| lead_time_days | INT | | 21 |
| preferred_supplier_id | INT | FK→supply_chain.suppliers | 118 |

---

## 11. `supply_chain` — 6 tables

### supply_chain.suppliers
*Grain: one supplier*
**Tells you:** who you buy from, on what payment and shipping terms, in what currency, and how risky they are.

| Column | Type | Key | Sample |
|---|---|---|---|
| supplier_id | INT | PK | 118 |
| supplier_code | TEXT | UNIQUE | SUP-118 |
| name | TEXT | | Lakeshore Outdoor Co |
| supplier_type | TEXT | | manufacturer |
| address_id | BIGINT | FK→core.addresses | 7714402 |
| country_id | INT | FK→core.countries | 1 |
| currency_code | TEXT | FK→core.currencies | USD |
| net_payment_days | INT | | 45 |
| incoterms | TEXT | | FOB |
| onboarded_at | DATE | | 2021-05-19 |
| status | TEXT | | active |
| risk_rating | TEXT | | low |

Kestrel Outdoor, the partner-share vendor, is a supplier row here as well as a settlement counterparty — the share it is owed is computed from Copperline's own sales, not reported by Kestrel.

### supply_chain.supplier_products
*Grain: supplier · variant · validity window*
**Tells you:** what each supplier charges for a SKU, their minimum order and lead time — the sourcing options for one item.

| Column | Type | Key | Sample |
|---|---|---|---|
| supplier_product_id | BIGINT | PK | 440228 |
| supplier_id | INT | FK→supply_chain.suppliers | 118 |
| variant_id | BIGINT | FK→product.product_variants | 550231 |
| supplier_sku | TEXT | | LS-CSH-SL-STD |
| unit_cost | DEC(18,4) | | 11.2000 |
| currency_code | TEXT | FK→core.currencies | USD |
| moq | INT | | 500 |
| case_pack_qty | INT | | 24 |
| lead_time_days | INT | | 21 |
| is_primary_source | BOOL | | true |
| valid_from | DATE | | 2026-02-01 |
| valid_to | DATE | | NULL |

⚠ Some variants carry two rows flagged `is_primary_source` at once. Nothing upstream forbids it, so it happens, and it is the kind of duplicate a uniqueness test should catch and a landed-cost model should never silently fan out on.

### supply_chain.purchase_orders
*Grain: one PO header*
**Tells you:** a commitment to buy — from whom, into which node, by which buyer, for how much, and where it is in approval.

| Column | Type | Key | Sample |
|---|---|---|---|
| po_id | BIGINT | PK | 660214 |
| po_number | TEXT | UNIQUE | PO-2026-660214 |
| supplier_id | INT | FK→supply_chain.suppliers | 118 |
| dest_warehouse_id | INT | FK→inventory.warehouses | 7 |
| dest_store_id | INT | FK→store.stores | NULL |
| buyer_employee_id | INT | FK→hr.employees | 2904 |
| order_date | DATE | | 2026-03-03 |
| expected_date | DATE | | 2026-03-24 |
| po_status | TEXT | | closed |
| currency_code | TEXT | FK→core.currencies | USD |
| total_amount | DEC(18,4) | | 268800.0000 |
| cost_center_id | INT | FK→finance.cost_centers | 5401 |

### supply_chain.purchase_order_lines
*Grain: one PO line*
**Tells you:** how much of each SKU was ordered at what cost, and how much has landed so far.

| Column | Type | Key | Sample |
|---|---|---|---|
| po_line_id | BIGINT | PK | 3301127 |
| po_id | BIGINT | FK→supply_chain.purchase_orders | 660214 |
| line_no | INT | | 1 |
| variant_id | BIGINT | FK→product.product_variants | 550231 |
| qty_ordered | DEC(14,3) | | 2400.000 |
| qty_received | DEC(14,3) | | 2280.000 |
| qty_cancelled | DEC(14,3) | | 120.000 |
| unit_cost | DEC(18,4) | | 11.2000 |
| line_total | DEC(18,4) | | 26880.0000 |
| expected_date | DATE | | 2026-03-24 |

### supply_chain.goods_receipts
*Grain: one receipt event*
**Tells you:** that a delivery physically arrived — when, where, against which PO. Several receipts per PO is normal.

| Column | Type | Key | Sample |
|---|---|---|---|
| receipt_id | BIGINT | PK | 880412 |
| receipt_number | TEXT | UNIQUE | GR-880412 |
| po_id | BIGINT | FK→supply_chain.purchase_orders | 660214 |
| warehouse_id | INT | FK→inventory.warehouses | 7 |
| shipment_id | BIGINT | FK→logistics.shipments | 3119004 |
| received_at | TS | | 2026-03-26 07:40:00 |
| received_by_employee_id | INT | FK→hr.employees | 2870 |
| receipt_status | TEXT | | posted |

⚠ Partial receipts are the norm, so on-time-in-full and fill rate have to be computed against the PO's expected date. There is no scorecard table anywhere in this model, on purpose.

### supply_chain.goods_receipt_lines
*Grain: one received line*
**Tells you:** what actually turned up against what was rejected, plus lot and expiry for traceability.

| Column | Type | Key | Sample |
|---|---|---|---|
| receipt_line_id | BIGINT | PK | 1102284 |
| receipt_id | BIGINT | FK→supply_chain.goods_receipts | 880412 |
| po_line_id | BIGINT | FK→supply_chain.purchase_order_lines | 3301127 |
| variant_id | BIGINT | FK→product.product_variants | 550231 |
| qty_received | DEC(14,3) | | 1200.000 |
| qty_rejected | DEC(14,3) | | 18.000 |
| reject_reason | TEXT | | seam_defect |
| lot_number | TEXT | | LOT-2603-A |
| expiry_date | DATE | | NULL |
| movement_id | BIGINT | FK→inventory.inventory_movements | 88100412 |

---

## 12. `logistics` — 7 tables

### logistics.carriers
*Grain: one carrier*
**Tells you:** who moves freight and in what mode — the grouping for cost-per-shipment and on-time comparisons.

| Column | Type | Key | Sample |
|---|---|---|---|
| carrier_id | INT | PK | 12 |
| name | TEXT | | Northbound Parcel |
| scac_code | TEXT | | NBPL |
| carrier_type | TEXT | | parcel |
| contact_email | TEXT | | ops@northbound.example |
| is_active | BOOL | | true |

### logistics.fulfillments
*Grain: one fulfilment of an order, or part of one*
**Tells you:** how an order was sourced and how fast each internal step ran — promised, picked, packed, shipped.

| Column | Type | Key | Sample |
|---|---|---|---|
| fulfillment_id | BIGINT | PK | 5520118 |
| order_id | BIGINT | FK→sales.orders | 8840127 |
| fulfillment_type | TEXT | | ship_from_dc |
| warehouse_id | INT | FK→inventory.warehouses | 7 |
| store_id | INT | FK→store.stores | NULL |
| supplier_id | INT | FK→supply_chain.suppliers | NULL |
| assigned_employee_id | INT | FK→hr.employees | 2870 |
| promised_at | TS | | 2026-04-18 23:59:00 |
| picked_at | TS | | 2026-04-15 06:20:00 |
| packed_at | TS | | 2026-04-15 08:05:00 |
| shipped_at | TS | | 2026-04-15 14:40:00 |
| completed_at | TS | | 2026-04-17 15:22:00 |
| fulfillment_status | TEXT | | delivered |

⚠ Split shipments give one order several fulfilments, so "was this order on time" has no answer until a document defines it — earliest, latest, or per-line. Until that definition is written, no on-time number here is gradeable.

### logistics.shipments
*Grain: one physical shipment*
**Tells you:** a parcel or freight load in motion — direction, carrier, cost, promise date, actual delivery.

| Column | Type | Key | Sample |
|---|---|---|---|
| shipment_id | BIGINT | PK | 3120884 |
| shipment_number | TEXT | UNIQUE | SH-3120884 |
| carrier_id | INT | FK→logistics.carriers | 12 |
| shipment_direction | TEXT | | outbound |
| origin_warehouse_id | INT | FK→inventory.warehouses | 7 |
| origin_store_id | INT | FK→store.stores | NULL |
| origin_supplier_id | INT | FK→supply_chain.suppliers | NULL |
| dest_address_id | BIGINT | FK→core.addresses | 7712045 |
| dest_warehouse_id | INT | FK→inventory.warehouses | NULL |
| tracking_number | TEXT | | NB4471982203 |
| service_level | TEXT | | ground_2day |
| weight_kg | DEC(12,3) | | 5.400 |
| freight_cost | DEC(18,4) | | 14.8200 |
| currency_code | TEXT | FK→core.currencies | USD |
| shipped_at | TS | | 2026-04-15 14:40:00 |
| promised_delivery_at | TS | | 2026-04-17 23:59:00 |
| delivered_at | TS | | 2026-04-17 15:22:00 |
| shipment_status | TEXT | | delivered |

`freight_cost` is what Corvid recorded at the time. What the carrier later invoices is a different number, on a different feed, priced off a rate card that has two versions in the fixture range — which is the whole of the shipping-cost repricing work.

### logistics.shipment_lines
*Grain: one shipped line*
**Tells you:** the physical contents of a shipment, linked back to whichever document caused it.

| Column | Type | Key | Sample |
|---|---|---|---|
| shipment_line_id | BIGINT | PK | 9910228 |
| shipment_id | BIGINT | FK→logistics.shipments | 3120884 |
| variant_id | BIGINT | FK→product.product_variants | 550231 |
| qty | DEC(14,3) | | 3.000 |
| order_line_id | BIGINT | FK→sales.order_lines | 19338451 |
| transfer_line_id | BIGINT | FK→inventory.stock_transfer_lines | NULL |
| po_line_id | BIGINT | FK→supply_chain.purchase_order_lines | NULL |
| return_line_id | BIGINT | FK→sales.return_lines | NULL |

⚠ Four mutually exclusive nullable foreign keys; which one is populated follows `shipments.shipment_direction`.

### logistics.tracking_events
*Grain: one carrier scan*
**Tells you:** where a shipment was at each scan and whether anything went wrong — the feed for transit-time and exception analysis.

| Column | Type | Key | Sample |
|---|---|---|---|
| event_id | BIGINT | PK | 60114228 |
| shipment_id | BIGINT | FK→logistics.shipments | 3120884 |
| event_code | TEXT | | OUT_FOR_DELIVERY |
| description | TEXT | | On vehicle for delivery |
| city | TEXT | | Brooklyn |
| country_id | INT | FK→core.countries | 1 |
| event_at | TS | | 2026-04-17 07:12:00 |
| is_exception | BOOL | | false |

This is the tall table the carriers push back. Each carrier uses its own event vocabulary, and the mapping to a common milestone set is authored at the extract, not here.

### logistics.delivery_routes
*Grain: one route · day*
**Tells you:** a driver's day — distance, stop count, and whether the route closed on time.

Columns: `route_id` PK, `carrier_id` FK, `origin_warehouse_id` FK, `driver_employee_id` FK, `vehicle_plate`, `route_date`, `planned_start`, `actual_end`, `total_distance_km`, `route_status`. Sample route 440112 ran from warehouse 7 on 2026-04-17 and closed at 16:48.

### logistics.route_stops
*Grain: one stop*
**Tells you:** stop-level last-mile performance — estimated against actual arrival, dwell time, and why a delivery failed.

Columns: `stop_id` PK, `route_id` FK, `shipment_id` FK, `stop_sequence`, `address_id` FK, `eta`, `actual_arrival`, `dwell_minutes`, `stop_status`, `failure_reason`. Sample stop 8801447 is shipment 3120884 at sequence 23, arriving 17 minutes after its estimate.

Routes and stops have no extract. Copperline's own last-mile fleet is small — most parcels move on Northbound and the other carriers, whose scans arrive through `tracking_events` — so these two tables are simulated to keep the fiction whole and read by nothing.

---

## 13. `hr` — 8 tables

Nothing in this schema is extracted. It exists because the company exists: a store has a manager, a PO has a buyer, a payslip charges a cost center, and every approval in the model points at a person. Cost-center and journal rows that reference labour are generated from it, so it has to be real. The column lists below are given in full only for `employees`, which everything else references.

### hr.departments
*Grain: one department, recursive*
**Tells you:** the org structure and which cost center each department charges to.

Columns: `department_id` PK, `name`, `parent_department_id` FK to itself, `function`, `cost_center_id` FK, `head_employee_id` FK.

### hr.job_titles
*Grain: one job title*
**Tells you:** the role, its level and pay band, and whether it is hourly.

Columns: `job_title_id` PK, `title`, `job_family`, `job_level`, `is_hourly`, `min_salary`, `max_salary`, `currency_code` FK.

### hr.employees
*Grain: one employee*
**Tells you:** who works here, where, reporting to whom, still active or not — the people dimension behind sales, picks and approvals.

| Column | Type | Key | Sample |
|---|---|---|---|
| employee_id | INT | PK | 3391 |
| employee_number | TEXT | UNIQUE | E-3391 |
| first_name | TEXT | | Marcus |
| last_name | TEXT | | Webb |
| work_email | TEXT | | marcus.webb@example.com |
| hire_date | DATE | | 2021-08-16 |
| termination_date | DATE | | NULL |
| department_id | INT | FK→hr.departments | 88 |
| job_title_id | INT | FK→hr.job_titles | 412 |
| manager_employee_id | INT | FK→hr.employees | 2201 |
| store_id | INT | FK→store.stores | 214 |
| warehouse_id | INT | FK→inventory.warehouses | NULL |
| address_id | BIGINT | FK→core.addresses | 7715880 |
| employment_type | TEXT | | full_time |
| employment_status | TEXT | | active |

⚠ Circular foreign key with `departments.head_employee_id` — load order matters in the generator.

### hr.employee_compensation
*Grain: employee · effective window*
**Tells you:** what someone was paid during a period and why it changed — the source for an SCD2 comp history.

Columns: `compensation_id` PK, `employee_id` FK, `effective_from`, `effective_to`, `base_salary_annual`, `hourly_rate`, `currency_code` FK, `bonus_target_pct`, `change_reason`. Marcus Webb's current row runs from 2026-01-01.

### hr.payroll_runs
*Grain: one payroll run*
**Tells you:** a completed pay cycle in aggregate, and the journal entry it posted to the ledger.

Columns: `payroll_run_id` PK, `fiscal_period_id` FK, `pay_date`, `period_start`, `period_end`, `run_status`, `total_gross`, `total_net`, `currency_code` FK, `journal_entry_id` FK. Run 2607 paid 2026-04-17 for 2026-04-01 to 2026-04-15, in fiscal period 202603.

### hr.payslips
*Grain: employee · payroll run*
**Tells you:** one person's pay for one cycle, charged to a cost center — the bridge from labour hours to labour cost.

Columns: `payslip_id` PK, `payroll_run_id` FK, `employee_id` FK, `cost_center_id` FK, `regular_hours`, `overtime_hours`, `gross_pay`, `tax_withheld`, `other_deductions`, `net_pay`.

### hr.shifts
*Grain: site · date · role slot*
**Tells you:** the labour the business *planned* to have on the floor — the baseline for coverage and adherence.

Columns: `shift_id` PK, `store_id` FK, `warehouse_id` FK, `shift_date`, `start_time`, `end_time`, `role_required`, `required_headcount`.

### hr.shift_assignments
*Grain: shift · employee*
**Tells you:** who was scheduled and who actually clocked in — planned against real labour hours.

Columns: `assignment_id` PK, `shift_id` FK, `employee_id` FK, `assignment_status`, `clock_in`, `clock_out`, `hours_worked`, `overtime_hours`, `register_id` FK.

---

## 14. `finance` — 9 tables

### finance.fiscal_periods
*Grain: one fiscal period*
**Tells you:** the retail 4-5-4 accounting calendar and whether a period is still open for posting.

| Column | Type | Key | Sample |
|---|---|---|---|
| fiscal_period_id | INT | PK | 202603 |
| fiscal_year | INT | | 2026 |
| period_no | INT | | 3 |
| quarter_no | INT | | 1 |
| period_start | DATE | | 2026-04-05 |
| period_end | DATE | | 2026-05-02 |
| period_status | TEXT | | closed |

⚠ Fiscal months are not calendar months. Every "monthly" question has to state which it means, and the answer is a lookup against the calendar rather than arithmetic on a date. FY2023 is the 53-week year; the extract's `raw.fiscal_calendar` carries the authored `comp_date_ly` and `comp_week_ly` per date so the comparison never has to be computed.

### finance.cost_centers
*Grain: one cost center*
**Tells you:** the accountable bucket a cost lands in, and which store, warehouse or department it maps to.

| Column | Type | Key | Sample |
|---|---|---|---|
| cost_center_id | INT | PK | 5214 |
| cost_center_code | TEXT | UNIQUE | CC-5214 |
| name | TEXT | | Store 214 Operations |
| department_id | INT | FK→hr.departments | 88 |
| store_id | INT | FK→store.stores | 214 |
| warehouse_id | INT | FK→inventory.warehouses | NULL |
| region_id | INT | FK→core.regions | 3312 |
| owner_employee_id | INT | FK→hr.employees | 3391 |
| is_active | BOOL | | true |

### finance.chart_of_accounts
*Grain: one GL account, recursive*
**Tells you:** the account tree, each account's type and normal balance — required to sign amounts correctly in a P&L.

| Column | Type | Key | Sample |
|---|---|---|---|
| account_id | INT | PK | 4000 |
| account_code | TEXT | UNIQUE | 4000 |
| name | TEXT | | Merchandise Revenue |
| account_type | TEXT | | revenue |
| parent_account_id | INT | FK→finance.chart_of_accounts | 4 |
| normal_balance | TEXT | | credit |
| is_postable | BOOL | | true |

### finance.journal_entries
*Grain: one journal entry header*
**Tells you:** a single accounting event, which subledger produced it, and which business document it traces to.

| Column | Type | Key | Sample |
|---|---|---|---|
| journal_entry_id | BIGINT | PK | 99014402 |
| entry_number | TEXT | UNIQUE | JE-99014402 |
| fiscal_period_id | INT | FK→finance.fiscal_periods | 202603 |
| entry_date | DATE | | 2026-04-14 |
| source_system | TEXT | | ecom |
| reference_type | TEXT | | order |
| reference_id | BIGINT | polymorphic | 8840127 |
| description | TEXT | | Web order revenue recognition |
| entry_status | TEXT | | posted |
| posted_at | TS | | 2026-04-15 02:10:00 |
| created_by_employee_id | INT | FK→hr.employees | NULL |

### finance.journal_lines
*Grain: one debit/credit line*
**Tells you:** the actual double-entry postings — the bottom of every financial number in the business.

| Column | Type | Key | Sample |
|---|---|---|---|
| journal_line_id | BIGINT | PK | 220118447 |
| journal_entry_id | BIGINT | FK→finance.journal_entries | 99014402 |
| line_no | INT | | 2 |
| account_id | INT | FK→finance.chart_of_accounts | 4000 |
| cost_center_id | INT | FK→finance.cost_centers | 5290 |
| debit_amount | DEC(18,4) | | 0.0000 |
| credit_amount | DEC(18,4) | | 67.4800 |
| currency_code | TEXT | FK→core.currencies | USD |
| fx_rate | DEC(18,8) | | 1.00000000 |
| base_amount | DEC(18,4) | | 67.4800 |
| memo | TEXT | | Net merchandise revenue |

⚠ Debits equal credits per entry, always. That identity is the built-in integrity check the whole ledger rests on.

**These two tables are the independent oracle's backbone.** The monthly ledger extract the warehouse gets — `raw.finance_ledger`, entity by closed fiscal month — is a hand-built extract of this double-entry fiction, not a flat file of typed totals. That is what makes the tie a real tie: the extract is derived from postings that balance, by hand, against an independent implementation, and it is never edited to agree with anything the warehouse computes. When it disagrees, the fixture or the policy text changes, not the ledger.

### finance.ap_invoices
*Grain: one supplier invoice*
**Tells you:** what a vendor is billing, against which PO and receipt, and where it sits in approval-to-payment.

| Column | Type | Key | Sample |
|---|---|---|---|
| ap_invoice_id | BIGINT | PK | 770211 |
| supplier_invoice_number | TEXT | | LSO-2026-4412 |
| supplier_id | INT | FK→supply_chain.suppliers | 118 |
| po_id | BIGINT | FK→supply_chain.purchase_orders | 660214 |
| receipt_id | BIGINT | FK→supply_chain.goods_receipts | 880412 |
| carrier_id | INT | FK→logistics.carriers | NULL |
| invoice_date | DATE | | 2026-03-27 |
| due_date | DATE | | 2026-05-11 |
| net_amount | DEC(18,4) | | 13440.0000 |
| tax_amount | DEC(18,4) | | 0.0000 |
| total_amount | DEC(18,4) | | 13440.0000 |
| currency_code | TEXT | FK→core.currencies | USD |
| invoice_status | TEXT | | matched |
| journal_entry_id | BIGINT | FK→finance.journal_entries | 99013880 |

⚠ The three-way match — PO against receipt against invoice — fails on a small population by design. Short receipts, rejected units billed anyway, and freight invoices with no PO at all.

### finance.ap_payments
*Grain: one outbound payment*
**Tells you:** when a supplier was actually paid — the driver of days-payable and cash-out timing.

| Column | Type | Key | Sample |
|---|---|---|---|
| ap_payment_id | BIGINT | PK | 330982 |
| ap_invoice_id | BIGINT | FK→finance.ap_invoices | 770211 |
| paid_at | DATE | | 2026-05-08 |
| amount | DEC(18,4) | | 13440.0000 |
| currency_code | TEXT | FK→core.currencies | USD |
| payment_method | TEXT | | ach |
| payment_reference | TEXT | | ACH-88420117 |
| journal_entry_id | BIGINT | FK→finance.journal_entries | 99018221 |

### finance.ar_invoices
*Grain: one customer invoice (trade)*
**Tells you:** money owed by trade customers and how much has settled — the source for days-sales-outstanding and aging.

| Column | Type | Key | Sample |
|---|---|---|---|
| ar_invoice_id | BIGINT | PK | 44012 |
| invoice_number | TEXT | UNIQUE | AR-2026-44012 |
| customer_id | BIGINT | FK→customer.customers | 88104 |
| order_id | BIGINT | FK→sales.orders | 8839002 |
| invoice_date | DATE | | 2026-04-02 |
| due_date | DATE | | 2026-05-02 |
| total_amount | DEC(18,4) | | 18420.0000 |
| amount_settled | DEC(18,4) | | 9210.0000 |
| currency_code | TEXT | FK→core.currencies | USD |
| invoice_status | TEXT | | partially_paid |
| journal_entry_id | BIGINT | FK→finance.journal_entries | 99012004 |

⚠ Only trade orders are invoiced. Store and web orders settle at the till or the gateway, so about two thirds of revenue has no row here at all. Any receivables or recognition number that treats this as the revenue population is wrong by construction, and the recognition policy reads these tables by name.

### finance.budgets
*Grain: cost center · account · period · version*
**Tells you:** what was planned and re-forecast, so actuals can be measured against it.

| Column | Type | Key | Sample |
|---|---|---|---|
| budget_id | BIGINT | PK | 110284 |
| fiscal_period_id | INT | FK→finance.fiscal_periods | 202603 |
| cost_center_id | INT | FK→finance.cost_centers | 5214 |
| account_id | INT | FK→finance.chart_of_accounts | 6100 |
| budget_amount | DEC(18,2) | | 96000.00 |
| forecast_amount | DEC(18,2) | | 101500.00 |
| currency_code | TEXT | FK→core.currencies | USD |
| version | INT | | 3 |
| approved_by_employee_id | INT | FK→hr.employees | 2201 |

⚠ Several versions per period; reading them all double-counts.

---

## 15. `support` — 3 tables

### support.ticket_categories
*Grain: one category, recursive*
**Tells you:** the contact-reason taxonomy and the service-level promised for each reason.

| Column | Type | Key | Sample |
|---|---|---|---|
| category_id | INT | PK | 52 |
| name | TEXT | | Delivery - Late Arrival |
| parent_category_id | INT | FK→support.ticket_categories | 40 |
| sla_response_hours | INT | | 4 |
| sla_resolution_hours | INT | | 48 |
| owning_department_id | INT | FK→hr.departments | 91 |

### support.tickets
*Grain: one ticket*
**Tells you:** a customer problem, what it was about, how fast it was answered and resolved, and how satisfied they were.

| Column | Type | Key | Sample |
|---|---|---|---|
| ticket_id | BIGINT | PK | 6602118 |
| ticket_number | TEXT | UNIQUE | TK-6602118 |
| customer_id | BIGINT | FK→customer.customers | 90412 |
| order_id | BIGINT | FK→sales.orders | 8840127 |
| return_id | BIGINT | FK→sales.returns | 440218 |
| shipment_id | BIGINT | FK→logistics.shipments | 3120884 |
| category_id | INT | FK→support.ticket_categories | 52 |
| contact_channel | TEXT | | chat |
| subject | TEXT | | Wrong size, need exchange |
| priority | TEXT | | normal |
| ticket_status | TEXT | | resolved |
| opened_at | TS | | 2026-04-22 19:05:00 |
| first_response_at | TS | | 2026-04-22 19:22:00 |
| closed_at | TS | | 2026-04-24 10:14:00 |
| assigned_employee_id | INT | FK→hr.employees | 4780 |
| csat_score | INT | | 4 |

⚠ Service levels are inherited, so measuring attainment means walking the category tree until a level is set.

### support.ticket_messages
*Grain: one message*
**Tells you:** the back-and-forth on a ticket, internal notes included — the source for handle time and reopen behaviour.

| Column | Type | Key | Sample |
|---|---|---|---|
| message_id | BIGINT | PK | 88044120 |
| ticket_id | BIGINT | FK→support.tickets | 6602118 |
| sender_type | TEXT | | agent |
| sender_employee_id | INT | FK→hr.employees | 4780 |
| sender_customer_id | BIGINT | FK→customer.customers | NULL |
| body | TEXT | | Sorry about the fit — RMA and prepaid label issued. |
| sent_at | TS | | 2026-04-22 19:22:00 |
| is_internal_note | BOOL | | false |

---

## 16. The Northwave upstream

Northwave Supply ran its own estate, and Copperline never merged it. The generator therefore simulates **a second, smaller upstream model of the same rough shape** — an OMS, a catalog, an account book, a stock ledger, a small general ledger — and it is a separate database with no shared identifiers. That is the point. An acquired book that is a filtered copy of the parent's tables is a naming exercise; an acquired book from a different system is a real integration problem.

What differs, and what it costs downstream:

- **Its own account numbering.** Trade accounts are `NWA-#####`, 1,400 of them. They collide with nothing in Copperline's `C-######` scheme, so no join by natural key can accidentally work, and the only bridge is the merge-candidate review.
- **Its own product hierarchy.** Three levels rather than four, different department codes, different SKU roots. There is no crosswalk. A category rollup that spans both books has to be built, and a rollup that silently drops the acquired half looks completely healthy.
- **Its own store master.** 44 stores with their own codes and their own open dates, which predate the acquisition close. Any comparable-store measure has to date those stores from the close rather than from when they opened, which is a policy clause and not a property of the row.
- **Its own clocks and its own money columns.** Northwave settled everything in USD and stamped local dates. Nothing about Copperline's era boundaries applies to it.
- **A smaller ledger.** Enough to make the acquired book's revenue tie to something, not enough to be a second oracle.

**Its extract is a one-off.** Northwave's book was cut once and landed in the warehouse on 2025-03-17, six weeks after the acquisition closed on 2025-02-03. It has sat there since. The generator keeps simulating the Northwave upstream forward — the acquired stores keep trading, and their sales arrive through Copperline's own systems after the cutover — but the *account and catalog* extract was never refreshed. The frozen copies in the `nwv` namespace stopped refreshing on 2025-09-30 and nothing in the tree says so.

The merge-candidate list is the only bridge and it is deliberately incomplete: 300 true pairs, of which 90 are invisible to any fuzzy match — married names, shared `info@` addresses, resellers trading under a different name — and a further 60 pairs that are *not* the same account look nearly identical. The best plausible fuzzy result is about 210 right and up to 60 wrong, against a flagged answer of exactly 300.

## 17. The external systems

Four kinds of system sit outside these twelve schemas. They are simulated beside the upstream model, they share transaction identity with it, and they disagree with it. They are not modelled as extra schemas here because they are not Copperline's tables — the whole difficulty comes from that.

**Halcyon Payments — the legacy card processor.** Settles Copperline's card tender from before the fixture range through 2025-09-30. Its records key on its own `txn_id` and carry `merchant_ref`, a settled amount and a settlement date — no order identifier, no fee breakdown, no capture time. It was authoritative through 2025-08-31.

**Meridian Pay — the current card processor.** Live from 2025-07-01 and authoritative from 2025-09-01. Keys on its own payment identifier, carries an order reference, an event time and its own load time. Two properties matter and both come free once two processors exist: Meridian **recycles its order reference across retry attempts**, so joining it straight to Copperline's order identifier matches most rows and is wrong; and it **restates settlements up to thirty days back**, which is the correction window any reconciliation policy has to name.

For one quarter — 2025-07-01 to 2025-09-30 — both processors sent files, on purpose. Two months of Meridian shadow traffic to prove the new integration, then one month of Halcyon shadow traffic to prove nothing had been dropped. A naive union double-counts about a quarter of that year's settlement volume, which is far too large to miss. The graded difficulty is in the boundary months and in the recycled reference, not in noticing the overlap.

Both processors share exactly one thing with `sales.payments`: the gateway reference. Everything else about a settlement — the fee, the payout date, the restatement, the chargeback — exists only on the outside.

**Copperline Marketplace settlement.** Copperline's own marketplace has run since 2022 on a commission model: third-party sellers list, Copperline takes a cut. The settlement platform is a separate system from the OMS, and its records are seller-facing rather than order-facing — a remittance is one seller for one period, netting commission, refunds and adjustments across many orders. So marketplace revenue reaches the ledger by two paths that have to agree: the order lines in `sales`, and the commission in the remittance. They are generated as separate systems, from the same underlying orders.

**The advertising and email platforms.** Beacon Ads, Tessera Social, Solstice Search and Larkspur each report their own spend, impressions, clicks and conversions, on their own calendar, in their own currency, with their own attribution window. `customer.marketing_campaigns` holds what Copperline budgeted and what it believes it sent. The two never tie, and neither side is wrong — a platform counts a conversion its way and Copperline counts an order its way. These platforms also receive audiences pushed back out of the warehouse, which is the only place a mart's output leaves the building.

## 18. Deliberate hazards, consolidated

Twenty properties of this model are load-bearing. Each one is adjudicated: **catalog-grade** means a task grades it, **substrate** means it must exist from the first generated row but grades nothing on its own, and **furniture** means it makes the company real and nothing depends on it yet.

| # | Hazard | Where | Adjudication | Maps to |
|---|---|---|---|---|
| 1 | Header discount not allocated to lines — line sums never tie to `grand_total` | `orders.order_discount_amount` | catalog-grade | clause arithmetic in the recognition policy; `int_order_lines_discounted` is the derive-then-build subject |
| 2 | Return quantity stored positive; net revenue needs an explicit subtraction | `return_lines.qty` | substrate | the sign convention is pinned in the documents and applied once, in `int_net_sales_lines` |
| 3 | Fan-out: `order_lines` × `payments` × `order_line_discounts` | `sales` | substrate | the reason the `int` layer exists; grain assertions sit on every `int` model |
| 4 | Snapshot and movement replay disagree by design | `inventory_balances` against `inventory_movements` | catalog-grade | an authority decision with no local oracle; the document that picks the authority is off the path, and `int_inventory_position_daily` is where the choice lands |
| 5 | Polymorphic foreign key (`reference_type` + `reference_id`) | `inventory_movements`, `journal_entries` | furniture | realism; no task depends on it |
| 6 | Four mutually exclusive nullable foreign keys, resolved by direction | `shipment_lines` | furniture | realism; no task depends on it |
| 7 | Two rows flagged `is_primary_source` for one variant | `supplier_products` | catalog-grade | data-quality material — a uniqueness test that should fail and a landed-cost model that must not fan out |
| 8 | Guest checkout leaves `customer_id` NULL on about 15% of orders | `orders` | substrate | a designed NULL population; the denominator is pinned in a document and declared in `int_customer_lifecycle`, or it grades luck |
| 9 | Multi-currency; correct conversion needs the rate at transaction date | `exchange_rates` | catalog-grade | the mechanism behind the recognition policy's FX clause; `fx_rate_ppm` is stamped on the row at the extract |
| 10 | Retail 4-5-4 fiscal calendar is not calendar months | `fiscal_periods` | substrate | the fiscal calendar ships complete with authored comp dates; the comparable-store policy reads it |
| 11 | A store is re-districted mid-year | `stores.region_id` | substrate | `dim_store` is an SCD2; the comparable-store restatement follows the new district |
| 12 | Circular foreign key between employees and departments | `hr` | furniture | generator load order only |
| 13 | Several budget versions per period | `budgets.version` | furniture | no extract until a task claims budgets |
| 14 | Split shipments make order-level on-time ambiguous | `fulfillments` | furniture until pinned | usable only once a document defines order-level on-time; the definition would land in `int_shipment_milestones` |
| 15 | Partial PO receipts — on-time-in-full must be computed | `goods_receipts` | substrate | no scorecard table exists; `int_po_receipt_matched` computes it once for both supply chain and finance |
| 16 | `loyalty_accounts.points_balance` does not tie to summed deltas | `customer` | catalog-grade | a reconciliation task: two authoritative-looking numbers, one rule |
| 17 | Removed cart lines must be excluded from abandonment value | `cart_lines.removed_at` | furniture | carts have no extract |
| 18 | Three recursive hierarchies of variable depth | `categories`, `regions`, `chart_of_accounts` | furniture | realism; the rollups they feed are shallow enough to hand-check |
| 19 | List price is not price paid; price windows overlap and are resolved by priority | `prices` against `order_lines.unit_price` | catalog-grade | clause material — the priority rule lives in the pricing policy, not the data |
| 20 | Only trade orders have receivable invoices | `ar_invoices` | catalog-grade | confirms the trade invoicing domain; the recognition policy reads these tables by name, and a revenue population built from them is wrong by two thirds |

Three of these are worth stating together, because they are the reason the finance side of the world is gradeable at all. The chart of accounts, the balanced journal entries and the three-way match give the ledger a real bottom. The monthly extract that reaches the warehouse is derived from that bottom by hand. So a recognition number the warehouse computes can be checked against a number that was never computed the same way — which is what an independent oracle means, and what a flat file of typed totals could never be.
