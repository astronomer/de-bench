# 04 — The transformation layer

Seventy-nine upstream tables, twelve schemas, landed as the `raw.*` extracts of chapter 03, and turned into published marts by one dbt project — `dbt/copperline_analytics`. This chapter says how: what the layers are, who owns each one, what belongs in the middle, and what the rules are. The rules matter as much as the models. They are written into the platform documents as Copperline's stated norms, and several things in this world break them on purpose.

## 1. The four layers

| Layer | Owner | Materialisation | Contains | Rule |
|---|---|---|---|---|
| L0 source | the application teams | tables | the landed `raw.*` extracts | never read by a mart |
| L1 `stg` | data-platform | views | one view per landed table, 1:1 | cast, rename, dedupe, filter. **No joins** |
| L2 `int` | data-platform for shared logic, the owning team for its own | views or ephemeral | 13 shared models, plus the team-local ones named in each team's table below | logic used by **two or more teams**, or logic that collapses a grain |
| L3 marts | the owning team | tables | dims, facts, aggregates | the team's published contract |

Staging models are named `stg_<source_schema>__<table>` and live in `models/shared/staging/`, one view per landed `raw.*` table; the source list in `03-extracts.md` fixes the count. Shared intermediate models are `int_<subject>` in `models/shared/intermediate/`, and the platform team's two operational models sit in `models/shared/ops/`. Everything a team owns — its own `int` models, its marts and its snapshots — lives in `models/<team>/`, one directory per team project. **This chapter is the single inventory: the conformed dims, the shared `int` models, the model paths and every team's model list are here, and the other chapters point at it rather than repeat it.**

**The platform team owns `stg`, the conformed dimensions, and the shared `int` models.** That is an extension of the rule that already governs this world: the platform team owns traps, not marts. Staging, dims and shared intermediates are infrastructure — every team reads them and no team publishes them — so they belong with the team that publishes nothing. It also puts the cause of a cross-team failure in one project and the symptom in another, which is exactly where the ownership-boundary work wants it.

**Why `int` exists.** A definition used by two teams has to live in one place. Net revenue is used by commerce and by finance-analytics. Receipt matching is used by supply-chain and by finance-analytics. Session-to-order stitching is used by growth and by customer. If those definitions live inside marts they fork, and then two teams answer the same question differently and both can defend their answer.

**Where `int` is skipped.** Support has no shared logic and no fan-out. It goes `stg` straight to the customer team's marts. `int` is earned, not mandatory.

## 2. The shared layer

**Conformed dimensions.** Built once, by the platform team. No team may redefine one; a team that wants a different `dim_product` is a conversation, not a new model.

`dim_date` · `dim_product` · `dim_customer` · `dim_store` · `dim_warehouse` · `dim_employee` · `dim_supplier` · `dim_channel` · `dim_promotion` · `dim_carrier` · `dim_geography` · `dim_account` · `dim_cost_center` · `dim_currency`

`dim_employee` is declared and not built. There is no HR extract, so there is nothing to build it from. It stays on the list because the contract for it is what a later task would be graded against.

Two of the fourteen never finished moving to the platform team. `dim_product` is still built by commerce's snapshot pair and `dim_customer` by the customer team's `cus_dim_customer_daily`, both under a platform contract they have to satisfy. They are listed under their building team below, and `05-platform.md` records the departure.

**Shared `int` models and who consumes them:**

| `int` model | Collapses or defines | Consumers |
|---|---|---|
| `int_net_sales_lines` | gross lines minus matched returns | commerce, finance-analytics, growth, customer |
| `int_order_lines_discounted` | allocates the header discount down to the line | commerce, finance-analytics |
| `int_order_lines_costed` | landed unit cost at order date | commerce, finance-analytics |
| `int_orders_enriched` | order header plus rolled-up lines plus tender status | commerce, growth, finance-analytics |
| `int_inventory_position_daily` | movement replay reconciled to snapshot, daily position | supply-chain, commerce |
| `int_po_receipt_matched` | PO against receipt against invoice, the three-way match | supply-chain, finance-analytics |
| `int_shipment_milestones` | tracking events pivoted to one milestone row | supply-chain, customer |
| `int_sessions_funnel` | session → search → product page → cart → order | growth, customer |
| `int_customer_lifecycle` | first and last order, tenure, churn flag, the guest denominator | customer, growth |
| `int_promo_exposure` | eligible against actually discounted | growth, customer |
| `int_gl_postings_unified` | every subledger normalised to debit and credit | finance-analytics |
| `int_labor_hours` | worked hours and cost per store · day | declared, not built — no HR extract |
| `int_employees_scd` | SCD2 employee history | declared, not built — no HR extract |

Eleven built, two declared. The two unbuilt models are on the list for the same reason `dim_employee` is: when a task claims the HR side, the shape it has to fill is already stated.

---

## 3. data-platform — `projects/platform/`

**Owns:** the staging layer, the conformed dimensions, the shared intermediates, and nothing a business reads directly.

| | |
|---|---|
| **L1 `stg`** | one view per landed table, 1:1, no joins |
| **L2 `int`** | the eleven built shared models above |
| **L3** | the conformed dimensions only, less `dim_product` and `dim_customer`. **No business marts.** |
| **Operational** | `ops_audit_log` — an incremental table the `on-run-start` hook writes — and `ops_build_manifest`, both under `models/shared/ops/`. Bookkeeping; no mart reads them. |

**Reasoning.** Every other team depends on this project and this project depends on none of them. That is what makes it the right place for a definition two teams share, and the wrong place for anything with a business owner. It is also why a change here has a blast radius that crosses every team boundary in the world — which is the point.

---

## 4. commerce — `projects/commerce/`

**Owns:** the order spine. What sold, through which channel, at what price and margin, and whether the money arrived.

| | |
|---|---|
| **L0 source** | the order, order-line, return, payment, POS and marketplace extracts |
| **L1 `stg`** | `stg_sales__*` · `stg_store__*` · `stg_product__*` (platform-built) |
| **L2 `int`** | **shared:** `int_net_sales_lines` · `int_order_lines_discounted` · `int_order_lines_costed` · `int_orders_enriched`. **team-local:** `int_payment_matched` · `int_settlement_matched` · `int_return_linked` |
| **L3 marts** | `fct_order` · `fct_order_line` · `fct_payments` · `fct_returns` · `agg_daily_product_sales` · `agg_daily_store_sales` · `marts.order_economics` · `marts.channel_daily` · `marts.settlement_weekly` · `marts.recon_exceptions` · `marts.gmv_daily` · `marts.cart_abandon_daily` · `dim_product` — the conformed dim commerce never handed over — plus three factory-rendered per-partner rollups |
| **Snapshots** | `snap_product_price` · `snap_product_attrs`, which build `dim_product` |

**Reasoning.** Margin needs net revenue and landed cost, and both are also finance-analytics' numbers, so both sit in `int` and neither team rebuilds them. Store sales and web sales land in the same facts here rather than in separate team projects, because they are the same order table upstream and splitting them would put the channel comparison across an ownership boundary. Settlement reconciliation lives here because the seam it works across — one internal payments table against two external processor feeds — is an order-spine problem, not a finance problem, right up until the number reaches the ledger.

---

## 5. growth — `projects/growth/`

**Owns:** traffic, pricing realisation, promotion performance, marketing return, and everything pushed back out.

| | |
|---|---|
| **L0 source** | the clickstream, the ad-platform and email extracts, the competitor price feed, the market seed |
| **L1 `stg`** | `stg_ecommerce__*` · `stg_product__prices` · `stg_product__price_lists` · `stg_customer__marketing_campaigns` |
| **L2 `int`** | **shared:** `int_sessions_funnel` · `int_promo_exposure` · `int_orders_enriched` · `int_net_sales_lines`. **team-local:** `int_session_attributed` · `int_touch_ordered` · `int_price_compared` |
| **L3 marts** | `fct_web_sessions` · `agg_channel_funnel_daily` · `agg_search_effectiveness` · `agg_price_realization` · `agg_promo_performance` · `marts.price_index_daily` · `marts.market_pricing` · `marts.channel_roi_daily` · `marts.audience_segments` · `marts.email_engagement_daily` · `marts.funnel_daily` · `marts.experiment_readout` · plus four factory-rendered per-channel rollups |

**Reasoning.** Session-to-order stitching defines conversion rate for the whole company, so it is one model in `int` and not two in two marts. Price realisation — list price against price paid — is the only pricing logic no other team needs, so it stays in the mart. The event stream is high-volume and stays close to raw: no intermediate step earns its cost at that width, and one is not added out of symmetry.

---

## 6. supply-chain — `projects/supply/`

**Owns:** availability, inbound flow, supplier performance, freight, and delivery.

| | |
|---|---|
| **L0 source** | the inventory and movement extracts, the purchase-order and supplier master, the carrier scan and invoice feeds, the rate cards |
| **L1 `stg`** | `stg_inventory__*` · `stg_supply_chain__*` · `stg_logistics__*` |
| **L2 `int`** | **shared:** `int_inventory_position_daily` · `int_po_receipt_matched` · `int_shipment_milestones`. **team-local:** `int_lane_day` · `int_scan_normalized` · `int_rate_applied` |
| **L3 marts** | `marts.inventory_position` · `snap_inventory_daily` · `fct_inventory_movements` · `fct_stock_count_variance` · `marts.sell_through_daily` · `fct_purchase_order_lines` · `fct_goods_receipts` · `fct_supplier_performance` · `agg_supplier_spend` · `agg_replenishment_gaps` · `marts.fct_shipping_costs` · `fct_shipments` · `marts.lane_performance_daily` · `marts.supplier_scorecard_weekly` · `marts.freight_accrual_monthly` — finance reads it — · `marts.backorder_daily` · `marts.dc_capacity_daily` · plus four factory-rendered per-carrier rollups |
| **Declared, not built** | `marts.fill_rate_daily` — A-4 builds it |

**Reasoning.** Inventory, procurement and delivery sit in one team project because they are one physical flow: stock is bought, it arrives, it sits somewhere, it moves. Splitting them across three team projects would put the three-way match on one side of an ownership boundary and the receipt that proves it on the other. Two decisions carry the whole layer. First, the snapshot and a movement replay disagree, and which one is authoritative is a business decision made once, in `int_inventory_position_daily` — every downstream team reads the position, never the movements. Second, on-time-in-full has to be computed from partial receipts against expected dates, because no scorecard table exists upstream on purpose, and that computation is also finance-analytics' three-way match, so it is one model serving both.

---

## 7. finance-analytics — `projects/finance/`

**Owns:** the ledger, recognition, the P&L, working capital, and the comparable-store numbers the board reads.

| | |
|---|---|
| **L0 source** | the monthly ledger extract, the trade invoicing domain, the FX file, the fiscal calendar |
| **L1 `stg`** | `stg_finance__*` |
| **L2 `int`** | **consumes shared:** `int_gl_postings_unified`, `int_net_sales_lines`, `int_order_lines_costed`, `int_order_lines_discounted`, `int_po_receipt_matched`, `int_orders_enriched`. **team-local:** `int_invoice_line_dated` · `int_recognition_schedule` · `int_fx_applied` · `int_closed_month` |
| **L3 marts** | `fct_gl_lines` — journal-line grain, about 6M rows — · `snap_trial_balance` · `fct_invoice_line` — about 1.05M rows — · `fct_ap_invoices` · `fct_ar_invoices` · `marts.daily_revenue` · `marts.comp_sales_daily` · `marts.account_rollup` · `marts.category_margin` · `marts.ar_aging_daily` · `marts.board_revenue_weekly` · `marts.tax_daily` · `marts.dispute_daily` · `marts.cash_recon_daily` · `marts.revenue_by_book` · `agg_pnl_by_cost_center` |
| **Declared, not built** | `marts.revenue_recognized_daily` and `marts.revenue_recognized_monthly` — FIN-311 builds both · `marts.budget_variance_weekly` — no budget extract |

**Reasoning.** This is the load-bearing rule of the whole architecture. **Finance does not rebuild revenue, cost of goods or the receipt match from source.** It consumes the same `int` models commerce and supply-chain consume. Otherwise the P&L and the sales dashboard disagree and both are defensible, which is the worst outcome a warehouse can produce — not a wrong number, but two right ones. Budget-against-actual is declared and not built: there is no budget extract yet, and a mart with no source is worse than an absent one. `int_gl_postings_unified` sits in the shared layer under platform ownership, not in `models/finance/`.

---

## 8. customer — `projects/customer/`

**Owns:** identity, retention, loyalty, consent, and support.

| | |
|---|---|
| **L0 source** | the CRM extract, the acquired account book, the crosswalk, the loyalty and consent feeds, the ticket extract |
| **L1 `stg`** | `stg_customer__*` · `stg_support__*` |
| **L2 `int`** | **shared:** `int_customer_lifecycle` · `int_net_sales_lines` · `int_sessions_funnel` · `int_promo_exposure` · `int_shipment_milestones`. **team-local:** `int_customer_resolved` · `int_address_keyed` |
| **L3 marts** | `marts.customer_360` · `fct_loyalty_transactions` · `fct_campaign_responses` · `agg_customer_rfm` · `agg_cohort_retention` · `fct_tickets` · `marts.support_sla_daily` · `marts.consent_daily` · `marts.feature_customer_daily` · `marts.loyalty_daily` · `marts.churn_scores_weekly` · `marts.nps_weekly` · `dim_customer` — the second conformed dim that never moved |
| **Declared, not built** | `marts.dim_location` — A-3 builds it |

**Reasoning.** Segments and recency-frequency-value scores are derived here, not read from source — the upstream model deliberately has no pre-computed segment table, so a segment is always somebody's definition. Guest checkout leaves about 15% of orders with no customer, so every per-customer metric declares its denominator in `int_customer_lifecycle` rather than each mart choosing. Support folds in here with no `int` models of its own: no shared logic, no grain collapse, so it goes `stg` straight to the mart. That thin case is deliberate — it is the demonstration that the middle layer is earned.

---

## 9. People

There is no people team project and no HR marts. The HR schema is upstream fiction with no extract, so headcount, labour cost and employee history have nothing to build from. `int_labor_hours`, `int_employees_scd` and `dim_employee` are declared in the shared layer and unbuilt. When a task claims the HR side, the extract, the models and the owning project are authored then, against the world as it stands.

---

## 10. Dependency rules — world policy

These six rules are Copperline's stated norms. They are written into `projects/platform/README.md` and the warehouse section of `dbt/README.md`, and every contract file assumes them.

1. **Marts read `stg` and `int` only. Never L0.** A mart that reaches past staging into a landed extract has bypassed every cast, rename and dedupe the staging layer exists to apply.
2. **Marts do not read other teams' marts.** Cross-team logic moves to `int`. A team that needs another team's number needs the definition behind it, not the table.
3. **An aggregate may read its own team's base fact.** That is the one exception to rule 2, and it does not extend across a team boundary.
4. **A definition used by two teams belongs in `int`, full stop.** The first team to need it does not own it — the platform team does.
5. **Conformed dimensions are built once and versioned.** A team wanting a different one raises it; it does not fork one.
6. **Every monetary column in a landed or published table is an integer in minor units.** A money column is `*_cents BIGINT`. A fact that converts carries the base-currency amount in cents alongside it, the rate used as `fx_rate_ppm` — an integer, parts per million — and the `fx_rate_date` that rate came from. No float, ever, in a money column. The upstream application tables keep decimal `amount`, `amount_base` and `fx_rate` columns, because that is what an ERP holds; the conversion to integer minor units happens at the extract boundary and nothing downstream of it undoes that.

## 11. Where the world breaks its own rules

Copperline's warehouse does not obey these rules everywhere, and that is the design. The platform deployment hosts consumers that read off the published lineage entirely — a reader assembled from configuration at run time, an export written in hand-rolled SQL against tables it names as strings, a sensor that finds its input by name pattern rather than by dependency, dashboards that query model files directly, and reverse-ETL jobs that push a mart's output to systems outside the building. Chapter 05 names each one. None of them appears in the dbt graph, so none of them appears in the lineage a change is assessed against.

The rules above are what make those consumers gradeable rather than unfair. A world with no stated norm has no violations in it — only surprises, and an agent cannot be marked down for failing to guess an unwritten convention. A world that states the norm in the platform documents, and then puts the off-lineage consumers the registry in `06-consumers-and-docs.md` names into the tree, is asking a question with a defensible answer: the norm tells the agent what the lineage is supposed to cover, so a consumer outside it is a finding rather than a trick. The same applies to ownership. Where the built tree departs from the layering above — a staging model inside a team project, a dimension built by the team that needs it most rather than by the platform — the departure is deliberate and is recorded in the answer key. It is never an oversight, and it is never undocumented in the world; it is simply not on the path the ticket points at.

## 12. Test placement

| Layer | Tests |
|---|---|
| `stg` | primary-key uniqueness · not-null · foreign-key relationships · accepted enum values |
| `int` | row-count parity against source · dedupe correctness · grain uniqueness assertion |
| marts | dimension foreign-key integrity · reconciliation ties (net revenue against orders minus refunds; debits equal credits per journal entry) |

A failure then tells you *where* the problem is, not only that a number is wrong. That three-way split is the frame the data-quality work is written against: a failing `stg` test means the source changed, a failing `int` test means a grain broke, and a failing mart test means two things that should tie no longer do — so "make the suite green" has three different right answers and none of them is deleting the test.
