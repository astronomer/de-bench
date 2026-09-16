# 05 — The platform: teams, DAGs, the house library, the pins

This chapter says what the six team projects hold, who owns which dbt layer, what the shared
library does, where each placed structural item sits, and what the version pins arm. Every item
here exists because a task in `../../hard-task-ideas.md` needs it. Nothing here is decoration.

Two files govern the words. `CLAUDE.md` fixes the vocabulary (world, team project, task, check,
flaw, find-task). `docs/world-spec.md` fixes the layout: `worlds/copperline/workspace/` is the
tree the agent unpacks, `worlds/copperline/PLANTED.md` sits beside it and never ships,
`include/` sits at the workspace root and every project imports it, and a project never reaches
into another project's `dags/`.

Dates live in `01-timeline.md`. This chapter names eras and events; it never restates their
dates except where a shipped file must carry one, and then the date comes from `01-timeline.md`.
`04-transformations.md` owns the model-by-model dbt architecture; this chapter owns who owns
each layer and where the policy text lives. Consumers, documents and the ops surface are
`06-consumers-and-docs.md`.

Everything below is fiction. Copperline, Northwave, the carriers, the marketplace, the
processors and the engineers are invented.

## 1. The six teams

Copperline Retail Group sells through three channels: 268 stores, the web shop and Copperline
Marketplace. Copperline Trade is the wholesale arm beside them, and its named trade accounts are
the customer grain every finance and board consumer counts. Copperline bought Northwave Supply
in early 2025; the acquired book landed six weeks later, and the frozen `nwv` namespace stopped
refreshing that September. Those
three moments are the world's era boundary, and half the catalog leans on them: the Northwave
customer book, the crosswalk, the old-format order ids, the 23:05 store-local batch stamps, the
stale namespace copy.

Six teams share one warehouse (a DuckDB file, one writer) and one Airflow deployment. They meet
through the warehouse and through published partition files, which is what makes "who owns this
table" a real question rather than a naming rule.

| Team | Project | Owner | What it holds |
|---|---|---|---|
| data-platform | `projects/platform/` | P. Vance, H. Oyelaran | `include/lib/`, the blueprint factory, the whole `stg` layer, the conformed dims, the shared `int` models, contracts, the IaC config sync, the dbt build and test DAGs |
| finance-analytics | `projects/finance/` | N. Brandt | recognition, the ledger tie, the fiscal calendar, restatements, the daily flash |
| commerce | `projects/commerce/` | J. Mwangi | orders, order lines, payments, settlement, reconciliation, store sales |
| supply-chain | `projects/supply/` | K. Duffy | inventory, carriers, freight cost, the workbook, the partner share, the `legacy/` remnants |
| growth | `projects/growth/` | S. Rasmussen | clickstream, marketing spend, pricing, reverse ETL, alerting |
| customer | `projects/customer/` | L. Baptiste | the 360, the Northwave merge, support, loyalty, the feature table |

**The platform team owns traps and shared infrastructure, not business marts.** It publishes no
business table. It owns the library every other team imports, the factory they render DAGs from,
the staging layer and the conformed dims their models build on, the contract files their marts
must satisfy, and the deployment config that quietly overwrites theirs. That is deliberate: it
puts the cause of several tickets in one team's code and the symptom in another's, which
`docs/world-spec.md` item 20 asks for and nothing in the benchmark has today.

## 2. Scale

`docs/world-spec.md` sizes a world by teams, and the DAG count is a consequence. Copperline
medium is six teams. Six teams at 16-22 DAGs lands near 115 DAGs, which is lodestone's *large*
DAG count at a *medium* team count. That is the right trade and the world spec should state it
rather than smooth it over: the catalog needs six distinct owners for its ownership-boundary
tickets, and the DAG count follows.

| Team | DAGs | Hand-written | Rendered | Directories | dbt models owned |
|---|---:|---:|---:|---:|---:|
| data-platform | 20 | 18 | 2 | 10 | 70 |
| finance-analytics | 17 | 12 | 5 | 8 | 16 |
| commerce | 22 | 17 | 5 | 11 | 19 |
| supply-chain | 22 | 17 | 5 | 12 | 17 |
| growth | 18 | 10 | 8 | 8 | 14 |
| customer | 16 | 13 | 3 | 8 | 10 |
| **total** | **115** | **87** | **28** | **57** | **146** |

Four more models are missing on purpose and a task builds each: `marts.revenue_recognized_daily`
and `marts.revenue_recognized_monthly` (FIN-311), `marts.fill_rate_daily` (A-4),
`marts.dim_location` (A-3). They are not in the counts.

Plus `dbt/northwave_reporting`: 41 models inherited with the acquisition, of which 3 are still
built nightly and 38 are dead. Nothing in the tree says which 3. That is world-spec item 1, the
orphaned stack, and it is also where CT-1's upgrade blast radius gets interesting.

Tasks per DAG follow the production shape: median 4, p90 21, one hand-written outlier at 34
(`nightly_close`). The cosmos-rendered `plat_dbt_analytics_daily` sits outside that shape at 94
tasks, because a generated graph is not a written one and nobody reads it as a file.

**Fan-out density beats count.** Finding 001 measured bulk at null: 17 DAGs and 62 DAGs scored
the same, and so did a deliberately messy 62. So no DAG in this world exists to move the count.
The number that matters is how many places one change has to land, and section 7 sets that, not
this table. **If a rendered DAG would not be worth reading, cut it and take the smaller number.**
The same rule governs the models: the validator has to prove every generated table has a
producer and at least one consumer, and a model that fails it is cut.

## 3. The dbt architecture, and who owns each layer

`04-transformations.md` is the full architecture and the single inventory of dims, shared
`int` models, model paths and per-team model lists. What the team projects need from it is the
ownership split and the policy text, both of which are load-bearing here.

**Four layers.** One dbt project, `dbt/copperline_analytics`.

| Layer | Owner | Materialization | Contains | Rule |
|---|---|---|---|---|
| `raw` | the extracts | tables | the landing tables in `03-extracts.md` | read-only to every task; no mart reads it |
| `stg` | platform | views | one view per source table | cast, rename, dedupe, filter; **no joins** |
| `int` | platform for shared logic, the team for its own | views, some ephemeral | the models in `04-transformations.md` | a definition two teams use, or a step that collapses a grain |
| `marts` | the team | tables | dims, facts, aggregates | the team's published contract |

Every mart materializes into the `marts` schema, so `04-transformations.md` gives bare names in
its model tables and prose here gives `marts.` names.

**Platform owns the shared half:** one `stg` view per landed `raw.*` table under
`models/shared/staging/`, the conformed dims, the shared `int` models under
`models/shared/intermediate/`, and the two operational models under `models/shared/ops/`.
`04-transformations.md` names each one and says which teams read it; this chapter does not
repeat the list.

**`int` is earned, not mandatory.** A model reaches `int` when two teams use its definition or
when it collapses a grain. Support has neither, so its tickets go `stg` → mart with no `int`
step at all, and that thin case is written into the policy as the example. Team-local `int`
models stay in the team's directory, under `models/<team>/`.

**Two conformed dims never finished moving.** `dim_product` is still built by commerce's
snapshot pair and `dim_customer` by `cus_dim_customer_daily`, under a platform contract they
both have to satisfy. The policy says platform builds conformed dims once; these two are the
exception nobody closed, and they are in the flaw inventory for that reason. NW-214 and CT-4
both land on a dim the stated policy says belongs to another team, which is the ownership
question the tickets want and costs nothing to arrange.

### 3.1 The dependency rules are world policy

These six rules are written into `projects/platform/README.md` and repeated in
`dbt/README.md`. They are shipped prose, in the imperative, and every team's models are meant
to satisfy them.

1. Marts read `stg` and `int` only. Never `raw`.
2. Marts do not read other teams' marts. Cross-team logic moves to `int`.
3. An aggregate may read its own team's base fact. That is the one exception.
4. A definition two teams use belongs in `int`. The first team to need it does not own it;
   platform does.
5. Conformed dims are built once and versioned. A team that wants a different `dim_product`
   opens a conversation, not a model.
6. A model that changes a mart's grain or drops a column amends `contracts/<mart>.yml` first.

**The off-lineage consumers break these rules on purpose.** The config-assembled reader in
`fin_ledger_tie`, the hardcoded SQL in `fin_gl_export`, the two name-pattern sensors and the
cross-project reverse-ETL readers are all transgressions of a standard the world states in
plain words. That is what turns them from contrivances into the kind of thing a real platform
team writes a policy about and then finds the off-lineage consumers the registry in
`06-consumers-and-docs.md` names — and it is what makes fan-out grading fair, because the agent
can read the rule and go looking for what breaks it.

### 3.2 Test placement, and DQ-1's frame

Tests are placed by layer, so a failure says where the problem is rather than that a number is
wrong.

| Layer | Tests | Count |
|---|---|---:|
| `stg` | primary-key uniqueness, not-null, accepted values, relationships to the conformed dims | ~100 |
| `int` | grain uniqueness, row-count parity against the source, dedupe correctness | ~40 |
| `marts` | dim foreign-key integrity, reconciliation ties, contract column and grain checks | ~60 |

`plat_dbt_test_nightly` runs the selection each team tags. Twelve of those tests fail on the
shipped tree. Four sit on `stg` over the inventory and WMS extracts, three on `stg` over gift
cards, five on commerce mart reconciliation ties. Nine of the twelve fail because they still
encode thresholds the April 2026 quality-contract revision replaced; three are real. The memo
that carries the revision is `docs/memos/2026-04-quality-contract-revision.md`
(`06-consumers-and-docs.md` owns its text), and DQ-1 grades whether the agent separates the
nine from the three instead of muting the selection.

### 3.3 The ten mart domains fold onto six teams

`04-transformations.md` designs ten mart domains against the upstream model. Six teams carry
them.

| Mart domain | Team |
|---|---|
| Merchandising & Pricing | commerce (assortment, margin), growth (price index, competitor feed) |
| Digital / E-commerce | growth (sessions, funnel, attribution), commerce (web orders) |
| Retail Operations | commerce |
| Inventory & Planning | supply-chain |
| Supply Chain & Procurement | supply-chain |
| Logistics & Fulfillment | supply-chain |
| Finance | finance-analytics |
| Customer / CRM & Marketing | customer |
| Customer Support | customer |
| People / HR | none — upstream fiction only |

People has no team project and no extract. The HR, routing and payroll schemas stay in the
upstream model because they make the company real, and no feed extracts them until a task
claims them. That is the earn-its-place rule applied to the extract list rather than to the
model.

## 4. The six team projects

Each table lists the DAG id, its schedule, what it does, its task count, and the catalog task it
hosts. A blank host column means the DAG is furniture — but furniture a fan-out task can land
on, which is the only kind worth writing.

### 4.1 data-platform — `projects/platform/dags/`

| DAG | Schedule | What it does | Tasks | Hosts |
|---|---|---|---:|---|
| `plat_bootstrap_raw` | `0 3 * * *` | rebuilds `raw` from the fixtures | 11 | |
| `plat_dbt_analytics_daily` | `0 4 * * *` | cosmos renders `copperline_analytics` from the committed manifest | 94 | CT-1, CT-4 |
| `plat_dbt_test_nightly` | `30 5 * * *` | runs the test selection each team tags | 6 | DQ-1 |
| `plat_dbt_source_freshness` | `15 * * * *` | freshness over 14 sources | 4 | CT-1 (DB-2) |
| `plat_contracts_enforce` | `0 6 * * *` | checks every mart against `contracts/<mart>.yml` | 5 | F-33, F-29 |
| `plat_config_sync` | `0 4 * * *` | applies `config/iac/` to the deployment; replace, not merge | 7 | G1 |
| `plat_upgrade_check` | `0 7 * * 1` | runs `tools/check_upgrade.py` and reports what it knows about | 3 | CT-1 |
| `plat_warehouse_vacuum` | `0 8 * * 6` | metadata cleanup, driven by the runbook | 4 | G4 |
| `plat_lineage_publish` | `0 9 * * *` | regenerates `ops/lineage.json` from the manifest | 3 | F-29 |
| `plat_manifest_check` | `45 3 * * *` | compares the committed manifest to the models; warns, never fails | 2 | |
| `plat_partition_retention` | `0 1 * * *` | deletes partitions past `docs/retention-policy.md` | 5 | DR-1 |
| `plat_metrics_export` | `0 10 * * *` | run and parse metrics to `ops/metrics/` | 4 | COST-1 |
| `plat_conn_healthcheck` | `0 * * * *` | checks three source connections for staleness | 5 | A-7 |
| `plat_asset_republish` | asset | republishes marts as assets for cross-team consumers | 4 | CT-5 |
| `plat_report_registry_sync` | `0 11 * * *` | writes `ops/report_registry.yml` from `config/reports.yml` | 3 | |
| `plat_backfill_broker` | `None` | the sanctioned way to replay a date range | 6 | CT-2, FIN-388 |
| `plat_workbook_inbox` | `0 5 * * *` | globs `landing/<team>/*.csv` and hands each file to its owning team | 4 | XLS-1 |
| `plat_privacy_sweep` | `0 13 * * *` | works `ops/privacy/deletion_requests.csv` across every surface `contracts/privacy.md` lists | 7 | PII-1, F-29 |
| `plat_dq_profile` | `0 12 * * *` | row counts per mart into `ops.profile_daily` | 4 | |
| `plat_secret_rotation_check` | `0 6 * * 1` | flags credentials past their rotation date | 3 | |

Two more platform surfaces sit outside `dags/`:

- `projects/platform/plugins/listeners.py` — a task-instance listener written against the
  pre-3.2 hookspec, with `session` in the signature. Hosts T-12.
- `projects/platform/config/airflow_local_settings.py` — a cluster policy that forces `retries`
  and an owner e-mail on every task. Hosts M5: on the target platform this file is ignored with
  no error.

`plat_privacy_sweep` is the platform team's one consumer, and it is a consumer of other teams'
outputs: the partner-share extract, the feature exports and the audience files, none of which
dbt can see. It is one of the off-lineage readers the registry in `06-consumers-and-docs.md`
names, and the one whose read set is a document rather than code.

**dbt (70 models).** Models per `04-transformations.md`: the staging layer, the conformed dims
less the two that never moved, the shared `int` models, and the two operational models. One of
those two is load-bearing here — `ops_audit_log` is an incremental table written by the
`on-run-start` hook, and it is the row set DB-2 makes appear during a freshness run.

**Its `CONVENTIONS.md` stance.** Every `dag_id` carries its team prefix. Shared code lives in
`include/lib/` and nothing outside it may open the warehouse, build a path, or write a
partition. Contracts govern: a mart with a contract file may not change grain or drop a column
without amending the file first. Any model over one million rows is incremental with a
`unique_key`.

### 4.2 finance-analytics — `projects/finance/dags/`

| DAG | Schedule | What it does | Tasks | Hosts |
|---|---|---|---:|---|
| `fin_revenue_daily` | `0 6 * * *` | builds `marts.daily_revenue` | 7 | CT-2 |
| `fin_close_monthly` | `0 7 1 * *` | month-end close pack, ties to `raw.finance_ledger` | 9 | FIN-311 |
| `fin_ledger_tie` | `0 8 * * *` | compares marts to the ledger, writes `ops.tie_breaks` | 5 | FIN-311 |
| `fin_gl_export` | `0 8 * * *` | hand-written SQL, writes `exports/gl/gl_<ds>.csv` | 4 | F-29, CT-4 |
| `fin_daily_flash` | `45 5 * * *` | the morning flash: comp sales by region and marketplace GMV | 5 | CMP-4, REV-16 |
| `fin_board_pack_weekly` | `0 9 * * 1` | the board deck's numbers | 7 | C-new-1, swapped books |
| `fin_margin_daily` | `30 6 * * *` | `marts.category_margin`, joins order economics 1:1 | 6 | F-29 |
| `fin_ar_aging` | `0 7 * * *` | receivables aging bands | 6 | |
| `fin_fx_rates_intake` | `0 5 * * *` | lands the daily rate file | 4 | FIN-311 |
| `fin_accrual_freight` | `0 9 * * *` | reads supply's `marts.freight_accrual_monthly` | 5 | XLS-1 |
| `fin_tax_daily` | `0 7 * * *` | tax by entity and market | 5 | NLO-3 |
| `fin_cash_recon_daily` | `30 8 * * *` | bank file against payments | 6 | |
| `fin_invoice_dispute_daily` | `0 10 * * *` | disputes open and closed | 4 | FIN-311 |
| `fin_restatement_apply` | `None` | applies a restatement to a closed month | 5 | FIN-311, CA-1 |
| `fin_budget_variance_weekly` | `0 10 * * 2` | plan against actual | 4 | |
| `fin_report_publish` | `0 9 * * *` | rendered from `config/reports.yml` | 12 | |
| `fin_ledger_archive_monthly` | `0 2 2 * *` | freezes last month's ledger | 3 | |

The flash is due at 06:00 to its owner, which is why it runs at 05:45 off marts the 04:00 dbt
build already landed. Its comp figures come from `dim_date`'s `comp_date_ly`, never from a
calendar-date offset, and the 53-week edge is what makes that difference visible.

**dbt (16 models, `models/finance/`).** Models per `04-transformations.md`. Four of them carry
a task each: `daily_revenue` is CT-2's target, `account_rollup` is the board pack's top-20 at
trade account × fiscal month, `comp_sales_daily` holds the flash's comp figures, and
`revenue_by_book` is the crosswalk's victim in the swapped-books task. `int_gl_postings_unified`
is not here — it is shared and platform-owned. `marts.revenue_recognized_daily`,
`marts.revenue_recognized_monthly` and `marts.budget_variance_weekly` do not exist; FIN-311
builds the first two, and the third waits on a budget extract.

**Its `CONVENTIONS.md` stance.** Money is integer cents, everywhere, and a float in a money
column is a defect. A published figure is only correct once `fin_ledger_tie` agrees with it.
Nothing in `marts/` reads `source()` directly. Close DAGs run on the 1st and the close is not
final until the 5th business day.

### 4.3 commerce — `projects/commerce/dags/`

| DAG | Schedule | What it does | Tasks | Hosts |
|---|---|---|---:|---|
| `orders_intake` | `20 * * * *` | web and store orders into `raw.orders` | 6 | |
| `order_lines_daily` | `0 2 * * *` | line grain into `raw.order_lines` | 5 | F-29 |
| `orders_enrich_daily` | `0 3 * * *` | joins customer, product, channel | 8 | G3 |
| `nightly_close` | `0 2 * * *` | ingest, enrich, price, publish, export — one DAG | 34 | F-31 |
| `payments_intake` | `40 * * * *` | processor files into `raw.pay_halcyon_settlements` and `raw.pay_meridian_settlements` | 5 | PAY-207 |
| `psp_bridge_build` | `0 3 * * *` | builds `stg_payment_intents` from `raw.payment_intents` | 4 | NLO-4 |
| `payments_recon_daily` | `0 5 * * *` | orders against the processor, row by row | 9 | PAY-207 |
| `payment_settlement_weekly` | `0 5 * * 1` | the Monday settlement summary | 7 | LF-1042 |
| `channel_daily_intake` | `0 4 * * *` | one branch per region, `none_failed` join | 11 | W2 |
| `order_economics_daily` | `30 4 * * *` | builds `marts.order_economics` at order grain | 6 | F-29 |
| `cart_abandon_daily` | `0 8 * * *` | abandoned carts | 4 | |
| `product_snapshot_daily` | `0 1 * * *` | dbt snapshots over the product book | 4 | CT-4, SCD2 |
| `price_change_intake` | `0 1 * * *` | price change events | 4 | SCD2 |
| `returns_daily` | `0 6 * * *` | returns and their reasons | 5 | |
| `refunds_daily` | `30 6 * * *` | refunds, day-14 boundary armed | 4 | FIN-311 |
| `marketplace_orders_na` | `10 * * * *` | North American marketplace orders; the sibling to copy | 5 | align-to-sibling |
| `marketplace_orders_eu` | `10 * * * *` | European marketplace orders; same shape, same conventions | 5 | align-to-sibling |
| `marketplace_settlement_intake` | `0 7 * * *` | the marketplace remittance files | 6 | LF-1042 |
| `store_pos_intake` | `30 * * * *` | point-of-sale close batches into `raw.pos_sales_header` | 6 | |
| `store_pos_late_catchup` | `0 4 * * *` | picks up stores that missed their window | 3 | W4 |
| `fct_payments_restore` | `None` | the restore path, written after the archive-gap incident | 4 | DR-1 |
| `commerce_export_partner` | `0 9 * * *` | partition CSVs for two partners | 5 | F-29 |

The two marketplace DAGs are siblings of one marketplace, split by region, not two
marketplaces. Copperline runs its own marketplace and takes a commission; the settlement feed
is written from the operator's side.

**dbt (19 models, `models/commerce/`).** Models per `04-transformations.md`, including
`order_economics` — F-29's subject — and the snapshot-backed `dim_product`, the conformed dim
commerce never handed over.

**Its `CONVENTIONS.md` stance.** A `dag_id` is a contract with everything downstream, so it is
**never prefixed** and never renamed. Every intake is hourly and every rollup is daily; nothing
in between. SQL lives in files, one statement each. Classic operators with explicit `retries`;
no dynamic mapping, because an auditor reads the graph before the run.

### 4.4 supply-chain — `projects/supply/dags/`

| DAG | Schedule | What it does | Tasks | Hosts |
|---|---|---|---:|---|
| `sc_inventory_snapshot_daily` | `0 2 * * *` | on-hand by SKU and location | 6 | |
| `sc_inventory_movements_hourly` | `5 * * * *` | receipts, picks, transfers | 5 | |
| `sc_carrier_scan_intake` | `25 * * * *` | carrier scan events, GATE among them | 5 | LF-1077 |
| `sc_carrier_invoice_intake` | `0 3 * * *` | carrier invoice CSVs into `raw.carrier_invoices` | 4 | A-6 |
| `sc_shipping_cost_daily` | `0 4 * * *` | builds `marts.fct_shipping_costs`, one partition a day | 7 | FIN-388 |
| `sc_rate_card_intake` | `0 6 * * 1` | lands the carrier rate card | 3 | FIN-388 |
| `sc_fill_rate_daily` | `30 4 * * *` | shipped over ordered, by lane | 5 | A-4 |
| `sc_replenishment_daily` | `0 5 * * *` | maps over open DCs and pushes positions to Ironwood; `depends_on_past` on | 9 | W8, F-29 |
| `sc_shrink_weekly` | `0 9 * * 4` | shrink by location | 4 | |
| `sc_dc_transfer_daily` | `0 6 * * *` | inter-DC transfers | 5 | |
| `sc_lane_performance_daily` | `0 6 * * *` | on-time and dwell by lane | 5 | |
| `sc_supplier_scorecard_weekly` | `0 7 * * 2` | supplier fill and lead time | 6 | |
| `sc_freight_accrual_workbook` | `0 6 * * *` | reads the analyst's workbook | 5 | XLS-1 |
| `sc_partner_share_kestrel` | `0 9 * * 5` | the weekly Kestrel Outdoor SFTP drop | 5 | PII-1 |
| `sc_carrier_api_poll` | `45 * * * *` | pages the carrier tracking API | 5 | pagination |
| `sc_pdi_nightly_load` | `0 1 * * *` | runs the legacy nightly load, step by step, over SSH | 8 | M1, M7 |
| `sc_pdi_history_complete` | `0 2 * * *` | closes the legacy history bracket | 2 | M1 |
| `sc_ssis_supplier_master` | `0 3 * * *` | runs the supplier-master package | 7 | M2 |
| `sc_autosys_bridge` | `0 0 * * *` | reads the JIL run calendar, triggers what is due | 4 | M2 |
| `sc_tidal_inventory_report` | `0 8 * * 3` | the one surviving Tidal report | 3 | |
| `sc_dc_capacity_daily` | `0 7 * * *` | capacity against forecast | 4 | |
| `sc_backorder_daily` | `30 7 * * *` | open backorders and their age | 5 | |

**dbt (17 models, `models/supply/`).** Models per `04-transformations.md`. Four of them carry a
task each: `fct_shipping_costs` lands as ninety daily partitions and is FIN-388's subject,
`int_scan_normalized` is where LF-1077's era-aware fix lands, `inventory_position` is read by
the replenishment feed and the merch dashboards, and `sell_through_daily` by the dashboards and
the Kestrel share. `marts.fill_rate_daily` does not exist; A-4 builds it.

**Its `CONVENTIONS.md` stance.** Prefix `sc_`, and the cadence goes at the end of the name.
Anything that touches the legacy runner goes through `include.lib.legacy_runner`, one step per
task, so a step can retry alone. Every DAG that maps over locations sets `depends_on_past=True`,
because a movement out of order corrupts on-hand. The team keeps its own `lib/paths.py` and does
not use `include.lib.warehouse` for partition paths.

### 4.5 growth — `projects/growth/dags/`

| DAG | Schedule | What it does | Tasks | Hosts |
|---|---|---|---:|---|
| `gro_clickstream_intake` | `0 * * * *` | Driftwood events into `raw.web_events` | 6 | EVT-1 |
| `gro_sessionize_daily` | asset | sessions from events | 7 | NLO-2 |
| `gro_event_replay_repair` | `None` | repairs a replayed window | 4 | EVT-1 |
| `gro_attribution_daily` | asset | touch to order attribution | 8 | |
| `gro_marketing_spend_intake` | `0 5 * * *` | ad spend by channel | 5 | |
| `gro_channel_roi_daily` | asset | spend against attributed revenue | 6 | |
| `gro_comp_prices_intake` | `0 6 * * *` | the competitor price feed | 5 | G1, F-30 |
| `gro_price_index_daily` | `0 7 * * *` | `marts.price_index_daily`, one row per lane per day | 5 | F-30 |
| `gro_market_seed_build` | `0 8 * * 1` | rebuilds the market seed | 4 | NLO-3 |
| `gro_pricing_mart_daily` | asset | price and margin by market | 6 | NLO-3 |
| `gro_reverse_etl_crm` | asset | pushes segments to Halyard | 5 | CT-5 |
| `gro_reverse_etl_ads` | asset | pushes audiences to Beacon Ads and Tessera Social | 5 | CT-5, G2 |
| `gro_audience_export` | `0 9 * * *` | audience CSVs | 4 | |
| `gro_funnel_daily` | asset | funnel steps by day | 5 | |
| `gro_experiment_readout_daily` | `0 10 * * *` | experiment results | 6 | |
| `gro_email_engagement_daily` | `0 8 * * *` | Larkspur opens, clicks, unsubscribes | 4 | |
| `gro_seo_rank_intake` | `0 4 * * *` | pages the rank API | 4 | pagination |
| `gro_alerting_daily` | `0 11 * * *` | fires the volume and revenue alerts from `config/alerts.yml` | 5 | NLO-1, CA-2 |

**dbt (14 models, `models/growth/`).** Models per `04-transformations.md`. `market_pricing` is
NLO-3's target and `audience_segments` is what the reverse-ETL pair pushes out.

**Its `CONVENTIONS.md` stance.** Schedule on the asset that feeds you, never on a clock, unless
the source is a clocked file drop. TaskFlow with type hints; a task that does not return
something is a task in the wrong place. Every asset is named once in `lib/assets.py` and
referred to by that name. Config belongs in YAML; the Python is a renderer.

### 4.6 customer — `projects/customer/dags/`

| DAG | Schedule | What it does | Tasks | Hosts |
|---|---|---|---:|---|
| `cus_customer_intake` | `0 2 * * *` | the Copperline book into `raw.customers` | 5 | NW-214 |
| `cus_northwave_accounts_intake` | `30 2 * * *` | the acquired book into `raw.nwv_accounts` | 4 | NW-214 |
| `cus_merge_candidates_review` | `0 3 * * *` | writes `ops.merge_candidates` from the integration team's review | 5 | NW-214 |
| `cus_crosswalk_apply_daily` | `30 3 * * *` | applies the acquisition crosswalk to account books | 5 | swapped books |
| `cus_dim_customer_daily` | `0 4 * * *` | builds `marts.dim_customer` | 6 | NW-214 |
| `cus_address_normalize_daily` | `0 5 * * *` | normalizes and keys addresses | 4 | A-3 |
| `cus_customer_360_daily` | `0 6 * * *` | the 360 table; the world's slow model | 9 | NLO-5 |
| `cus_360_publish` | asset | publishes the 360 once the 360 table lands | 4 | T-11 |
| `cus_features_daily` | `0 8 * * *` | builds `marts.feature_customer_daily` and writes `exports/features/<ds>.parquet` for Compass | 6 | ML-1 |
| `cus_support_tickets_intake` | `15 * * * *` | Halyard support tickets | 5 | |
| `cus_support_sla_daily` | `0 7 * * *` | SLA attainment by queue | 5 | |
| `cus_loyalty_daily` | `0 7 * * *` | points earned and burned | 5 | |
| `cus_consent_sync_daily` | `0 8 * * *` | marketing consent state | 4 | |
| `cus_churn_scores_weekly` | `0 9 * * 5` | churn scores | 6 | |
| `cus_nps_weekly` | `0 10 * * 5` | survey rollup | 3 | |
| `cus_northwave_backfill` | `None` | replays a mart into the pre-acquisition era | 4 | NW-231, CT-3 |

**dbt (10 models, `models/customer/`).** Models per `04-transformations.md`. Two carry a task:
`dim_customer` is NW-214's target and the second conformed dim that never moved to platform,
and `feature_customer_daily` is SCD2-joined and ML-1's leak surface. `marts.dim_location` does
not exist; A-3 builds it.

**Its `CONVENTIONS.md` stance.** Every customer-facing model carries `source_book` (`copperline`
or `northwave`) and every join across the two goes through `stg_crosswalk`, never on a natural
key. A model that touches personal data declares it in `meta.pii` and never lands in an export.
Surrogate keys come from `dbt_utils.surrogate_key` with the columns listed in the same order as
the dimension they key. Anything that reads pre-acquisition dates states the era in the model
description.

## 5. Where the teams differ, and the one contradiction

Six teams, six `CONVENTIONS.md` files, one house file at the workspace root that holds only what
all six accepted. **The `CONVENTIONS.md` files hold style, and nothing else.** They are read
first, every time, so an authority placed in one is a single-file lookup and costs a task
nothing. Every rule below is true, current, and free to follow.

| | platform | finance | commerce | supply | growth | customer |
|---|---|---|---|---|---|---|
| `dag_id` | always team-prefixed | `fin_` | **never prefixed** | `sc_`, cadence last | `gro_` | `cus_` |
| Style | TaskFlow, typed | classic operators | classic, explicit `retries` | classic plus the runner | TaskFlow, typed | mixed, follow the file |
| Fanning out | map, never loop | no mapping in finance | **no dynamic mapping at all** | mapping with `depends_on_past` | map freely | map over books |
| Scheduling | cron | cron, close on the 1st | hourly intake, daily rollup | cron | **on the asset, not a clock** | cron plus one asset |
| Paths | `include.lib.warehouse` only | `include.lib.warehouse` | SQL in files | **its own `lib/paths.py`** | `lib/assets.py` | `include.lib.warehouse` |

These are differences, not contradictions. That is on purpose: six mutually contradicting files
would make every ticket a puzzle about the files. The cheap and useful one is the first row —
commerce never prefixes a `dag_id`, platform always does. It bites whenever a task adds a DAG
both teams read, and it makes "match the file you are in" a real decision rather than a slogan.

### 5.1 The load-bearing contradiction, and where it lives

Two rules point in opposite directions on exactly the tables the finance tickets touch. Neither
sits in a `CONVENTIONS.md`.

| Rule | Stated in | Why |
|---|---|---|
| No full refresh on the shared warehouse; any model over one million rows is incremental with a `unique_key` | `projects/platform/README.md`, and the warehouse section of `dbt/README.md` | the DuckDB file takes one writer, and a full rebuild of a large model locks it for minutes |
| No incremental models in `models/finance/` | `contracts/finance-close.md`, and `projects/finance/README.md` | a restated month has to restate, and an incremental model quietly keeps the old figures |

Two finance marts have crossed one million rows — `fct_invoice_line` at about 1.05M invoice
lines and `fct_gl_lines` at about 6M journal lines.
Both files say their rule in plain words, and both are read only when the agent goes looking for
the team's standard rather than its style.

`docs/change-management.md` settles it, and settles it in a way the agent has to read rather
than guess: **the owning team's file governs inside that team's folder; the platform file
governs shared infrastructure; where they meet, the contract file wins, and a change that
neither allows needs the conflict written down before the work ships.** That is the ladder CA-1
leans on, and the right answer to CA-1 is to ship the compatible half and record the conflict,
not to pick a side. CM-1 through CM-4 in `06-consumers-and-docs.md` carry the text.

## 6. The house library

`include/` sits at the workspace root, on `PYTHONPATH` for the trial and for scoring.
`CONVENTIONS.md` names it protected: drive it, read it, never rewrite it.

**The docstrings are the answer key.** That is the whole design. The trace evidence says agents
open house-library source almost never — the forked `freight.py` was opened in 0 of 108 trials —
so a trap inside a house wrapper is a bet that the agent trusts a familiar-looking name. The bet
is only fair if the truth sits in one file, one open away, written plainly. Write the docstrings
first; the code follows them.

```
include/
  lib/
    __init__.py
    pipeline.py        @lake_task, Reject, the manifest
    loaders.py         CsvToWarehouseOperator, JsonApiToWarehouseOperator
    warehouse.py       connect(), delete_insert(), write_partition()
    watermark.py       since(), advance()
    calendar.py        the 4-5-4 fiscal calendar, the market calendar, today()
    contracts.py       loads contracts/<mart>.yml for the enforcement DAG and the tests
    legacy_runner.py   the stub runner for PDI, SSIS and JIL artifacts
    blueprint/         the factory: registry.py, kinds/, render.py
  data/                partition files, written at run time
```

### 6.1 `pipeline.py` — `@lake_task`

```python
def lake_task(_fn=None, **kwargs):
    """The house task decorator. Same call shape as Airflow's @task, three
    differences that matter.

    1. RETURN VALUES GO TO THE MANIFEST, NOT XCOM. Whatever a lake_task returns
       is written to the run manifest under `<dag_id>/<task_id>/<data_interval_start>`
       and nothing is pushed to XCom. Read it back with `manifest.get(task_id)`,
       which resolves against the CURRENT interval. `.output` and `xcom_pull`
       still work at the API level and still return a value: the LAST manifest
       entry written for that task, whichever interval wrote it. On a first run
       that is usually right. On a re-run, a backfill, or any second interval it
       is yesterday's answer, and nothing warns you.

    2. `Reject` IS NOT A FAILURE. Raise `Reject(reason, rows=...)` and the wrapper
       writes the rows to the reject table named in the DAG's `lake_config`,
       marks the batch skipped, and lets the run continue. Any other exception
       fails the whole DAG run, not the task: the wrapper sets `retries=0` on the
       generated operator and routes retry policy through
       `lake_config["retry"]`. Setting `retries` on the task does nothing (see 3).

    3. UNKNOWN KWARGS BECOME MANIFEST TAGS. Anything this decorator does not
       recognize is not an error and is not forwarded to the operator. It is
       written to the manifest entry as a tag, which is how the ops dashboard
       gets its labels. `@lake_task(retries=3)` is legal, silent, and has no
       effect on retries at all.

    Recognized kwargs: task_id, pool, queue, map_index_template, lake_config.
    Everything else is a tag.
    """
```

`Reject` carries `reason: str` and `rows: list[dict]`. `manifest.get(task_id, interval=None)`
defaults to the current interval. The manifest itself is a JSON file per DAG under
`include/data/_manifest/`.

Hosts A-2. The trap: an agent copies the neighbours' `@lake_task` and then wires the reject path
with remembered `@task` semantics — raises a plain exception, sets `retries=3`, pulls the
upstream value with `.output`. It parses. Run one may pass. Run two reads a stale manifest entry,
and the reject fixture row fails the run.

### 6.2 `loaders.py` — `CsvToWarehouseOperator`

```python
class CsvToWarehouseOperator(BaseOperator):
    """Load CSV files into a warehouse table.

    mode="append" is the DEFAULT and it is not what the public operators of this
    name do. History tables were this operator's first use and the default never
    moved. An append load of the same file twice puts the rows in twice; there is
    no key check and no dedup.

    mode="replace" scopes the delete to ONE PARTITION and needs `partition_col`
    to know which. Passing mode="replace" without `partition_col` is accepted,
    logs a warning at INFO, and falls back to append — the guard exists because a
    table-wide delete on the shared warehouse locked it for nine minutes once.

    So the rerun-safe call is both arguments together:

        CsvToWarehouseOperator(
            task_id="load",
            source="fixtures/carrier_invoices_{{ ds }}.csv",
            table="raw.carrier_invoices",
            mode="replace",
            partition_col="invoice_date",
        )

    Args:
        source: a path or a glob; templated.
        table: `schema.table`; created from the CSV header if absent.
        mode: "append" (default) or "replace".
        partition_col: the column the replace deletes on. Required for replace.
        columns: explicit column types. Without it the CSV sniffer guesses, and
            the guess is not stable across DuckDB minors.
    """
```

Hosts A-6. `sc_carrier_invoice_intake` uses it correctly, with both arguments. A dozen other DAGs
use it in append mode on genuine history tables, so the default looks reasonable everywhere you
meet it. `ops/incidents/2025-11-03-vendor-file-doubled.md` names the symptom, never the cause.

`JsonApiToWarehouseOperator` sits in the same file and is the pagination surface — see 7.5.

### 6.3 `warehouse.py` — the connection helper

```python
def connect(read_only: bool = False) -> duckdb.DuckDBPyConnection:
    """Open the house warehouse.

    Attaches two databases and sets the search path:

        ATTACH 'warehouse/copperline.duckdb'  AS main
        ATTACH 'warehouse/northwave.duckdb'   AS nwv   (READ_ONLY)
        SET search_path = 'nwv,main'

    NOTE THE ORDER. An unqualified name resolves against `nwv` FIRST. That was
    the integration's compromise: the acquired reports kept running unqualified
    while their models were ported, and the port is not finished. Twelve tables
    exist in both databases with the same name, and the `nwv` copies stopped
    refreshing when the namespace was frozen on 2025-09-30.

    Qualify every name you write. `select * from staging.orders_enriched` reads a
    copy frozen last September and tells you nothing about it.
    `select * from main.staging.orders_enriched` reads today's.

    read_only=True opens a snapshot copy instead of the live file. Use it for
    anything that only reads: the live file takes one writer, and a scheduled
    run holds it.
    """
```

Hosts G3. `staging.orders_enriched`, `staging.customer_book` and ten others exist in both.
Queries return plausible rows from the wrong era; totals stay believable and the graded rows are
off by the stale delta.

The copy is nine months stale at world today, which is further from the truth than a
five-month copy would be. **Re-measure trap conservation at that distance before G3 is
authored**: if a company total moves far enough that an ordinary sanity check catches it, the
trap has stopped being silent and the graded delta has to be re-chosen. The measurement goes in
the hand-written half of `PLANTED.md`.

`delete_insert(table, partition_col, ds, rows)` and `write_partition(layer, name, ds, header,
rows)` sit beside it and are the sanctioned rerun-safe writes.

### 6.4 `watermark.py` — the watermark helper

```python
def since(name: str, context) -> datetime:
    """The low-water mark for this run, for incremental reads.

    Returns the stored mark for `name`, or `context["data_interval_start"]` if
    there is none — NOT `ds`, and not the wall clock. That matters the moment a
    DAG is not daily: on an hourly schedule `ds` is the same string for
    twenty-four consecutive runs, so a helper keyed by `ds` re-reads the whole
    day every hour and a delete-insert scoped to `ds` deletes the sibling hours
    that already landed.

    `advance(name, context)` stores `data_interval_end` and must be the last
    thing a successful task does. A task that reads with `since()` and never
    calls `advance()` re-reads the same window forever and never says so.

    Both functions are interval-keyed, so replaying an interval reproduces it.
    Nothing here reads the wall clock.
    """
```

Hosts F-30. The world ships three DAGs that call `since()` correctly on a daily schedule; moving
one to hourly is where the difference appears.

### 6.5 `calendar.py`, `contracts.py`, `legacy_runner.py`

- `calendar.py` — `fiscal_week(d)`, `fiscal_period(d)`, `comp_date_ly(d)`,
  `is_market_holiday(d, market)`, `business_days_between(a, b)`, and `today()`. The 4-5-4 retail
  calendar makes "the week of the graded Monday" a defined thing, and the market calendar is what
  makes W2's all-branches-skip day exist. `today()` reads the `WORLD_TODAY` environment variable
  the harness exports, so nothing in the tree reaches for the wall clock. T-11's second candidate
  lives here too.
- `contracts.py` — `load(mart)` reads `contracts/<mart>.yml` and returns columns, grain and
  freshness. `plat_contracts_enforce` calls it, and so does a pytest under `tests/`. It is what
  makes F-29's sixth landing spot exist.
- `legacy_runner.py` — a small readable stub that shells one step of a PDI job, an SSIS package
  or a JIL box over SSH and returns its exit code and its variable block. It is readable on
  purpose: M1 and M7 grade on the artifact's semantics, so the runner must not be the puzzle.

### 6.6 The blueprint factory, and the file that explains it

`include/lib/blueprint/` renders every `projects/*/dags/*.dag.yaml` into a DAG. `registry.py`
maps a blueprint name to a builder; `kinds/` holds the builders; `render.py` walks the YAML and
wires `depends_on`.

```yaml
# projects/growth/dags/comp_price_index.dag.yaml
dag_id: gro_comp_price_index
schedule: "0 7 * * *"
description: competitor price index by market
steps:
  land:
    blueprint: csv_intake
    source: comp_prices_{{ ds }}
    table: raw.comp_prices
  index:
    blueprint: rollup
    depends_on: [land]
    source: raw.comp_prices
    group_by: [market_id, lane_id]
    agg: median
    column: price_cents
    target: marts.price_index_daily
```

Five blueprint kinds ship: `csv_intake`, `rollup`, `dbt_select`, `partition_export`,
`sensor_wait`.

**Tasks extend the factory; they do not repair it.** That is the fork world-spec item 12 asks
for, and it is the honest version of reuse-or-reinvent. A ticket that needs a step the five kinds
do not cover has two paths: add a sixth kind under `kinds/` and register it, or hand-write a DAG
beside the YAML. The first is right; the second runs green. `include/lib/blueprint/` is
protected, so a task that needs a new kind ships it as a team-local kind registered through the
documented entry point — `registry.register(name, builder)` from `projects/<team>/dags/kinds.py`,
which the loader imports if it exists.

The guidance file that goes with it is `docs/blueprints.md`: what the five kinds do, the YAML
schema, how `register()` works, and one worked example of a team-local kind. It says what the
mechanism is and stops there. Per finding 004, no guidance file in this world states a
verification workflow, a date range, or anything a check grades. The lean rule holds: policy and
protected paths in `AGENTS.md` and `CONVENTIONS.md`, mechanism in `docs/`, and never a grading
criterion in world prose.

## 7. Placed structural items

Each item has one address. If it moves, the task that needs it moves with it.

### 7.1 The fat DAG worth splitting

`projects/commerce/dags/nightly_close.py`. 34 tasks in one file: intake for four channels,
enrichment, pricing, the economics build, three exports and a notification. The ingest half takes
about nine minutes; the finance half takes forty seconds and is what people rerun. One `@daily`
schedule, one set of retries, no task groups. Its docstring says it was three DAGs until the 2025
close and nobody has split it back. Hosts F-31.

### 7.2 The config-assembled consumer

`projects/finance/dags/fin_ledger_tie.py`, line ~40:

```python
cfg = yaml.safe_load(open(CONFIG / "recon.yml"))
table = f"{cfg['schema']}.{cfg['subject']}_{cfg['grain']}"   # marts.order_economics
```

`config/recon.yml` holds `schema: marts`, `subject: order`, `grain: economics`. Grep for
`order_economics` finds five places; this is the sixth and it is in another team's project.
Missing it fails `fin_ledger_tie`'s total-against-source assertion at run time, not at parse.
`docs/lineage.md` lists it, which is the fairness spine: the grep loop finds five of six, the
document finds six of six.

### 7.3 The hardcoded-SQL export

`projects/finance/dags/fin_gl_export.py`. A `PythonOperator` with a 40-line SQL string, no
`ref()`, no source declaration, writing `exports/gl/gl_<ds>.csv` for Ironwood. dbt lineage cannot
see it. It reads `marts.order_economics` at order grain (F-29) and it filters
`where dbt_valid_to is null` against `dim_product` (CT-4). Two tasks land on it and neither
ticket names it.

### 7.4 The two name-pattern sensors

`projects/supply/dags/sc_freight_accrual_workbook.py` waits on partition files with a glob:

```python
FileSensor(task_id="wait_for_costs", filepath="include/data/marts/shipping_costs_*_{{ ds }}.csv")
```

`projects/supply/dags/sc_partner_share_kestrel.py` does the same on the weekly sell-through
extract, matching `sell_through_*_{{ macros.week_of(ds) }}.csv` before it opens the SFTP session.
Rename either output or change its layout and the sensor waits forever — no error, no failed
task, a run that sits in `running` until the timeout, and in Kestrel's case a vendor who stops
receiving a file nobody notices. Nothing anywhere references either producing model by name.

### 7.5 The paginated fixture API and CLI

`include/lib/loaders.py::JsonApiToWarehouseOperator` reads a local stub service,
`scripts/fixture_api.py`, which serves the fixtures over a loopback port at trial setup. Every
listing endpoint returns at most 50 rows and a `next_page_token`; the token is absent on the last
page only. The same stub backs the CLI, `scripts/cpl`, so `cpl orders list --since <date>` also
pages.

```json
{"items": [...50 rows...], "next_page_token": "eyJvZmZzZXQiOjUwfQ", "total": null}
```

`total` is always null, so nothing tells the agent how many pages there are. This is the one
lever that makes every enumeration-graded task punish a first-page answer, and it costs one file.
`sc_carrier_api_poll` and `gro_seo_rank_intake` both page correctly and are the readable example.

### 7.6 The legitimately slow verification step

`cus_customer_360_daily` takes about seven minutes per logical date on the generated fixtures:
eight joins over two years of events, no incremental.

`sc_shipping_cost_daily` is FIN-388's whole design — the brute-force plan does not fit the wall
and the informed plan does. **The per-partition runtime is an open probe.** Measure it at the
canonical package volume, then size the model so that ninety partitions cost **at least 1.5x the
wall cap**: against a thirty-minute cap that is forty-five minutes of work, so at least thirty
seconds a partition. A margin of a few minutes fails on a slow box for the wrong reason and
grades the machine instead of the plan. Never lower the cap to make this bite; price the plan
instead. The measured number goes in the hand-written half of `PLANTED.md`.

### 7.7 The blocked verification path

The warehouse is one DuckDB file and it takes one writer. A scheduled run holds it, so a
`dbt run` started while `plat_dbt_analytics_daily` is running fails with a lock error. The way
through is `connect(read_only=True)` or `scripts/warehouse_snapshot.sh`, which copies the file to
`warehouse/snapshot.duckdb` for read-only work. Both are documented in `dbt/README.md`, one open
away.

This must stay resolvable in one read. A block whose unblocking is a coin flip is a grading bug
that reads as a capability gap. It is graded because CT-1 and NLO-5 both need a differential
against a baseline the agent has to snapshot before it changes anything: the agent that never
unblocks the path never gets a baseline, and its own edit destroys the one it needed.

### 7.8 The sibling pairs

Three pairs carry the de facto spec for align-to-sibling tasks:

- `marketplace_orders_na` and `marketplace_orders_eu` — same shape, same retry policy, same
  reject handling, same partition naming. A third region is a copy job with two real decisions in
  it.
- `gro_reverse_etl_crm` and `gro_reverse_etl_ads` — both asset-scheduled, both idempotent by
  segment key.
- `fin_revenue_daily` and `fin_margin_daily` — the finance house style: full rebuild, integer
  cents, ledger tie at the end.

### 7.9 `legacy/` — the foreign artifacts

Read-only text. Nothing executes it; `include/lib/legacy_runner.py` shells it as a stub.

```
legacy/
  pdi/
    nightly_load.kjb                    the job: 8 steps, a history bracket
    trn/ set_constants.ktr              staging schema, watermark column
         resolve_load_control.ktr       reads ops.load_control, sets 4 variables
         load_inventory.ktr             consumes the variables
         load_movements.ktr
         load_carrier_scans.ktr
         load_supplier_master.ktr
         history_complete.ktr           closes the bracket for the whole job
    README.txt                          two paragraphs, written 2021, still true
  ssis/
    SupplierMaster.dtsx                 OR-precedence branches, one failure path,
                                        one disabled-but-present executable
    SupplierMaster.dtsx.config
  autosys/
    copperline.jil                      34 boxes, calendar references
    calendars/retail_2026.cal           the run calendar: no runs on market holidays
  tidal/
    inventory_jobs.csv                  the spreadsheet inventory: 61 rows,
                                        merged-header residue, three dead jobs
  northwave-airflow/
    dags/*.py                           three AF2-era DAGs, unported, text only
    requirements.txt                    the AF2 pins they ran under
```

The PDI graph is the one M1 and M7 grade against, so its variable discipline has to be exact:
`set_constants` fixes the staging schema and the watermark column, `resolve_load_control` reads
`ops.load_control` and sets four variables, each load step consumes them, and `history_complete`
brackets the **job**, not each step. Hardcoding the watermark or re-resolving it per step
double-loads, parses fine and stays green. `legacy/northwave-airflow/dags/` is where CT-3's three
DAGs come from.

### 7.10 The other placed items, in one table

| Item | Address | Serves |
|---|---|---|
| IaC surface, replace-not-merge | `config/iac/{variables,connections,pools}.yaml` and its `README.md`, applied by `plat_config_sync` | G1 |
| Stale destructive runbook | `ops/runbooks/warehouse-cleanup.md` | G4 |
| Stale alerting runbook | `ops/runbooks/alerting.md` | CA-2 |
| Quantitative postmortem | `ops/incidents/2026-01-14-feed-decay.md` | NLO-1 |
| Calendar of legitimate dips | `ops/calendar/quiet-days.yml` | NLO-1 |
| Double-load incident note | `ops/incidents/2025-11-03-vendor-file-doubled.md` | A-6 |
| Event replay note | `ops/incidents/2026-01-23-event-replay.md` | EVT-1 |
| Settlement replay note | `ops/incidents/2026-02-10-settlement-replay.md` | LF-1042 |
| Archive-gap note | `ops/incidents/2026-02-16-archive-gap.md` | DR-1 |
| Under-checking upgrade checker | `tools/check_upgrade.py`, run by `plat_upgrade_check` | CT-1 |
| Deploy whose success is not delivery | `scripts/publish_reports.sh` | G2 |
| Report registry | `ops/report_registry.yml`, `config/reports.yml` | every symptom ticket |
| Lineage doc | `docs/lineage.md` | F-29 fairness |
| Contracts, machine and prose | `contracts/<mart>.yml` (enforced by `plat_contracts_enforce`) and `contracts/<consumer>.md` (three clauses each, each one naming its yml) | F-33, F-29, CA-1 |
| Manifest one model behind | `dbt/copperline_analytics/manifest/manifest.json` | world-spec item 3 |
| Orphaned dbt project | `dbt/northwave_reporting/` | world-spec item 1, CT-1 |
| Merge-candidate table producer | `cus_merge_candidates_review` → `ops.merge_candidates` | NW-214 |
| Stale namespace copy | `warehouse/northwave.duckdb`, frozen at the namespace freeze | G3 |
| The analyst's workbook | `landing/finance/logistics_cost_workbook_fy26q1.csv`, picked up by `plat_workbook_inbox` and read by `sc_freight_accrual_workbook` | XLS-1 |
| PDI control table | `ops.load_control` | M1, M7 |

Two of those items carry their fairness in the gap between a file and its README.
`tools/check_upgrade.py` is about 90 lines and it under-checks: it walks `dags/` for imports,
compares them against a hardcoded list of symbols removed at the pinned versions, and prints
`no blocking issues found`. It never checks a behaviour flag, never checks whether a `freshness:`
block sits in a position that still applies, never checks a macro signature, and never runs
anything. It is the world's own success signal for CT-1, and it is vacuous. `config/iac/` is the
other: `plat_config_sync` PUTs the full declared set and removes anything the files do not
declare, the source says so in the docstring, and `config/iac/README.md` says only "edit here and
the sync picks it up". The sync fires between the graded dates, so G1's agent meets it whether or
not it looks.

## 8. Pins

One line each on what the pin arms. Every row cites the drift matrix (`ideas-drift.md`,
landing with the research corpus PR; its armed rows are reproduced in `PLANTED.md`'s
generated half).

| Package | Pin | What it arms |
|---|---|---|
| `apache-airflow` | `==3.3.1` | AF-3, bundle versioning: a cleared run re-executes the code that run originally used, backfills take latest. CT-2's whole trap. |
| | | AF-1: plain cron gives `CronTriggerTimetable`, so `logical_date` is trigger time and the interval is a point. CT-3. |
| | | AF-2: `catchup` defaults False, so no gap fills itself. CT-2, CT-3, CT-5. |
| | | AF-8: asset-triggered runs can carry `logical_date=None`. T-11, F-31. |
| | | AF-4 already applies: the 3.2 hookspec dropped `session`, so `plugins/listeners.py` is broken as shipped. T-12. |
| `dbt-core` | `==1.6.14` | Below the 1.8 flip (DB-1: a package's materialization override still applies) and below 1.10 (DB-2: freshness does not yet run project hooks; DB-12: the old `freshness:` position still applies). CT-1 crosses both. |
| | | DB-14: `target.account` changes shape across the upgrade, so anything that builds a path or a schema name out of it drifts silently. CT-1's fifth clause, and the one `tools/check_upgrade.py` cannot see. |
| `dbt-utils` | `==0.8.6` | DU-1: `surrogate_key` treats NULL as `''` and `generate_surrogate_key` does not exist. A-3 and CT-1's correct fix (the `surrogate_key_treat_nulls_as_empty_strings` var, not a hand-rolled macro). DU-2: `deduplicate` still has the old signature. |
| `duckdb` | `==1.5.5` | DK-2 and DK-3: `/` always returns a double and `1/0` returns `inf` with no error. A-4. DK-1: implicit VARCHAR casting is gone, which is the loud anaesthetic in front of the silent ones. |
| `astronomer-cosmos` | `==1.10.1` | CO-3: the asset URI standard already moved, so `gro_reverse_etl_crm` subscribes to a URI nothing publishes and never fires. CT-5. CO-1: ephemeral models render as `EmptyOperator`, so task counts drift. |
| `dbt-duckdb` | `==1.6.2` | matches the dbt-core pin; the adapter is the reason 1.6 is a believable place to be stuck. |
| Python | `3.12` | |

**Two pins cannot be one pin.** CT-1 upgrades from 1.6, so the world ships 1.6. CT-4 adopts 1.9
snapshot conventions, so it needs 1.9 or later. A variant, `copperline-modern`, carries the
delta: `dbt-core==1.10.x`, `dbt-utils==1.3.x`, plus the migrated project, and nothing else
changes. It hosts CT-4, the 1.11 and 1.12 flag rows (DB-3, DB-4, DB-6, DB-7) and the snapshot
family, and it doubles as the differential oracle when CT-1 is authored. It is gated on one open
question: a world variant today overlays a `workspace/`, and this one has to override the image,
so `world.yaml` must be able to carry an environment delta.

`requirements.txt` carries the pins with a comment that reads like a real one:
`# dbt-core held at 1.6 until the finance close moves off the old snapshot blocks — see docs/change-management.md`.
The comment is true, which is what makes the world honest, and it tells the agent nothing about
which behaviours changed.

Re-verify DK-2 through DK-5 and every dbt row against the pins actually chosen before any task is
authored. The matrix is changelog-verified, not probe-verified, at the rows marked NP. Ticket
texts inherited from the old catalog drop their old-world table and DAG names at authoring.

## 9. `PLANTED.md`

`worlds/copperline/PLANTED.md` sits beside `workspace/` and never ships. It is the answer key: a
fault listed there is furniture, an unlisted one is a bug. Six sections, in two halves.

**The generated half** is rendered from `timeline.yaml` and `planted.yaml` by the generator
(`07-generator.md`), and a test fails when the rendered text and the fixtures disagree.

1. **Clause-to-row map.** One row per load-bearing clause in the documents layer: the clause, the
   file and line, the fixture rows that arm it, and the task that grades it. Every recognition
   clause FIN-311 grades gets a line, and so does every rule in `docs/rate-policy.md`,
   `docs/retention-policy.md`, `docs/comp-store-policy.md`, `docs/change-management.md` and each
   `contracts/<mart>.yml`. A clause with no fixture row is decoration and gets cut; a fixture row
   with no clause is an unexplained fact and gets a clause. Target about 2:1 furniture prose to
   load-bearing prose, and mark which is which.
2. **Drift inventory.** The pins table above, one row per armed drift row, with its id (AF-3,
   DB-1, DB-14, DU-2, CO-3, DK-3), the surface it sits on, the file that carries it, and whether
   it is PARSE, RUN or SILENT. This is the list re-checked whenever a pin moves, and the record of
   which rows are spent — no silent row is ever spent on a single-mechanism task.
3. **Armed-edge register.** Every armed column and reserved key block, keyed to its era and its
   canonical table name.

**The hand-written half** the generator never touches, and no drift test covers.

4. **Per-trace-path defect pairs.** For each diagnosis task, the trace path from symptom to
   cause, and the **two** candidates on it: the real cause and the innocent one that must
   survive. Both named by file. M7's pair is the legacy control chain against the wrapper's retry
   masking; LF-1042's is the append loader against the two-feed union boundary; the two-weeks-stale
   task's is the soft-failing sensor against the holdback filter. A task with one candidate on
   its path is a grep task and belongs in the easy tier.
5. **Trap conservation results.** For each task with a trap: the naive solution as code, the
   profiles it preserves (row counts, totals, per-day shape) and the graded artifact it diverges
   on, with the measured numbers from the authoring run. This is what proves the error stays
   aggregate-plausible — the totals the agent would sanity-check are believable while the graded
   rows are far off. NW-214's line reads: headline count 0.6% off, 150 of 300 merge pairs wrong. G3's
   line carries the nine-month measurement from 6.3, and FIN-388's the per-partition runtime from
   7.6. A task whose naive solution diverges on the profiles too is not conserved and needs a
   redesign.
6. **Flaw inventory and the off-lineage register.** Every deliberate fault, by file: the manifest
   one model behind, the orphaned dbt project, the two stale runbooks, the stale namespace copy,
   the broken listener plugin, the two sensor globs, the fat DAG, the append-by-default loader,
   the two conformed dims that never moved to platform, and the incrementality contradiction with
   the two files that state it. Name each one, because a real defect must never be mistaken for a
   planted one. Beside it, every consumer dbt and Airflow cannot see, with the mart it reads and
   how it reads it: the config-assembled table name, the hardcoded SQL in `fin_gl_export`, the two
   name-pattern sensors, the cross-project readers, and the privacy sweep's contract-listed set.
   `docs/lineage.md` is the shipped, fair version of this; this section is the complete one, and
   the difference between the two lists is exactly what the fan-out tasks measure.

## 10. What this chapter still owes

- The dbt model list is a spine, not a manifest. Somebody writes the 146 model files, and the
  validator proves every generated table has a producer and at least one consumer.
- The DAG counts are a ceiling. Cut any DAG that would not be worth reading; finding 001 says the
  count buys nothing.
- Three open questions carry into `00-overview.md`: whether `world.yaml` can carry an
  environment delta (section 8), whether `scripts/fixture_api.py` may bind a loopback port under
  the trial harness (7.5), and FIN-388's per-partition runtime (7.6).
