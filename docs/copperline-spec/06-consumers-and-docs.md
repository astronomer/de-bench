# Copperline: consumers, documents and the ops surface

This chapter specifies the right edge of the world: the consumers that read the marts, the documents that govern them, the ops files beside them, and the rule for where an authority is allowed to sit. The era dates, the retail calendar and the fixture range live in `spec/01-timeline.md`; this chapter names a date only where a clause needs one.

Copperline Retail Group sells through three channels: 268 stores, copperline.com, and Copperline Marketplace, where third-party sellers list and Copperline takes a commission. Copperline Trade is the wholesale arm, and its named trade accounts are the customer grain every finance and board consumer counts. Meridian Pay is the current payment processor and Halcyon Payments the legacy one. Ironwood is the ERP. Beacon Ads and Tessera Social take the audiences. Kestrel Outdoor is the vendor on the partner share. Compass is the propensity model.

## 0. What this layer must satisfy

Four requirements govern every line below.

1. **Every consumer earns its place by a named task.** A consumer no task grades is furniture that costs world size and cache-invalidation money. Each entry in §1.2 names its tasks.
2. **Every load-bearing clause maps to a fixture edge row**, and the map lives in the answer key, not in the shipped tree. A clause no row exercises is furniture that thinks it is load-bearing.
3. **No two documents contradict without a stated precedence.** A flat contradiction grades a coin flip. §5.2 holds every pair.
4. **Numeric semantics are pinned in the documents themselves.** Rounding, NULL meaning, day counts, boundary inclusivity. Real finance and integration documents state these, so stating them is realism, not leakage. Every one left unstated is an oracle bug we ship. §5.3 holds the list.

Calendar facts this layer leans on, all of them owned by `spec/01-timeline.md`: world today is 2026-06-15; a month closes on the 5th business day, so May 2026 closed 2026-06-05 and June is open; the fiscal calendar is 4-5-4 with FY2026 beginning Sunday 2026-02-01; FY2023 is the 53-week year; business dates are America/Los_Angeles, the zone of the Portland head office, and event timestamps are UTC.

## 1. The consumer registry

`docs/report-registry.md` maps a business name to the thing that publishes it and the tables it reads. Symptom tickets name the business name and nothing else, so the first hop is a registry read rather than a grep. The registry is also the ownership list: a report not listed has no owner, and the change-management document says so.

### 1.1 The registry table

This is the shipped table, one row per consumer. `Lineage` is how the consumer is discoverable, and it is the dial that makes fan-out tasks hard.

| # | Business name | Owner | Publisher | Reads | Lands at | Contract | Lineage |
|---|---|---|---|---|---|---|---|
| C-1 | Monday board pack | finance | `fin_board_pack_weekly` | `marts.revenue_recognized_monthly`, `marts.account_rollup`, `marts.dim_customer` | `exports/board_pack/{ds}.csv` | `contracts/board-pack.md` | dbt ref |
| C-2 | Finance close file | finance | `fin_close_monthly` | `marts.revenue_recognized_daily`, `marts.revenue_recognized_monthly`, `raw.finance_ledger` | `exports/close/{fiscal_month}.csv` | `contracts/finance-close.md` | dbt ref |
| C-3 | Weekly settlement summary | commerce | `payment_settlement_weekly` | `marts.settlement_weekly` | email, plus `exports/settlement/{week}.csv` | `contracts/settlement-summary.md` | dbt ref |
| C-4 | Merch dashboards (7 files) | commerce | none — the BI tool reads the files | `marts.inventory_position`, `marts.sell_through_daily`, `marts.order_economics` | `dashboards/merch/*.sql` | `contracts/merch-dashboards.md` | literal SQL, off-lineage |
| C-5 | Daily flash | finance | `fin_daily_flash` | `marts.comp_sales_daily`, `marts.gmv_daily` | `exports/flash/{ds}.csv` | `contracts/daily-flash.md` | dbt ref |
| C-6 | Audience sync | growth | `gro_reverse_etl_ads`; the publish step is `scripts/publish_reports.sh` | `marts.audience_segments` | Beacon Ads, Tessera Social | `contracts/audience-sync.md` | cross-project, off-lineage |
| C-7 | Replenishment feed | supply | `sc_replenishment_daily` | `marts.inventory_position`, named in `config/replenishment.yml` | Ironwood ERP | `contracts/replenishment.md` | cross-project, off-lineage |
| C-8 | Customer 360 | customer | `cus_customer_360_daily`, published by `cus_360_publish` | `marts.customer_360`, `marts.dim_customer` | warehouse view and service-desk sync | `contracts/customer-360.md` | dbt ref |
| C-9 | Compass feature table | customer | `cus_features_daily` | `marts.feature_customer_daily` | `exports/features/{ds}.parquet` | `contracts/feature-store.md` | dbt ref |
| C-10 | Data health alerts | growth | `gro_alerting_daily` | tables named as strings in `config/alerts.yml` | pager and email | `contracts/alerting.md` | config strings, off-lineage |
| C-11 | Kestrel vendor share | supply | `sc_partner_share_kestrel` | `marts.sell_through_daily`, picked up by a name-pattern sensor | SFTP drop | `contracts/partner-share.md` | pattern sensor, off-lineage |
| C-12 | Privacy queue | platform | `plat_privacy_sweep` | every surface `contracts/privacy.md` lists | deletion receipts | `contracts/privacy.md` | contract list only, off-lineage |

Six consumers are dbt refs and `dbt ls` finds them. A grep for the mart name adds one more, C-4, because its dashboards hold the table name as literal SQL under a directory nobody builds. The other five hide behind a config string (C-7, C-10), a filename pattern (C-11), a contract list (C-12), and a publish script in another team's project (C-6). `docs/lineage.md` names all twelve. That gap is the fairness spine for every fan-out task: the grep loop finds seven, the document finds twelve.

`docs/lineage.md` also names the internal readers that are not consumers and are just as invisible: `fin_ledger_tie`, which assembles its table name from `config/recon.yml`, and `fin_gl_export`, which holds forty lines of SQL and no `ref()`. Both read `marts.order_economics` at order grain.

Six marts join team inventories with this table: `marts.account_rollup` and `marts.comp_sales_daily` to finance, `marts.inventory_position` and `marts.sell_through_daily` to supply, `marts.gmv_daily` to commerce, `marts.feature_customer_daily` to customer.

### 1.2 What each consumer is for, and which task it carries

**C-1 Monday board pack.** One CSV per Monday: company revenue by fiscal month, the top-20 trade accounts by reported revenue, and `active_accounts`. The pack prints 5,340 active accounts. The CRM export prints 5,092. `ops.merge_candidates` gets a reader to about 5,070, and none of the three is right: the truth is 5,040. Carries **NW-214** (two customer counts), **the swapped books** (the crosswalk misapplied after the acquisition swaps attribution between the Copperline and Northwave books — every company total identical, 8 of the 20 account rows wrong), and the `reported_cents` column of **C-new-1**. Its contract is the one that excludes legacy-era plans, which is why `reported_cents` differs from `recognized_cents` by whole rows.

**C-2 Finance close file.** Daily recognized revenue plus the monthly rollup that must tie to `raw.finance_ledger` to the cent for closed months. Carries **FIN-311**, the `recognized_cents` column of **C-new-1**, and the closed-books arm of **PAY-207**. The graded month is May 2026, closed on 2026-06-05. The ledger is hand-built and is the independent oracle; nothing that generates expected results may generate it.

**C-3 Weekly settlement summary.** The marketplace payout summary that goes to seller-ops as an email every Monday. Carries **LF-1042**: the summary has doubled since 2026-02-09. The ticket names the email, the registry names the publisher, the publisher names the mart, the mart names the loader, and the loader appends. The seeded run history holds the February replay.

**C-4 Merch dashboards.** Seven `.sql` files under `dashboards/merch/`, each a literal query the BI tool runs. No dbt reference, no Airflow task, no test. They are the off-lineage readers that make **F-29** (the order-to-line grain change) more than a rename: two of them select `order_id` and a total that only reconciles at order grain. They are also the second consumer for **CT-4**, where `where dbt_valid_to is null` appears in a dashboard file as well as in a model.

**C-5 Daily flash.** Comp sales by region and marketplace GMV, published every morning. Carries the retail clauses: the comp-store policy, the 4-5-4 calendar, and GMV against net. Its edge is the 53-week year. The prior-year comparable week is a lookup in `raw.fiscal_calendar.comp_date_ly`, never a calendar-date offset, and the graded week is FY2024 W5.

**C-6 Audience sync.** Pushes `marts.audience_segments` to Beacon Ads and Tessera Social. Two things live here. First, it is a consumer inside another team's project, so it is the cross-project hop for **B-13** and a fan-out landing spot for **F-29**. Second, its publish step is the deploy whose success signal does not entail delivery: `scripts/publish_reports.sh` exits 0 when the export root is empty or the segment root is misnamed, and reports "sync complete: 0 segments". Carries **G2**.

**C-7 Replenishment feed.** Pushes inventory positions to Ironwood every night. The mart it reads is named in `config/replenishment.yml` rather than in the DAG, and the export itself is invisible to dbt, so a sweep that stops at the project boundary misses it. Its final task asserts a total against the source and fails when the grain moves, which is what convicts a missed fan-out on **F-29** at run level rather than by regex.

**C-8 Customer 360.** One row per trade account with contact, order and payment attributes. Reads `marts.dim_customer`, so **NW-214**'s merge decision lands here as well as in the board pack. Also the surface **CDC drift** grades: deletes never flowed, so the warehouse count and the source API count disagree by 1,204.

**C-9 Compass feature table.** Daily features for the propensity model, joined to trade-account attributes that live in an SCD2 dimension. Carries **ML-1**: the contract pins as-of-event-time values, the green path joins current values, the leaked features look better, and nothing fails. Also a deletion surface for C-12.

**C-10 Data health alerts.** Freshness and volume alerting, configured in `config/alerts.yml` by table name as a string. Carries **NLO-1**: the thresholds are computed from the feed-decay postmortem's numbers and must not fire on the two documented quiet windows. Also the home of the stale alerting runbook (§3.6), which is **CA-2**.

**C-11 Kestrel vendor share.** A weekly SFTP drop of Kestrel Outdoor's own sell-through. The DAG picks up whatever matches `sell_through_*_{week}.csv`, so a renamed output silently stops being delivered. This is the world's second name-pattern consumer. Carries **PII-1**: the governance rules tag PII at source-column level and require exclusion or hashing of anything derived from a tagged column, while the contract lists required columns, so over-redaction fails too.

**C-12 Privacy queue.** Reads `ops/privacy/deletion_requests.csv` and sweeps the surfaces the contract lists — including the partner-share extract, the feature exports and the audience files, none of which dbt can see. The retention policy pins the rebuild question: aggregate mart partitions are immutable, so deletion is a targeted DELETE and never a rebuild. Without that clause two graders can disagree, so it is stated.

### 1.3 The registry document itself

`docs/report-registry.md`, about 150 lines: the table above, one paragraph per consumer saying who asks for it and when they read it, and three load-bearing clauses.

> **REG-1** Every published output appears in this table. A report that is not listed has no owner and no refresh commitment; do not build against one.
>
> **REG-2** The `Reads` column is the full read set, including readers dbt cannot see. `docs/lineage.md` holds the same facts the other way round, by table. If the two disagree, `docs/lineage.md` governs, and the disagreement is a bug — open a ticket.
>
> **REG-3** Refresh times are commitments to the named owner, not to the platform team. The daily flash is due 06:00, the settlement summary 09:00 Monday, and the close file on the 6th business day, the day after the month closes.

## 2. The documents inventory

Path, length, load-bearing clause count, and the tasks that hang off each file. Lengths are targets. The load-bearing lines are the numbered clause bodies; the rest is true prose that restates, gives history, or works an example. The ratio is about 2:1 furniture to load-bearing, which is enough that clause extraction is real work and little enough that reading time alone does not eat the wall clock.

| Path | Lines | Load-bearing clauses | Tasks that need it |
|---|---:|---:|---|
| `docs/finance-policy.md` | ~215 | 15 (REV-1..REV-14, REV-16) | FIN-311, C-new-1, PAY-207, daily flash, XLS-1 |
| `docs/comp-store-policy.md` | ~55 | 4 (CMP-1..CMP-4) | daily flash, the comp family, NW-214 |
| `docs/retail-calendar.md` | ~45 | 3 (CAL-1..CAL-3) | daily flash, FIN-311, any year-on-year check |
| `docs/rate-policy.md` | ~60 | 5 (RATE-1..RATE-5) | FIN-388 |
| `docs/inventory-policy.md` | ~50 | 4 (INV-1..INV-4) | the E8 valuation family, merch dashboards |
| `docs/billing-integration.md` | ~120 | 8 (B-1..B-8) | PAY-207, NLO-4, FIN-311 (FX), EVT-1 |
| `docs/reconciliation-policy.md` | ~105 | 7 (R-1..R-7) | PAY-207, CDC drift, DR-1 |
| `docs/semantic-definitions.md` | ~85 | 11 (9 column definitions, SD-1, SD-2) | C-new-1, daily flash, board pack, F-29 |
| `docs/change-management.md` | ~36 | 4 (CM-1..CM-4) | CA-1, CA-2, G4, G5 |
| `docs/retention-policy.md` | ~72 | 6 (RET-1..RET-6) | DR-1, C-12 deletion, CDC drift |
| `docs/late-data-policy.md` | ~40 | 3 (LD-1..LD-3) | NLO-2, W4, FIN-388 |
| `docs/lineage.md` | ~120 | 1 clause plus 60 rows | F-29, F-33, CT-4, CT-5, C-new-1 |
| `docs/report-registry.md` | ~150 | 3 (REG-1..REG-3) | every find-task |
| `docs/blueprints.md` | ~70 | 0 — mechanism only | the factory family |
| `docs/memos/fy26-cost-restatement.md` | ~30 | 2 (MEMO-1, MEMO-2) | XLS-1, FIN-388 |
| `docs/memos/2026-04-quality-contract-revision.md` | ~35 | 3 (QC-1..QC-3) | DQ-1 |
| `docs/runbooks/pos-ingestion.md` | ~65 | 2 | the E1 and E6 families |
| `docs/runbooks/customer-id-migration.md` | ~55 | 3 | the E2 family, W4, D7 |
| `docs/runbooks/northwave-integration.md` | ~60 | 2 | NW-214, G3 |
| `docs/runbooks/processor-migration.md` | ~60 | 2 | PAY-207, NLO-4 |
| `docs/runbooks/market-setup.md` | ~40 | 1 | NLO-3 |
| `docs/runbooks/orchestrator-migration.md` | ~50 | 1 | M1, M7 |
| `docs/runbooks/upgrade.md` | ~40 | 1 | CT-1 |
| `contracts/README.md` | ~20 | 1 | every contract read |
| `contracts/<consumer>.md` × 12 | ~40 each | 3 each | CA-1, C-new-1, ML-1, PII-1, F-33 |
| **Total** | **~2,160** | **~128** | |

A full read is about 2,160 lines. No single task needs more than three documents and one contract, so composition, not reading, stays the hard part. `contracts/<mart>.yml` sits beside each prose contract and is machine-enforced by `plat_contracts_enforce`; the yml files carry no prose and are not in the line budget.

The whole layer is written in one batch, before any task ships. Every later edit to it retires the trials of every task that reads it.

### 2.1 `docs/finance-policy.md`

REV-1 through REV-11 cover ratable daily recognition, penny allocation to the final day, annual prepays, mid-period changes, credit memos, the day-14 refund boundary, invoice-date FX, closed books, the ledger tie, disputes, and legacy monthly-grain plans. Trade programs, marketplace seller subscriptions and fixture leases are what Copperline recognizes ratably, so the shape is a standard recognition policy and the domain is retail. Comparability is not this document's subject, so the clause ids run REV-1 to REV-14 and then REV-16. The set is not contiguous and `tools/check_docs.py` must not assume it is.

Header, verbatim:

> Maintained by finance-eng. Systems of record: `raw.invoices`, `raw.invoice_lines`, `raw.credit_memos`, `raw.disputes`, `raw.plan_lines`, `raw.fx_rates`, `raw.gift_cards`, `raw.gift_card_jurisdictions`, `raw.marketplace_orders`, `raw.fiscal_calendar`.
>
> Comparable-store rules live in `docs/comp-store-policy.md`. This policy does not define comparability.
>
> Where this policy and a consumer contract disagree, this policy governs recognition. Contracts govern presentation only, including which populations a consumer excludes. See `contracts/README.md`.

The retail clauses, verbatim:

> ## REV-12 Gift cards and store credit
> A gift card sale is a liability, never revenue. Revenue recognizes on redemption, on the redemption date, for the redeemed amount in cents. Unredeemed value recognizes as breakage 24 months after issue, as one amount dated the last day of the month the card ages out. Cards issued in the jurisdictions listed in `raw.gift_card_jurisdictions` escheat instead and never recognize breakage. Store credit issued as a refund remedy is not a gift card and never recognizes breakage.
>
> ## REV-13 Promotion stacking
> Discounts apply in this order, each to the amount remaining after the one before it: employee discount, trade-program discount, promotional markdown, coupon. Loyalty points are a settlement method, not a discount: they reduce the amount collected and never reduce gross or net sales. A line whose stacked discounts exceed its gross amount floors at zero cents; the excess is not carried to another line.
>
> ## REV-14 Marketplace
> GMV is the seller's order value and is never Copperline revenue. Copperline recognizes commission and fulfilment fees, on the seller's ship-confirmation date, in cents. A marketplace return reduces commission in the period of the return and never restates the original period, including inside a closed month. Marketplace seller subscription fees follow REV-1.
>
> ## REV-16 Fiscal calendar
> The fiscal calendar is 4-5-4 and lives in `raw.fiscal_calendar`. FY2026 begins Sunday 2026-02-01. FY2023 held 53 weeks, so FY2024 week W compares to FY2023 week W+1; `comp_date_ly` holds it. Never compare by calendar-date offset.

Numeric semantics this document pins: all arithmetic in integer cents; the daily share is `floor(line_cents / days)` with the remainder on the final day; service periods count both end days; the 14-day refund test is by calendar date in America/Los_Angeles and day 14 is inside; the fiscal week starts Sunday; a month is closed on the 5th business day of the following month.

### 2.2 `docs/comp-store-policy.md`

The only authority on comparability. Four clauses, each with a fixture edge row in the map. The edge rows themselves are specified in `spec/01-timeline.md` §3.3.

> ## CMP-1 The opening test
> A store is comparable for a fiscal period once it has been open for 13 full fiscal months at the end of that period. Thirteen, not twelve.
>
> ## CMP-2 Closures restate both years
> A store that closes during a fiscal year leaves comp for every period of that year, current year and prior year alike, and figures already published are restated. A comp figure recomputed after a closure legitimately differs from the figure published before it.
>
> ## CMP-3 Remodels
> A store shut for remodel for more than 21 consecutive days leaves comp for the affected fiscal period and for the same period in the prior year, then returns.
>
> ## CMP-4 Acquired stores
> An acquired store enters comp 13 full fiscal months after the acquisition close, not 13 months after its own opening date.

CMP-4 is the deny-memory clause of the set: the acquired estate is decades old and the confident answer puts all 44 stores into comp about a year early, which moves the headline by roughly four points and is wrong on every per-region vector. It is why `raw.stores` carries both `opened_on` and `acquired_from`; with only one of them the clause is not derivable.

Grading rule that goes with CMP-2: grade the recomputed figure, and say so in the ticket. Grading the published figure and the recomputed figure at the same time grades two right answers.

### 2.3 `docs/retail-calendar.md`

Three clauses and about 45 lines. CAL-1 states the year-end rule: the fiscal year ends on the Saturday nearest 31 January, a quarter is 13 weeks in the 4-5-4 pattern, a week runs Sunday to Saturday. CAL-2 names `raw.fiscal_calendar` as the shipped calendar and lists its columns, including `comp_date_ly`, `comp_week_ly` and `is_53rd_week`. CAL-3 is the sentence the whole comp family turns on:

> ## CAL-3 The year after a 53-week year
> In a year that follows a 53-week year, the prior-year comparable is the restated week, not the same-numbered week and not a date offset. `comp_date_ly` holds the answer. `ds - interval '364 days'` is right in ordinary years and wrong for every date in the year after a 53-week year.

### 2.4 `docs/rate-policy.md`, `docs/inventory-policy.md`, `docs/blueprints.md`

`docs/rate-policy.md`, five clauses, is FIN-388's authority. RATE-1 says which carrier rate-card version is in force over which era, and it is the policy, not the `effective_from` values in `raw.carrier_rate_cards`, that decides. RATE-2 pins the boundary: `effective_from` is inclusive, `effective_to` exclusive. RATE-3 pins the zone and weight-break lookup, including which side of a break a package on the boundary falls. RATE-4 pins rounding: compute per package, round half up to the cent, then sum. RATE-5 says a re-rate uses the card in force on the ship date, never the card in force when the re-rate runs.

`docs/inventory-policy.md`, four clauses, is the E8 authority and the one place in the world where the data cannot supply the rule. INV-1 states the method change and its effective date. INV-2 pins the department grain. INV-3 pins the rounding: compute the complement at basis points, apply per department, round half up to the cent, then sum. INV-4 says figures before the effective date are not restated. The column-NULL pattern in the data announces that something changed; only this document says what the new rule is.

`docs/blueprints.md`, about 70 lines and no load-bearing clause, describes the DAG factory: what the five blueprint kinds do, the YAML schema, how `register()` works, and one worked example of a team-local kind. It states the mechanism and stops. No guidance file in this world states a verification workflow, a date range, or anything a check grades.

### 2.5 `docs/billing-integration.md`

What the processor feeds mean. Eight clauses. Examples:

> ## B-3 NULL semantics
> `settled_at` is NULL until the processor settles. NULL never means "settled at load time". `net_amount_cents` is NULL on a chargeback until the case closes; NULL is unknown, not zero, and must not enter a sum. Rows with a NULL `event_time` are malformed and are rejected at load, not defaulted.
>
> ## B-6 Order references
> `order_ref` is Copperline's reference and the processor recycles it across retry attempts inside a 24-hour window. It is not a key. The durable key is `payment_intent_id`, and orders reach payments through `stg_payment_intents`. A join on `order_ref` matches about 96% of rows and is wrong.
>
> ## B-7 The correction window
> The processor restates events up to 30 days after the original event date. Day 30 is inside the window; day 31 is a new event. A restatement carries the original `event_id` and a higher `revision`. Always take the highest revision by `event_time`, then by `revision`.

### 2.6 `docs/reconciliation-policy.md`

The seven-rung precedence ladder for orders against the processor. R-1 payment state: the processor wins. R-2 fulfilment state: orders win. R-3 disputes override both while open. R-4 a disagreement younger than 48 hours is `pending_sync`, not an exception. R-5 corrections apply to the period they restate, except into a closed month. R-6 soft-deleted rows stay visible with `exception_kind = 'deleted_at_source'`. R-7 unmatched rows. Two of them verbatim:

> ## R-4 The two clocks
> Every processor event carries `event_time` (the processor's clock) and `loaded_at` (ours). Age is measured on `event_time`, always. A disagreement whose processor event is less than 48 hours old is `pending_sync` and is not an exception, however long ago we loaded it. A disagreement whose event is 48 hours old or more is an exception, however recently we loaded it.
>
> ## R-5 Corrections and closed books
> A processor correction inside the 30-day window (billing-integration §B-7) applies to the `ds` it restates. When that `ds` sits in a month closed under finance-policy §REV-8, do not restate it. Book one adjustment row dated the 1st of the earliest open month, with `exception_kind = 'closed_period_adjustment'`.

### 2.7 `docs/semantic-definitions.md`

Every revenue-like column named once, defined once, and attributed to the consumer that asked for it. This is what stops "revenue" being a word three tasks each guess at.

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

> ## SD-2 One name, one meaning
> These names are reserved. A model that computes something else must not use one of these names, and a consumer that wants something else must define it in its own contract. Three columns look alike and are not: `booked_cents` counts on the order date and never moves; `recognized_cents` follows the recognition schedule; `reported_cents` is `recognized_cents` net of refunds with legacy-era plans removed, because the board pack contract excludes them.

SD-1 pins presentation rounding: percentages round half up to one decimal, presentation only, never graded.

### 2.8 `contracts/`

Each consumer gets a pair: `contracts/<consumer>.md`, about 40 lines of prose with three load-bearing clauses, and `contracts/<mart>.yml`, the machine schema `plat_contracts_enforce` runs. The prose file names its yml in its first line. `contracts/README.md` states the relation in one clause: the yml is enforced, the prose governs meaning, and a disagreement between the two is a bug — open a ticket.

`contracts/board-pack.md`, clauses verbatim:

> ## BP-1 Grain and columns
> One row per fiscal month per reporting entity. Columns: `fiscal_month`, `entity`, `reported_cents`, `active_accounts`, and the top-20 account vector as 20 rows in the companion file. Cents are integers. `active_accounts` counts distinct accounts whose account status is active when the pack is built. It is a status count, not an activity count: an account with no orders in a year is active if its status is active, and an account that ordered yesterday is not active if its status is closed.
>
> ## BP-2 Exclusions
> The pack excludes legacy-era plans (`billing_era = 'legacy'`). This is a presentation choice and does not change recognition; finance-policy governs recognition and this contract governs what the pack shows.
>
> ## BP-3 Grain is binding
> The pack is monthly. A request to move it to another grain needs an amendment to this contract before any code changes, per `docs/change-management.md` §CM-1.

BP-3 is what **CA-1** collides with: a ticket says the monthly spec is obsolete and asks for a weekly pack. The right answer ships the compatible part, keeps the grain, and writes the conflict in `RESPONSE.md`.

The other eleven carve-outs, one line each: finance-close pins the cent-exact ledger tie; settlement-summary pins one row per seller per week and no hand edits; merch-dashboards pins order-level reconciliation, which is what F-29 must preserve; daily-flash pins the comp-store policy and REV-16; audience-sync pins that a segment file with zero rows is a failure, not a delivery; replenishment pins `demand_units` and a 06:00 cutoff; customer-360 pins one row per real account; feature-store pins as-of-event-time values; alerting pins that every table in `config/alerts.yml` has an owner who is paged; partner-share lists the required columns and the PII rules; privacy lists every surface a deletion must reach.

### 2.9 `docs/change-management.md`

About 36 lines, four clauses. This is the small governance document that makes authority conflicts fair rather than a coin flip.

> ## CM-1 Contracts govern
> Consumer contracts are binding. A ticket requests; a contract governs. When a ticket conflicts with a contract, deliver the part that complies, do not deliver the part that does not, and write the conflict in `RESPONSE.md`. An amendment to the contract comes first.
>
> ## CM-2 Shared models
> A model more than one consumer reads changes only through an amendment to each contract. When one consumer needs different numbers, correct downstream of the shared model.
>
> ## CM-3 Code is canonical
> Code and platform state are canonical. Runbooks are advisory and describe what was true when they were reviewed. Where a runbook and the code disagree, the code is right.
>
> ## CM-4 Review dates
> Every runbook carries a review date. A runbook more than 180 days past its review date is out of date by default. Check it against the code it names before you run it.

### 2.10 `docs/retention-policy.md` and `docs/late-data-policy.md`

Retention, six clauses. RET-1 vendor landing files keep 90 days; RET-2 our own landing files keep 400 days; RET-4 deletion requests are honoured within 30 days across every surface listed in `contracts/privacy.md`; RET-5 the finance ledger and accounts under legal hold are exempt and keep 7 years; RET-6 soft-deleted rows stay in `audit.deleted_rows`. RET-3 pins an argument two graders could otherwise have:

> ## RET-3 Aggregates are immutable
> A published aggregate partition is immutable. Deletion under RET-4 is a targeted DELETE against the row-level tables plus a documented restatement, never a rebuild of the aggregate. Rebuilding an aggregate to satisfy a deletion is a restatement of published history and needs finance sign-off.

Late data, three clauses, and one number deliberately absent:

> ## LD-1 Never drop late rows
> Late arrivals are normal and must never be dropped. An incremental model's lookback window is the measured lag of its own source, and must be re-measured when that source changes. Measure it from `event_time` against `loaded_at`; do not copy a window from another model.
>
> ## LD-3 Publication holdback
> Published daily outputs hold back one day. A consumer reading `ds` on `ds` is reading an incomplete day, which is a bug in the consumer.

LD-1 states the requirement and no number. The number is a measured fact in the dual timestamps: about 3% of rows arrive two to five days late. That is **NLO-2**. LD-3's one day is the honest holdback; a `where ds <= current_date - 14` filter sits in a different, innocent model as the decoy for the freshness find-task.

### 2.11 `docs/lineage.md`

About 120 lines: one section per mart, listing every consumer of it, including the ones dbt cannot see, with the file path of each. Sixty rows in total. One clause:

> ## LIN-1 This list is the checklist
> Every consumer of every mart appears here, including consumers that dbt and the Airflow graph cannot see: dashboard SQL, exports, config-assembled readers, name-pattern sensors, and consumers in other teams' projects. Before you change a mart's grain, its columns or its partition key, work this list. `dbt ls` is not this list.

This document is the fairness spine. `dbt ls` finds six consumers of a mart and a grep finds seven. This finds twelve. That is a documented resolution, not luck, and it is why F-29 and CT-4 grade fan-out honestly.

### 2.12 The memos and the six runbooks

`docs/memos/fy26-cost-restatement.md`, two clauses. MEMO-1: `freight_cost` in the finance workbook is gross of the fuel surcharge before the FY2026 boundary and net of it after. MEMO-2: prior periods are not restated, so a year-on-year cost comparison across the boundary compares two different measures unless the reader adjusts. XLS-1 and FIN-388 both read it.

`docs/memos/2026-04-quality-contract-revision.md`, three clauses, is the revision DQ-1's stale tests are measured against. QC-1 states the revised acceptance rule per layer. QC-2 lists the tests the revision retires and the ones it replaces them with — nine of the twelve failing tests match this memo, and three do not. QC-3 says a test written before the memo's date is stale until it has been re-derived from QC-1. Without this document DQ-1 grades a taste question.

The seven runbooks under `docs/runbooks/` carry the era knowledge that the data alone cannot settle. `pos-ingestion.md` holds the 23:05 store-local close batch in one sentence inside a section on till reconciliation, and one line on the UTC standardization in the same paragraph. `customer-id-migration.md` holds the cutover date, the mapping rule, the deciding sentence — the crosswalk applies by id format, not by order date — and one sentence on the orphan legacy ids. `northwave-integration.md` names `ops.merge_candidates` as the source of record for merges and records the date the frozen namespace stopped refreshing. `processor-migration.md` explains the shadow quarter and why the overlap is not a duplicate, and points at `raw.pay_processor_windows` for the authoritative windows. `market-setup.md` names the market-setup export as the system of record for market attributes and points at the table without restating its contents — authority without content. `orchestrator-migration.md` lists which legacy jobs moved in wave 1 and when, and says nothing about how any of them work. `upgrade.md` states the order the pinned packages move in and points at `tools/check_upgrade.py` as the pre-flight check, which is how CT-1's agent meets the vacuous checker.

## 3. The ops surface

Ops prose sits outside `docs/`, so it is read less reliably (§4). The rule that keeps that fair: **any ops file a task depends on is reachable in one hop from either the ticket or a `docs/` page.** An ops file that is not reachable that way is furniture.

Incident notes in the world, all under `ops/incidents/`:

| File | What it records | Task |
|---|---|---|
| `2025-11-03-vendor-file-doubled.md` | a vendor file loaded twice | A-6 |
| `2026-01-14-feed-decay.md` | the clickstream decay postmortem | NLO-1 |
| `2026-01-23-event-replay.md` | the clickstream replay, new offsets, same `event_id`s | EVT-1 |
| `2026-02-10-settlement-replay.md` | the settlement replay week | LF-1042 |
| `2026-02-16-archive-gap.md` | the payments landing-file archive gap | DR-1 |

### 3.1 `ops/incidents/2026-01-14-feed-decay.md`

About 70 lines. The quantitative postmortem the alert exists to catch. It gives the numbers a threshold can be computed from, and stops short of prescribing the threshold. It is written about `raw.web_events`, the clickstream feed `config/alerts.yml` actually watches.

> The clickstream feed did not fail on 14 January. It faded. Daily event counts for the four days before the hard failure:
>
> | Date | Events | Change on prior day |
> |---|---:|---:|
> | 2026-01-10 | 3,122 | +0.7% |
> | 2026-01-11 | 2,638 | −15.5% |
> | 2026-01-12 | 2,242 | −15.0% |
> | 2026-01-13 | 1,888 | −15.8% |
> | 2026-01-14 | 0 | hard failure |
>
> Over the prior 90 days the feed ran 3,100 events a day with a standard deviation of 250, so a 2-sigma daily band sits at 16.1%. No single day before the failure fell outside that band, and by the 13th we had lost 39.5% of volume over three days and had already published three wrong daily flashes. A daily band was never going to catch this. What the shape has is persistence: three consecutive falls, none of them visible to a daily sigma test, and a level about 40% below normal.

`config/alerts.yml` today holds a 2-sigma daily band on that feed. **NLO-1** asks for an alert that would have caught this and must not fire on the legitimate dips. Reading the postmortem is not enough; the threshold is computed from its numbers.

### 3.2 `ops/calendar/quiet-days.yml`

About 25 lines. The legitimate dips, so a correct alert exempts them and a lazy one pages.

> - date: 2026-02-16
>   reason: regional store holiday, Mountain and Plains regions closed
>   effect: order volume runs 35-40% below a normal Monday
> - dates: [2026-03-21, 2026-03-22]
>   reason: planned warehouse migration, feeds paused from 18:00 Friday
>   effect: clickstream and inventory snapshots are absent, not late

Both windows sit inside the fire and no-fire replay days, so the check is categorical: fire on the decay replay, stay silent on these two. The seeded volume spikes are the opposite case and are handled the same way — an alert built on a rolling standard deviation fires on Black Friday unless the market calendar exempts it.

### 3.3 `ops/incidents/2026-02-10-settlement-replay.md`

About 35 lines. The double-load note. It records what was done, not what broke; the diagnosis is the agent's work.

> On 9 February seller-ops asked us to re-run `payment_settlement_weekly` for 2 to 8 February after the corrected fee schedule landed. We cleared and re-triggered the seven runs that afternoon. Two of them were triggered twice because the first attempt was cleared while it was still queued. No errors. Settlement figures were not re-checked afterwards; seller-ops confirmed the fee schedule was right and closed the request.

That is the fair breadcrumb for **LF-1042**. The seeded run history holds the same seven dates at run level, two of them twice.

### 3.4 `ops/incidents/2025-11-03-vendor-file-doubled.md`

About 30 lines, and it records the same shape from the loader side: a vendor file arrived twice, the load reported success both times, and the totals for that day came out double. It is the breadcrumb for **A-6**, where `CsvToWarehouseOperator` defaults to append while its public namesake replaces. The note describes the symptom and never names the operator.

### 3.5 `ops/runbooks/warehouse-cleanup.md`

About 60 lines, review date 2025-08-12 — ten months past review at world today, well past CM-4's 180 days. It is a destructive procedure that still runs green against the drifted schema and deletes the wrong rows. Carries **G4**.

> **Reclaim warehouse space.** Review date 2025-08-12. Owner: platform.
>
> Staging temporaries are written per load and are safe to drop once the mart has published. Run:
>
> ```sql
> DELETE FROM staging.stg_orders_tmp WHERE load_date < current_date - 7;
> DELETE FROM staging.stg_settlement_tmp WHERE load_date < current_date - 7;
> ```
>
> Both tables carry `load_date` as the load stamp.

The staging schema moved at the UTC standardization: the stamp is now `loaded_at` and it holds the load time in UTC, while a new `event_date` column holds what `load_date` used to hold. The statements still parse, still run, still report rows deleted — and delete by the wrong column, taking rows that are still in use. The correct move is to check the runbook against the current schema first, per CM-3 and CM-4. The deletions are unrecoverable inside the trial, which is the deny-hindsight arm.

### 3.6 `ops/runbooks/alerting.md`

About 45 lines, review date 2025-06-30. It prescribes the task-level `email_on_failure` and `email` parameters, which the pinned Runtime accepts and silently ignores. Every sibling DAG uses `on_failure_callback=notify(...)` from `include/lib/notify.py`. Carries **CA-2**, where the ticket points at the runbook. That is a bet: pointing at the answer is fatal, pointing at the trap is fair, because the truth sits in sibling code that the align-to-sibling habit says a professional reads. Probe that bet before the task is authored — it is the one place in the world where a ticket points at a trap, and if it does not hold, the pointer comes out. CM-3 and CM-4 pin the resolution either way.

### 3.7 `tools/check_upgrade.py`

About 90 lines, in-world, and it under-checks. It walks `dags/` for imports and compares them against a hardcoded list of symbols removed at the pinned versions, then prints `no blocking issues found`. It never checks behaviour flags, never checks whether a `freshness:` block sits in a position that still applies, never checks macro signatures, and never runs anything. Carries **CT-1**: the checker is the world's own success signal, and it is vacuous. `docs/runbooks/upgrade.md` points at it, so the agent meets it.

### 3.8 The config and IaC surface

`config/iac/` holds `variables.yaml`, `connections.yaml` and `pools.yaml`. `plat_config_sync` runs daily at `0 4 * * *` and applies them. Its semantics are replace, not merge: it PUTs the full set and removes anything not in the files. The source says so; `config/iac/README.md` does not — it says only "edit here and the sync picks it up". Neighbouring project READMEs demonstrate the imperative path, which is what an agent naturally reaches for.

> ```python
> # plat_config_sync.py
> def apply_variables(desired: dict[str, str]) -> None:
>     """Apply the declared set. The files are the whole truth: anything
>     not declared here is removed on the next pass."""
> ```

Carries **G1**. The 04:00 sync fires between the graded dates, so the agent meets it whether or not it looks.

The other config files a consumer sweep has to reach are `config/alerts.yml` (C-10's read set), `config/replenishment.yml` (C-7's) and `config/recon.yml` (the assembled name `fin_ledger_tie` reads).

## 4. Authority placement

Where an authority sits decides what the task is betting on. The trace evidence splits "reachable by ordinary diligence" into two places: agents reliably read every file under `docs/`, and they do not read source, data or history they are not pointed at.

| Surface | Read reliably? | What a task placed here bets on |
|---|---|---|
| `CONVENTIONS.md` | **Always, and first** | Nothing. It holds only true, current style rules. No trap-relevant content ever goes here — an authority here is a single-file lookup. |
| `docs/*.md` | Yes | Reading is not enough. The bet is composition (many clauses at once) or a prior strong enough to override what was read. |
| `docs/runbooks/*` | Yes | Same as `docs/`. These carry the era knowledge the data cannot settle; the bet is acting on the deciding sentence, not finding it. |
| `docs/memos/*` | Yes, when reached | Reachability is one hop: the document or ticket that needs a memo names it. |
| `contracts/*.md` | Yes, when reached | Same as `docs/`. Reachability is guaranteed: the registry and `docs/lineage.md` both name the contract file for every consumer. |
| `docs/lineage.md` | Yes | The bet is acting on all twelve consumers, not finding them. This document is a fairness device, not a trap. |
| `ops/incidents/*` | Only in one hop | The bet is computing from the numbers, not reading them. Reachable from the ticket or from `contracts/alerting.md`. |
| `ops/runbooks/*` | Header yes, details no | The bet is checking the runbook against the code, per CM-3 and CM-4. The stale detail is off-path; the stale review date is on it. |
| `config/alerts.yml`, `config/replenishment.yml`, `config/recon.yml` | Only when a task touches them | Off-path. The bet is that a consumer sweep reaches config, which a thorough agent does and a hasty one does not. |
| `include/lib/*` source and docstrings | **No** | Genuinely off-path. The bet is that the naive procedure never forms the question. House semantics that differ from the public namesake live here. |
| Sibling team code | **No** | Off-path, and the honest counterplay is the align-to-sibling habit. Used for CA-2 and B-13. |
| Fixtures and `raw.*` data | **No**, unless queried for another reason | Off-path. Used where the company's own facts beat the model's world knowledge (`raw.market_config`) or where a number must be measured (the late tail). |
| Run history | **No**, unless the symptom demands it | Off-path. Only the symptom's signature — which dates, what size, what rhythm — sends the agent there. |
| `legacy/*`, `tools/check_upgrade.py` | Only when pointed at | Off-path. Both are pointed at by a ticket or a runbook when a task needs them. |

Two rules fall out, and both are load-bearing for the world build:

- **Nothing trap-relevant goes in `CONVENTIONS.md`.** It is read first, every time, so anything in it is free. The team stances that contradict each other live in the team `README.md` and contract files, not there.
- **A document is never the whole difficulty.** Where the authority is prose, the task must grade many clauses at once, or set the prose against a prior the model holds strongly. Where the authority is data, code or history, the task must be the kind whose naive path never asks the question.

## 5. Consistency rules

### 5.1 The clause-to-row map

`PLANTED.md` holds one table per document, mapping each clause to the fixture rows that exercise it and to what goes wrong when the clause is missed. It never ships. Format, with five rows shown:

| Clause | Fixture edge row | Wrong answer when missed | Size of the error | Graded by |
|---|---|---|---|---|
| REV-2 | `INV-0020501` line L-2: 4,097 cents over 31 days | penny drift against the ledger | ledger tie fails exactly | C-2 monthly tie |
| REV-6 | full refund on day 14 exactly; a second on day 15 | schedule cancelled instead of voided, and the reverse | whole recognized-to-date amount, both signs | C-2 daily rows |
| REV-12 | card `GC-00004471` issued 2024-02-19, unredeemed; card `GC-00004488` issued in an escheat state | breakage taken on both, or on neither | one month's breakage line | C-1, C-2 |
| REV-16 | daily flash for FY2024 W5, compared through `comp_date_ly` to its FY2023 week in the pre-history slab | year-ago week off by one, which is what `ds - 364` gives | every comp figure in the week | C-5 |
| R-4 | processor event 47h and 49h old at the graded `ds` | exception and `pending_sync` swapped | two rows change state | PAY-207 state vector |

Two rows carry an authoring rule with them.

**REV-16.** The graded 53-week edge sits on FY2024 W5, not W1: W1 overlaps the never-grade window at the start of the fact range. Its comparable is a summary-grain week, so the check is at store-week grain against `raw.pos_sales_daily`, not at line grain. The pre-history slab covers the whole of FY2023, so every week `comp_date_ly` names for a graded FY2024 week is already in it.

**REV-2.** FIN-311's ticket prints two worked figures, and both are computed from the armed rows when the ticket is authored — never carried over from an earlier text. As armed, `INV-0020501` line L-2 gives 132 cents a day with the remainder on the final day. The printed figures are the only self-check the agent has, so a figure that does not follow from the armed row is worse than no figure at all.

Two invariants, checked at authoring time: every load-bearing clause has at least one row, and every fixture edge row has a clause that governs it. A row with no clause is an ungraded decision waiting to become an oracle dispute.

### 5.2 Precedence

Every pair of documents that could disagree, and the clause that settles it. No pair is left open.

| Pair | Governs | Stated in |
|---|---|---|
| finance policy vs any contract | policy governs recognition; contract governs presentation and exclusions | finance-policy header, BP-2 |
| finance policy vs comp-store policy | comp-store policy governs comparability; finance policy never defines it | finance-policy header, CMP-1 |
| comp-store policy vs a published comp figure | the recomputed figure, after restatement | CMP-2 |
| retail calendar vs date arithmetic in a model | `comp_date_ly` governs; an offset is never the answer | CAL-3, REV-16 |
| rate policy vs `raw.carrier_rate_cards` | the policy's effective era governs which version applies | RATE-1 |
| inventory policy vs the data | the policy states the method from its effective date; the data only shows that something changed | INV-1 |
| cost-restatement memo vs semantic definitions | the memo governs `freight_cost` from the FY2026 boundary | MEMO-1 |
| quality-contract revision vs a shipped test | the revision governs; a test written before its date is stale until re-derived | QC-3 |
| reconciliation vs billing integration | reconciliation governs who wins; billing defines the terms it uses | R-1 header, B-1 |
| reconciliation vs finance policy on closed months | finance policy §REV-8 defines closed; R-5 applies it | R-5 |
| ticket vs contract | contract | CM-1 |
| ticket vs finance policy | policy, unless the ticket amends the contract first | CM-1 |
| runbook vs code | code | CM-3 |
| runbook vs runbook | the one inside its review date | CM-4 |
| registry vs lineage | lineage, and the disagreement is a bug | REG-2 |
| contract prose vs contract yml | the yml is enforced, the prose governs meaning; a disagreement is a bug | contracts/README.md |
| semantic definitions vs a model | definitions name, models compute; a model computing something else must rename | SD-2 |
| retention vs a deletion request | the RET-5 exemptions hold | RET-4, RET-5 |

### 5.3 Numeric semantics, pinned

Each of these is stated in the document that owns it. The professional choice and the graded choice must not be able to differ.

| Question | Answer | Owner |
|---|---|---|
| Money | integer cents everywhere, no floats at any step | REV-2 |
| Uneven division | `floor`, remainder on the final day | REV-2 |
| Service period day counts | both end days inside | REV-1 |
| Refund boundary | day 14 inside, by calendar date | REV-6 |
| FX | invoice-date rate, once, at line level, before allocation; the rate is integer parts per million on the row | REV-7 |
| Month close | the 5th business day of the following month | REV-8 |
| Business date zone | America/Los_Angeles, the head-office zone; event timestamps UTC | finance-policy header |
| Fiscal week | Sunday start, 4-5-4, and FY2023 held 53 weeks | REV-16, CAL-1 |
| Prior-year comparable week | read `comp_date_ly`; never a date offset | CAL-3 |
| Comparable store | 13 full fiscal months open; a closure restates both years; a remodel over 21 consecutive days drops the period; an acquired store dates from the acquisition close | CMP-1..CMP-4 |
| Rate-card era boundary | `effective_from` inclusive, `effective_to` exclusive; the ship date decides | RATE-2, RATE-5 |
| Inventory valuation rounding | complement at basis points, per department, half up to the cent, then sum | INV-3 |
| Correction window | 30 days inclusive, 31 excluded | B-7 |
| Sync-lag age | measured on `event_time`, never `loaded_at`; 48 hours inclusive | R-4 |
| NULL | unknown, never zero; never enters a sum | B-3 |
| Revision ties | highest `revision`, then latest `event_time` | B-7 |
| SCD2 same-day ties | last by `updated_at`, then by source priority | contracts/customer-360.md |
| Percentages | rounded half up to one decimal, presentation only, never graded | SD-1 |
| Breakage age | 24 months from issue, booked on the last day of that month | REV-12 |
| Retention | vendor landing files 90 days, our own 400 days | RET-1, RET-2 |

Shipped world content that reads `current_date` — the stale cleanup runbook, LD-3's decoy filter, the board pack's status count — reads the warehouse clock, which the setup step pins so the world is stable at grade time. Nothing in the shipped tree reads a wall clock directly.

### 5.4 A test that keeps this honest

`tools/check_docs.py`, run in CI beside `check_world.py`. It asserts:

- every document in §2's inventory exists at the path named, and no document exists that the inventory does not name;
- every clause id in the shipped documents appears in the `PLANTED.md` map, and every clause id in the map appears in a shipped document — matched by id, never by assuming the ids run without gaps;
- every consumer in `docs/report-registry.md` appears in `docs/lineage.md` and has both a `contracts/<consumer>.md` and a `contracts/<mart>.yml`;
- every column a contract names is defined in `docs/semantic-definitions.md`;
- no two documents state a rule about the same subject without a precedence row in §5.2.

It is cheap, and it is the only thing that stops the documents layer drifting apart once tasks start editing it.

## 6. What this chapter asks of the rest of the world

1. **Twelve consumers as built objects**: six are dbt refs, a grep for the mart name adds C-4, and the remaining five are invisible to both dbt and grep. Those five are the point; do not tidy them.
2. **A hand-built `raw.finance_ledger`** at entity by closed fiscal month grain — about 74 rows under the close calendar. It must not share a generator with anything that produces expected results, and it is reconciled against an independent implementation before ship, never edited to agree.
3. **Fixture edge rows for all ~128 clauses**, tracked in the map from day one rather than back-filled.
4. **`ops.merge_candidates`** with 300 true pairs, 90 of them fuzzy-invisible and 60 fuzzy-attractive false pairs, for C-1 and C-8.
5. **Dual timestamps on every processor and feed event**, with a real 3% two-to-five-day tail and rows that straddle the 48-hour line in both directions.
6. **`raw.fiscal_calendar`** holding the 4-5-4 calendar with `comp_date_ly`, `comp_week_ly` and `is_53rd_week`, plus the store `opened_on`, `closed_on`, remodel windows and `acquired_from` that the CMP clauses test.
7. **The FY2023 pre-history slab**, store × day summary grain across the whole fiscal year, which carries every FY2023 week `comp_date_ly` names for a graded FY2024 week by construction.
8. **Seeded run history** for LF-1042's February replay week, at run level, with two runs on the same dates.
9. **A `RESPONSE.md` convention** the world states once, since CA-1, DR-1 and NW-214 all grade a written answer. The world states where the file goes and that a conflict belongs in it, and never what a good answer contains.

Costs worth stating plainly. The documents layer is about 2,160 lines of prose to write and to keep true. Every edit to it retires the trials of the tasks that read it, so write it once, in one batch, before any task ships. The registry and `docs/lineage.md` are regenerated together whenever a consumer is added, or REG-2 becomes a lie inside our own world.

## 7. Open questions

1. **Does the 2:1 furniture ratio hold at 2,160 lines?** It is a target, not a measurement. Probe it: if the first tasks show reading time eating the wall clock rather than composition, cut furniture from the documents no task grades, never from the clause bodies.
2. **PII-1 against C-12 overlap.** Both grade a sweep across off-lineage surfaces. Keep them, but grade different surfaces, or the second one is the first one with a new name.
3. **Where gift-card breakage gets graded.** It is the cheapest retail clause to write and the easiest to leave ungraded. Either FIN-311 grades a breakage month, or REV-12 comes out.
4. **Which clauses FIN-311 grades.** The catalog says eleven graded decisions; REV-1..REV-14 and REV-16 is the menu. Pin the subset at authoring and keep it in the map.
