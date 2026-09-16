# 3. The extracts — the landing layer

Copperline's applications are the 79-table model in `02-upstream-model.md`. The
warehouse never sees them. It sees **extracts**: files and API pages that each
application drops for the data team, landed under `raw.*` and `ops.*`. This
chapter specifies every extract — which upstream tables or which outside system
it draws from, how it is delivered, where it lands, what shape it has, how big
it is, which eras cross it, and which tasks read it.

Thirteen source families, S1 to S13. Every one names the tasks it arms. A source
that arms too little is called out in §19.

**What the extract boundary adds.** The upstream model is the company's own
truth, and it is tidy: one clock per row, `DEC(18,4)` money, integer surrogate
keys, no delivery history. The mess is applied when the row leaves the
application, and it is applied in the same six ways every time:

1. **Money becomes integer cents.** `DEC(18,4)` amounts land as `*_cents`
   `BIGINT`. Where a row is not in USD it also carries `currency_code` and
   `fx_rate_ppm`, an integer in parts per million, so any expected result can be
   re-derived from the landed row alone.
2. **One clock becomes two or three.** Every fact row that can arrive late
   carries `event_time_utc` (when it happened), `loaded_at` (when the warehouse
   saw it), and, where a local business day matters, `event_time_local` with the
   zone on the row or one join away.
3. **Keys become text in the format of their era.** `customer.customers.customer_code`
   lands as `CUST####` or `C-######` depending on when the row was written;
   surrogate BIGINTs never leave the application.
4. **The feed's own bookkeeping is added.** `loaded_at`, `restates_event_id`,
   `attempt_no`, `batch_id`, `file_date`, `received_at`, `deleted_at` — columns
   the application has no use for and the delivery does.
5. **Vocabulary follows the sender, not the warehouse.** A second system spells
   a status its own way, and nothing translates it on the way in.
6. **Names follow Copperline's fiction.** The upstream model's generic names map
   to the names in this chapter at extract time.

Everything else about the extract — grain, keys, relationships — comes straight
from the upstream table it draws on. Where an extract carries a table the
upstream model does not enumerate, the source says so and names the application
module it comes from.

**The second systems are not in the upstream model, and that is the point.**
The two processor settlement feeds, Copperline Marketplace's operator platform,
the carriers' rate cards and invoices, the ad platforms and Larkspur email are
other companies' systems, simulated separately. They share transaction identity
with `sales.payments` and `sales.orders` and nothing else. That gap is the
reconciliation seam PAY-207, NLO-4 and DR-1 all work in.

Northwave is a second, smaller upstream model. Its one-off extract landed at E3
and never refreshed, which is why the acquired book's shape is foreign rather
than a filtered copy of Copperline's.

Dates, eras and the calendar belong to `01-timeline.md`. This chapter names eras
by id and gives dates only for the delivery incidents that are its own content.

---

## 1. Conventions the whole landing layer obeys

State these once; every source below assumes them.

1. **Money is an integer count of minor units**, in a column ending `_cents`,
   with `currency_code` beside it wherever more than one currency can appear.
   Two exceptions, both armed and both from outside systems: the Halcyon
   settlement feed (S2b, two-decimal strings) and the finance workbook (S6c,
   locale-formatted strings). Nothing else in the world carries a float.
   Quantities are not money: `qty` stays `DECIMAL(12,3)` because Copperline
   sells timber and aggregate by length and weight.
2. **Timestamps are UTC and end `_utc` or `_at`.** Dates end `_date`. A
   local-clock column says so in its name (`event_time_local`,
   `local_order_date`) and carries a zone or joins to one. Two era exceptions,
   both armed: the POS stamp under E1, and Halcyon's zone-less merchant-local
   datetimes.
3. **Two timestamps on every feed that can arrive late.** `event_time_utc` and
   `loaded_at`. The gap is real, it has a distribution (§4), and
   `docs/late-data-policy.md` states the per-source bound.
4. **Identifiers are text, fixed-width and zero-padded.** `ORD-########`
   (order), `OE-#######` (the processor-facing order reference), `C-######` and
   `CUST####` (trade account, current and legacy), `NWA-#####` (Northwave
   account), `S-####` (store), `SKU-#####`, `INV-#######` (invoice), `L-##`
   (invoice or order line within its parent), `PI-########` (payment intent),
   `PM-#######` (payment), `RMA-#######`, `GC-########` (gift card), `LANE-##`.
5. **Two delivery paths, both real, both present.** Dated landing files under
   `landing/<source>/dt=YYYY-MM-DD/…`, which the ingest DAGs consume, and bulk
   mirrors in `raw.*` for the tables that arrive as a whole-table extract. A
   source that arrives as files in life arrives as files here. `raw` is
   read-only to every task, and `CONVENTIONS.md` says so.
6. **The fixtures are written at trial setup, not shipped.** The generator
   simulates the upstream model, derives these extracts from it, and writes both
   the warehouse tables and the landing tree into the trial before the agent
   starts. `workspace/` holds source only. The exceptions are the hand-authored
   files in rule 7.
7. **Four kinds of file are authored by hand and ship as source**: the ERP
   ledger (S5), the finance workbook (S6c), the reference tables whose exact
   values a task grades (`raw.market_config`, `raw.entities`,
   `raw.pay_processor_windows`, `raw.gift_card_jurisdictions`,
   `raw.carrier_rate_cards`), and `seeds/markets.csv`. The generator does not
   write them and contains no reference to them.
8. **`ops.*` is the platform's own bookkeeping**, not an extract:
   `ops.load_control` and `ops.merge_candidates`. It is landed
   by the generator with the same rules and is read-only to tasks.

---

## 2. Delivery, landing paths and retention

| Source | What sends it | Lands at | Owner for retention |
|---|---|---|---|
| S1 orders | Copperline OMS | `landing/oms/dt=<ds>/orders.csv`, `order_lines.csv` | own |
| S2a Meridian | Meridian Pay | `landing/meridian/dt=<ds>/hr=<hh>/events.jsonl` | vendor |
| S2b Halcyon | Halcyon Payments | `landing/halcyon/dt=<ds>/settlement_<acct>.csv` | vendor |
| S2c marketplace | Copperline Marketplace | paginated API, `include/lib/marketplace_api.py` | own |
| S3 POS | Northgate store servers | `landing/pos/dt=<ds>/store_<S-####>.csv` | own |
| S4 clickstream | Driftwood collector | `landing/events/dt=<ds>/hr=<hh>/producer=<p>/part-000.parquet` | own |
| S5 ledger | Ironwood ERP, by hand | `fixtures/finance/ledger_monthly.csv` | own, exempt |
| S6a inventory | Corvid WMS | `landing/wms/dt=<ds>/` | own |
| S6b carriers | three carriers | `landing/carriers/<carrier>/` | vendor |
| S6c workbook | a person in finance | `landing/finance/logistics_cost_workbook_fy26q1.csv` | own |
| S7 marketing | Beacon Ads, Tessera Social, Solstice Search, Larkspur | `landing/ads/dt=<ds>/<platform>.csv` | vendor |
| S8 CRM | Halyard, paginated API | `include/lib/halyard_api.py` | own |
| S9 PIM | Palette | `landing/pim/dt=<ds>/changes.csv` | own |
| S10 reference | seeds and small files | `fixtures/reference/` | own |
| S11 orders export | Copperline OMS | `landing/orders_export/dt=<ds>/part-000.parquet` | own |
| S12 promos, cards | Copperline OMS | in the S1 nightly extract | own |
| S13 invoicing | Ironwood ERP | `landing/ar/dt=<ds>/` | own |

**Retention** is `docs/retention-policy.md`. RET-1 keeps vendor landing files 90
days. RET-2 keeps Copperline's own landing files 400 days. Everything older
moves to an archive, and the archive is the only copy. RET-5 exempts the finance
ledger and anything under legal hold, which keeps 7 years.

The consequence a task grades: the Meridian landing zone holds 90 live days, so
any rebuild reaching back beyond that reads the archive — and the archive job
was itself broken from **2026-02-09 to 2026-02-14**, six days that no longer
exist anywhere (`ops/incidents/2026-02-16-archive-gap.md`). That is DR-1's
unrecoverable window. It is documented, not inferred, and the right answer flags
it rather than interpolating it from a downstream mart.

---

## 3. Which upstream tables each source draws on

| Source | Upstream tables (`02-upstream-model.md`) or outside system |
|---|---|
| S1 | `sales.orders`, `sales.order_lines`, `sales.order_line_discounts`, `store.channels`, `store.stores`, `customer.customers` |
| S2a, S2b | Meridian Pay and Halcyon Payments, sharing identity with `sales.payments.gateway_reference` |
| S2c | Copperline Marketplace's operator platform, joined to `sales.orders` on the marketplace channel |
| S3 | Northgate store servers, which post into `sales.orders` / `sales.order_lines`; `store.stores`, `store.registers` |
| S4 | `ecommerce.web_sessions`, `page_views`, `carts`, `cart_lines`, `search_queries` |
| S5 | `finance.journal_entries`, `journal_lines`, `chart_of_accounts`, `fiscal_periods`, `cost_centers` |
| S6a | `inventory.inventory_balances`, `inventory_movements`, `stock_counts`, `stock_count_lines` |
| S6b | `logistics.shipments`, `shipment_lines`, `fulfillments`, `carriers`, `tracking_events`; the carriers' own rate cards and invoices |
| S6c | one person's export from the logistics cost mart and the carrier invoices |
| S7 | Beacon Ads, Tessera Social, Solstice Search and Larkspur; `customer.marketing_campaigns`, `campaign_responses` |
| S8 | `customer.customers`, `support.tickets`, `support.ticket_categories`; Northwave's own model |
| S9 | `product.products`, `product_variants`, `categories`, `brands`, `prices`, `price_lists` |
| S10 | `core.currencies`, `countries`, `exchange_rates`, `regions`, `tax_rates`; `finance.fiscal_periods` |
| S11 | `sales.orders`, exported a second time for a second consumer |
| S12 | `product.promotions`, `promotion_products`, `sales.coupons`, `order_line_discounts`, `sales.returns`, `return_lines`, and the OMS gift-card tender module |
| S13 | `finance.ar_invoices`, and the AR and billing modules behind it |

Nothing extracts `hr.*`, `logistics.delivery_routes`, `logistics.route_stops`,
`supply_chain.*`, `product.product_reviews` or `ecommerce.search_queries`. Those
schemas make the company real inside the generator and no feed carries them
until a task claims one.

---

## 4. The late tail

The distribution is `01-timeline.md` §4.4 and is the same everywhere it applies.
What differs per feed is which distribution it draws from and what bound the
docs state.

| Feed | same day | tail | stated bound |
|---|---|---|---|
| OMS orders, incremental | 92.4% | 5 days | `docs/late-data-policy.md` LD-1 |
| Meridian settlements | 92.4% | 5 days | LD-1 |
| Marketplace settlement | 71% | 5 days | LD-1 |
| In-store card settlements | 100% | none | the POS runbook |
| Clickstream, web / ios / android / email_click | — | 4 hours | the contract |
| Clickstream, `nightly_batch` | — | 26 hours | the contract |
| Ads spend | — | restates 3 to 7 days back | none — NLO-1's problem |
| PIM change feed | — | out of order, no bound | the SCD contract |

Nothing arrives later than day 5, and something always arrives on day 5. So the
true safe lookback is a measured fact — five days — and the habitual three-day
guess drops about 0.8% of default-feed rows and about 4% of marketplace rows.
A uniform five-day lookback is not free either: an in-store card settlement is
never late, so five days there is pure waste. That per-feed split is what
NLO-2 grades.

The 100% row is the settlement leg, not the file. A store's POS batch file can
arrive up to three days late (§8), which delays the whole file at once; no row
inside it carries a lag of its own. A lookback tuned on row-level lag and a
lookback tuned on file arrival are different numbers, and both are right about
different things.

---

## 5. Source index

Volumes are targets the generator meets at the shipped profile, over the fixture
range and the 268-store estate. §20 holds the size budget.

| Id | Source | Delivery | Tables | Rows | Arms |
|---|---|---|---|---:|---|
| S1 | Orders (OMS) | nightly extract + dated landing | 4 | 12.7M | D7, NW-231, NLO-4, NW-214, PAY-207, LF-1042, DR-1, C-new-1, INC-1 |
| S2a | Meridian Pay settlements | hourly JSONL | 2 | 3.7M | PAY-207, NLO-2, NLO-4, DR-1, FIN-311 |
| S2b | Halcyon settlements | nightly CSV | 1 | 0.9M | PAY-207, E4 currency work |
| S2c | Marketplace settlement | paginated API | 3 | 0.74M | PAY-207, C-new-1, FIN-311 (REV-14) |
| S3 | Store POS batches | one file per store per night | 4 | 2.15M | W2, NW-231, D7, CMP-2/3/4 |
| S4 | Clickstream (Driftwood) | hourly parquet | 1 | 2.7M | EVT-1, NLO-1, NLO-2, ML-1, wall-clock pressure |
| S5 | ERP ledger (Ironwood) | monthly, hand-typed | 2 | 103 | FIN-311 (the oracle), C-new-1 |
| S6a | Inventory / WMS (Corvid) | daily and weekly files | 3 | 3.4M | ML-1, DQ-1, E8 valuation |
| S6b | Carrier feeds | per-carrier weekly CSV + rate cards | 4 | 4.5M | FIN-388, DR-1, DQ-1 |
| S6c | Finance workbook | one authored file | 1 | 1,193 | XLS-1 |
| S7 | Marketing platforms | daily API pull, nightly email export | 2 | 0.16M | PII-1, NLO-1, NLO-3, C-new-1 |
| S8 | CRM, support, Northwave book | paginated API + one-off load | 4 | 0.096M | NW-214, the swapped books, PII-1, D7 |
| S9 | Product / PIM (Palette) | change feed | 2 | 21,400 | SCD2-from-contract, CT-4, ML-1, W4 |
| S10 | Reference data | seeds and small files | 8 | 0.025M | NLO-3, FIN-311, W2, T-11, XLS-1, FIN-388 |
| S11 | Parquet orders export | dated parquet | 1 (2 shapes) | 2.6M | W4, D7, NW-231 |
| S12 | Promotions, gift cards, returns | OMS side tables | 5 | 3.12M | C-new-1, FIN-311, PAY-207, DQ-1 |
| S13 | Trade invoicing | ERP AR export | 7 | 1.50M | FIN-311, C-new-1, BP-1, DQ-1 |

---

## 6. S1 — Orders (Copperline OMS)

The revenue spine. Everything that is a sale starts here, whatever channel took
it. The extract is `sales.orders` and `sales.order_lines` with the header
discount left where the application put it — on the header, not allocated to the
lines. Allocating it is the derive-then-build subject in the `int` layer, and
nothing upstream does it for you.

**Delivery.** Two paths, both present. A nightly whole-table extract lands under
`landing/oms/dt=<ds>/` and is mirrored into `raw`. Separately an incremental
extract, keyed on `updated_at`, is what the ingest DAG consumes. The incremental
extract carries the late tail, so it is the one NLO-2 and INC-1 measure.

**Grain.** One row per order. One row per order line. One row per crosswalk
entry. One row per status change.

### `raw.orders`

| column | type | notes |
|---|---|---|
| order_id | varchar | `ORD-########` |
| customer_ref | varchar | **armed (E2)** — `CUST####` before the re-key and `C-######` after, in one column; 45 rows dated after the cutover still carry the old format |
| loyalty_id | varchar | consumer orders only; trade orders join `raw.customers` |
| brand | varchar | `copperline` \| `northwave` |
| channel | varchar | `store` \| `web` \| `marketplace` |
| store_id | varchar | `S-####`, null off-store |
| market_code | varchar | joins `raw.market_config` |
| event_time_utc | timestamp | **armed (E6)** — NULL before the standardization on store-sourced rows |
| event_time_local | timestamp | the local clock the channel wrote |
| local_order_date | date | the business date, which is not `event_time_utc::date` for store orders under E1 |
| order_status | varchar | `placed` \| `paid` \| `fulfilled` \| `cancelled` \| `returned` |
| currency_code | varchar | **armed (E4)** — NULL before the cutover, USD by construction |
| fx_rate_ppm | bigint | integer parts per million, the rate used at generation |
| subtotal_cents | bigint | |
| order_discount_cents | bigint | **armed** — header level, never pushed down to lines |
| tax_cents | bigint | |
| shipping_cents | bigint | |
| grand_total_cents | bigint | transaction currency |
| gift_card_applied_cents | bigint | sums the redeem entries in `raw.gift_card_ledger` |
| order_ref | varchar | **armed (E5)** — `OE-#######`, the processor-facing reference, recycled across retry attempts |
| source_system | varchar | `oms` \| `pos_replay` \| `marketplace_sync` \| `nwv` |
| is_test | boolean | **armed** — 18,000 synthetic rows a naive population includes |
| deleted_at | timestamp | **armed (PAY-207 R-6)** — soft delete; the row stays visible with an exception kind |
| updated_at | timestamp | the incremental watermark |
| loaded_at | timestamp | **armed (NLO-2)** — the day-5 tail |

### `raw.order_lines`

| column | type | notes |
|---|---|---|
| order_line_id | varchar | `L-##`, unique within order |
| order_id | varchar | |
| line_no | integer | |
| sku | varchar | joins S9 |
| qty | decimal(12,3) | |
| unit_price_cents | bigint | |
| unit_cost_cents | bigint | |
| line_discount_cents | bigint | line level only; the header discount is not in here |
| tax_rate_id | varchar | |
| tax_cents | bigint | |
| line_total_cents | bigint | |
| fulfillment_type | varchar | **armed** — `ship` \| `pickup` \| `marketplace_fbm`; a `pickup` line returned by mail is the cross-channel case |
| line_status | varchar | |

### `raw.customer_id_map` — the E2 crosswalk

`legacy_id` (`CUST####`), `customer_id` (`C-######`), `migrated_at` (all on the
re-key date), `note` (free text on the unmapped rows). 2,600 rows, of which 60
legacy ids never migrated. Those 60 roll up under `customer_id = legacy_id`, and
the runbook says so, so dropping them is wrong too.

### `raw.order_status_history`

One row per status change: `order_id`, `from_status`, `to_status`, `changed_at`,
`changed_by`, `feed` (`oms` \| `pos_replay`). Furniture with one job — LF-1042's
second suspect is the two-feed union and its boundary rows.

**Volumes.** Orders 2,600,000, channel mix stores 58%, web 31%, marketplace 11%.
Order lines 7,100,000, 2.7 per order. Crosswalk 2,600 with 60 orphans. Status
history 3,000,000 — the generator writes it for the trailing 400 days only,
which is all LF-1042 reads.

**Era edge populations.** About 33% of order volume predates E2. Exactly **45
orders dated after the re-key carry old-format `customer_ref`**, written by the
store integration that nobody told; a date-based reading of the crosswalk gets
those and only those wrong, which is over 2% of the graded month. One order on
the cutover date in each format. One trade account with orders in both eras,
whose revenue must unify under one id.

**Eras.** E2, E4, E5, E6; E1 through the store channel's business date.

**Tasks.** D7 and NW-231 (backfill across E2), NLO-4 (the recycled `order_ref`),
NW-214 and the swapped books (the customer spine), PAY-207 (the orders side of
the ladder), LF-1042 (the doubled window), DR-1 (what `fct_payments` reconciles
to), C-new-1 (`booked_cents`), EVT-1 (the order count the clickstream is
compared against), INC-1 and NLO-2 (the incremental watermark).

---

## 7. S2 — Payments

Three feeds, deliberately unalike, plus the bridge that makes them joinable.
This is the densest arming in the world.

### S2a — Meridian Pay, the current processor

**Delivery.** Hourly JSON-lines into `landing/meridian/dt=<ds>/hr=<hh>/events.jsonl`,
converted to parquet by the ingest DAG. Live from the start of E5's dual-feed
quarter. Vendor retention: 90 live days, then archive, with the six-day archive
gap in §2.

**Grain.** One row per payment event. Several events per intent, several intents
per order.

#### `raw.pay_meridian_settlements`

| column | type | notes |
|---|---|---|
| event_id | varchar | the true idempotency key |
| payment_id | varchar | `PM-#######` |
| intent_id | varchar | `PI-########`, joins the bridge |
| order_ref | varchar | **armed (NLO-4)** — same name as `orders.order_ref`, matches 96%, and is wrong |
| processor_txn_id | varchar | Meridian's own key |
| event_type | varchar | `authorized` \| `captured` \| `refunded` \| `chargeback` \| `chargeback_reversed` \| `void` |
| amount_cents | bigint | |
| currency_code | varchar | |
| event_time_utc | timestamp | when the processor says it happened |
| loaded_at | timestamp | **armed (R-4, NLO-2)** — the day-5 tail |
| settlement_date | date | **armed (REV-7)** — the date whose FX rate must *not* be used for recognition |
| restates_event_id | varchar | **armed (R-5)** — non-null on restatements, up to 30 days back |
| attempt_no | integer | **armed (NLO-4)** — 2 or 3 on the retried population |
| deleted_at | timestamp | **armed (R-6)** — the source API's soft delete |

#### `raw.payment_intents` — the bridge

`intent_id`, `order_id` (the true link to S1), `order_ref` (recycled across
attempts for one order), `attempt_no`, `created_at`, `outcome` (`succeeded` \|
`failed` \| `abandoned`). This is the extract of the OMS's payment-attempt log,
which the upstream model summarizes into `sales.payments` plus its
`gateway_reference`. `stg_payment_intents` is built from it, and it is NLO-4's
answer.

**Volumes.** 1,600,000 events. 2,100,000 intents covering 1,820,000 orders, of
which **273,000 orders — 15% — carry two or three attempts**, so 15% of
order-to-payment pairs are wrong under the naive `order_ref` join while totals
inflate only about 2%. Restatements 29,000, of which 40 sit at exactly 30 and 31
days (both sides of the R-5 boundary) and 120 restate a closed month, which is
the adjustment-row branch. Soft deletes 4,800.

Card settlement covers about 70% of orders. Trade accounts buy on terms and
settle through S13, which is why a payments-only reconciliation of total revenue
is short by a third and is meant to be.

**Eras.** E5 (its whole life), E6, and the closed-book boundary.

**Tasks.** PAY-207 (every rung), NLO-4 (the bridge is the answer), NLO-2 (the
lookback is measurable as `loaded_at − event_time_utc` and is 5 days, not 3),
FIN-311 (REV-7 — settlement rates are never recognition rates), DR-1 (the
landing files a truncated fact is rebuilt from, and the gap where it stops).

### S2b — Halcyon Payments, the legacy processor

**Delivery.** One nightly CSV per merchant account,
`landing/halcyon/dt=<ds>/settlement_<acct>.csv`. Runs from before the fixture
range to the end of E5's overlap quarter, then stops. Halcyon never reaches E6,
so this feed has no UTC column and never gets one.

**Grain.** One row per settled transaction per file. No event grain and no
restatement mechanism — the older system reissued the whole file instead.

#### `raw.pay_halcyon_settlements`

| column | type | notes |
|---|---|---|
| txn_id | varchar | |
| merchant_ref | varchar | the same value as `orders.order_ref`, spelled differently |
| status | varchar | **armed** — `SETTLED` \| `REVERSED` \| `PENDING`, a different vocabulary from Meridian's |
| amount | varchar | **armed** — two-decimal string, not cents, in the transaction currency |
| currency | varchar | **armed (E4)** — NULL on 34% of rows, where the merchant region decides it |
| txn_datetime | varchar | **armed** — merchant-local, no offset, no zone column |
| settled_on | date | |
| file_date | date | the only load clock this feed has |
| batch_id | varchar | |
| merchant_acct | varchar | joins `raw.merchant_regions` |

**Volumes.** 900,000 rows. The E5 overlap quarter carries about **193,000
payments present in both feeds**, so a naive union double-counts roughly a
quarter of that year's settlement volume — far too big to miss.
`raw.pay_processor_windows` and `docs/runbooks/processor-migration.md` give the
rule; the graded difficulty is in the two boundary months, not in noticing the
overlap. After E4 about 30% of Halcyon volume is non-USD, GBP and EUR being most
of it, so an unconverted `sum(amount)` on the post-cutover window is wrong by 8
to 12% of any graded total. Before E4 the NULL currency is harmless, because
everything is USD by construction — the same column is a trap on one side of an
era boundary and furniture on the other.

**Eras.** E4, E5.

**Tasks.** PAY-207 (R-4 cannot be applied to Halcyon the way it is applied to
Meridian, because Halcyon has no event clock; the policy says which clock stands
in), the E4 currency work, DR-1.

### S2c — Copperline Marketplace settlement

Copperline runs its own marketplace and takes a commission. The settlement feed
is written from the **operator's** side: Copperline owes the seller the
principal and keeps the commission and the fulfilment fee. GMV is the seller's
number and is never Copperline revenue (REV-14). The feed exists across the
whole fixture range.

**Delivery.** A paginated API stub, `include/lib/marketplace_api.py`, whose
listings carry `next_page_token`. This is the world's enumeration lever: any
task that counts marketplace rows and stops at page one is wrong, and nothing
warns it.

**Grain.** One row per seller order. One row per settlement line per payout
period. One row per payout.

#### `raw.marketplace_orders`

`marketplace_order_id`, `order_id` (the matching `raw.orders` row on the
marketplace channel), `seller_id`, `gmv_cents` (**armed** — the seller's order
value, never Copperline revenue), `commission_cents`, `fulfilment_fee_cents`,
`commission_rate_bps`, `currency_code`, `entity_code` (the entity the order
billed through; the currency cannot supply it, because CL-IE and CL-DE both
bill in euro), `placed_at`, `ship_confirmed_at` (**armed (REV-14)** — the
seller's ship-confirmation date is the recognition date, not the order date),
`order_state`, `loaded_at`. Named in the finance-policy header as a system of
record.

#### `raw.marketplace_settlements`

`settlement_id`, `payout_id`, `marketplace_order_id`, `order_id`, `line_type`
(`principal` \| `commission` \| `fulfilment_fee` \| `refund` \| `reserve_hold` \|
`reserve_release`), `amount_cents` (**armed** — the principal is money out and
signs negative from the operator's side), `currency_code`, `posted_at` (the
payout run that wrote the line, so a refund posts three weeks after the sale it
reverses), `payout_date`, `loaded_at`.

#### `raw.marketplace_payouts`

`payout_id`, `seller_id`, `payout_date`, `principal_cents`, `commission_cents`,
`fee_cents`, `net_paid_cents`, `payout_status`.

**Volumes.** 286,000 seller orders, 430,000 settlement lines, 22,140 payouts —
180 active sellers paid weekly across 123 fiscal weeks. Marketplace returns
reduce commission in the period of the return and never restate the original
period, including inside a closed month, which is a clause with two fixture rows
on each side.

**Eras.** E4 (sellers bill in market currency from the cutover), E6.

**Tasks.** PAY-207 (a third system that disagrees), C-new-1 (`booked_cents` is
principal; the board pack wants net of commission), FIN-311 (REV-14 and the
seller subscription line, which invoices through S13).

---

## 8. S3 — Store POS batches (Northgate)

**Delivery.** One file per store per night, `landing/pos/dt=<ds>/store_<S-####>.csv`,
pushed by the store server after close. Files arrive up to three days late; a
closed store sends nothing at all. The ingest DAG branches by region, and each
branch skips when its region has no files — normal on most days, and total on
the all-market holiday W2 grades.

**Grain.** One row per POS transaction, tender-typed, with the line detail
posted into `raw.order_lines` through `source_system = 'pos_replay'`.

### `raw.pos_sales_header`

| column | type | notes |
|---|---|---|
| pos_txn_id | varchar | |
| store_id | varchar | `S-####` |
| register_id | varchar | |
| business_date | date | what the store calls the day |
| event_time_local | timestamp | **armed (E1)** — the constant 23:05 close-batch stamp for the whole of E1, then real per-transaction times |
| event_time_utc | timestamp | **armed (E6)** — NULL through E1, populated from the standardization |
| received_at | timestamp | when the file landed |
| order_id | varchar | joins S1; null on 4% — cash sales never keyed to an account |
| customer_ref | varchar | trade sales only; null on consumer sales |
| tender_type | varchar | `card` \| `cash` \| `gift_card` \| `split` |
| gross_cents | bigint | |
| discount_cents | bigint | |
| tax_cents | bigint | |
| net_cents | bigint | |
| return_flag | boolean | |
| void_flag | boolean | **armed** — 1.8% of rows, a population boundary the contract decides |
| loaded_at | timestamp | |

Reading `event_time_local` as UTC pushes the whole close batch of every Americas
store into the next day. That is about 62% of the estate; the GB, IE and DE
stores sit at UTC+0 or UTC+1, where 23:05 local is still the same UTC day. So a
fleet-wide row count is only 62% wrong while per-store-day counts are wrong for
two thirds of stores and right for the rest — a partial, region-shaped error
that is much harder to profile away than a uniform one.

### `raw.pos_sales_daily` — the pre-history slab

Summary grain, store × day, the whole of FY2023: `store_id`, `business_date`,
`txn_count`, `gross_cents`, `discount_cents`, `tax_cents`, `net_cents`. It
exists so the 53-week comp comparison has a prior year to land on when the
transaction-grain history does not reach back that far. About 70,900 rows across
the 371 days of that year, which includes the 53rd week.

### `raw.stores` — SCD2

`store_id`, `store_name`, `store_format`, `region_code`, `market_code`,
`tz_name`, `opened_on`, `closed_on`, `acquired_from` (**armed (E3)** —
`'northwave'` on 44 rows whose `opened_on` predates the acquisition), `status`
(`open` \| `remodel` \| `closed`), `valid_from`, `valid_to`, `is_current`.
**Armed** — 9 stores changed region mid-history, so "region as of the charge
date" and "region as of now" differ; and the remodel spans are what CMP-3 reads.
268 current rows plus 22 prior versions.

### `raw.pos_batch_manifest`

`batch_id`, `store_id`, `business_date`, `file_name`, `row_count`, `stamped_at`,
`received_at`, `status` (`ok` \| `late` \| `missing`). This is the table a
completeness check reads. It is landed with the feed and, like all of `raw`, is
read-only: **W2's fix writes its explicit empty partition into the daily mart
`channel_daily_intake` publishes, never into `raw`.**

**Volumes.** POS headers 1,900,000. Pre-history slab 70,861 (see the slab section above; ch. 01 §4 fixes the count). Manifest 225,000
rows, of which 6,500 are late and 990 missing. Stores 290 rows.

**Eras.** E1 and E6 (the whole stamp story), E2 (the 45 late old-format rows are
written by this integration and land on `raw.orders.customer_ref`), E3 (the
acquired estate), the store-region SCD changes.

**Tasks.** W2 (every branch skips on the all-market holiday and the `none_failed`
join skips with them), NW-231 and D7 (backfills into E1 shift whole days),
CMP-2, CMP-3 and CMP-4 (closures, remodels and the acquired estate), FIN-388's
calendar arm.

---

## 9. S4 — Clickstream (Driftwood collector)

The world's one big fixture, and the reason the wall-clock cap can be made to
bind.

**Delivery.** Hourly parquet,
`landing/events/dt=<ds>/hr=<hh>/producer=<p>/part-000.parquet`. At-least-once
delivery: a producer may resend, and does.

**Grain.** One row per event.

### `raw.web_events`

| column | type | notes |
|---|---|---|
| event_id | varchar | **armed (EVT-1)** — the contract's idempotency key, and the one the consumer does not use |
| producer | varchar | `web` \| `ios` \| `android` \| `email_click` \| `nightly_batch` |
| partition | integer | 0-11 |
| offset | bigint | **armed (EVT-1)** — the key the consumer deduped on; correct until the replay reissued the window with new offsets |
| session_id | varchar | |
| anonymous_id | varchar | |
| customer_ref | varchar | null on 62% of events — a NULL population big enough to move any answer |
| event_name | varchar | `page_view` \| `product_view` \| `add_to_cart` \| `checkout_start` \| `purchase` |
| event_time_utc | timestamp | |
| load_time | timestamp | **armed** — per-producer bounds, 4 hours for web, ios, android and email_click, 26 hours for `nightly_batch` |
| page_path | varchar | |
| sku | varchar | null off product events |
| order_id | varchar | non-null on `purchase` only — the join to S1 that makes drift visible |
| utm_source, utm_medium, utm_campaign | varchar | joins S7 |
| device, country_code | varchar | furniture |

**Volumes.** **The shipped profile runs the generator at `--scale 0.25`**, and
the shipped baseline is about **3,100 events a day**, rising with the annual
growth rate through the range and 30x on each Black Friday — about 2.7M rows
over the 862 days, roughly 75 MB of parquet. Those are the numbers the
feed-decay postmortem in `06-consumers-and-docs.md` §3.1 prints, and the
numbers `config/alerts.yml` computes its band from: the postmortem's figures
are the fixture figures. Full scale, four times the shipped one, is a knob for
the wall-clock pressure probe, and §20 says what still has to be measured.

The event-to-order ratio is low for a retailer and is meant to be. Most of
Copperline's volume is trade accounts reordering by phone, at a counter, or in
the yard, and none of that touches the web funnel.

**The replay.** Three days, 2026-01-20 to 2026-01-22, were reissued with new
offsets: about 42,000 duplicate events, same `event_id`s, new `offset`s, new
`load_time`s (`ops/incidents/2026-01-23-event-replay.md`). Total volume after
the incident reads about 4% high — inside seasonal noise — while the per-producer
daily row sets are wrong by whole rows.

**The feed decay.** NLO-1's postmortem is written against this feed and its
baseline. The shape carries the task, not the level: three consecutive
sigma-invisible falls, 40% cumulative loss, three wrong daily flashes published
before anyone noticed (`ops/incidents/2026-01-14-feed-decay.md`). A daily
two-sigma band on a feed this size never fires; persistence does.

**One deliberate gap: there is no clickstream from the marketplace.** Copperline
Marketplace runs on its own storefront stack, the Driftwood collector was never
installed on it, and the seller portal has no collector at all. The gap is real,
it is documented, and it stops any task assuming one funnel covers all three
channels.

**Eras.** E6. The volume shape crosses every era and carries none of them.

**Tasks.** EVT-1 (flagship), NLO-1 (the decay), NLO-2 (the tail is measurable
here too), ML-1 (as-of feature joins), and any task that needs a replay to cost
real seconds.

---

## 10. S5 — ERP ledger (Ironwood): the independent oracle

**Delivery.** One monthly close export, `fixtures/finance/ledger_monthly.csv`,
**typed by hand**. It is a hand-built extract of the double-entry fiction —
`finance.journal_entries` and `journal_lines` summed to the account rollup the
close pack publishes.

**The authorship rule, which is load-bearing.** This file is the only check on
FIN-311 that does not come from the same place as the answer. It must not be
produced by the generator, by any solution, or by any code that computes an
expected result. Write it by hand, from the policy, by a person who did not
write the FIN-311 solution. Pin its hash in the world manifest and add a test
that fails if the generator ever touches it. Reconcile it, cent for cent,
against an independent `Decimal` implementation before the world ships, and
**never edit the ledger to make it agree** — fix the fixtures or fix the policy
text.

**Grain.** One row per closed fiscal month per entity.

`raw.finance_ledger`: `period_month`, `entity_code`, `currency_code` (always USD
— the ledger is reporting currency), `reported_cents`, `closed_on`,
`source_note`. About **74 rows** under the close calendar: `CL-US` for every
closed month in range, `CL-GB` / `CL-IE` / `CL-DE` from their first trading
period, `CL-MX` from FY2026 P1.

`raw.finance_close_calendar`: `period_month`, `close_date` (the fifth business
day of the following month, computed against `raw.market_calendar`), `status`
(`closed` \| `open`). 29 rows.

If FIN-311's tie needs account grain, that is a second hand-built file under the
same rules.

**Eras.** E4 (the entities that start trading), E8 (valuation changes what the
inventory accounts carry), and the close calendar itself.

**Tasks.** FIN-311 (the tie), C-new-1 (the board pack reconciles here), and any
task that needs a third book.

---

## 11. S6 — Inventory, carriers, and the finance workbook

### S6a — Inventory and WMS (Corvid)

**Delivery.** Two files a day into `landing/wms/dt=<ds>/`: a snapshot of the
A-class SKUs and a movement log. The long tail of SKUs snapshots weekly.

**Grain.** One row per (snapshot date, sku, location). One row per movement. One
row per department per fiscal period.

#### `raw.inventory_snapshots`

`snapshot_date`, `sku`, `location_id` (a DC or a forward-stock hub),
`on_hand_units`, `reserved_units`, `in_transit_units`, `unit_cost_cents`
(**armed (E8)** — NULL from the valuation change), `retail_value_cents`
(**armed (E8)** — NULL before it), `cost_complement_bps` (**armed (E8)** —
integer basis points, department grain, NULL before the change),
`last_counted_at`, `loaded_at`.

The two methods do not agree and are not meant to. Applying weighted-average
cost to FY2026 understates inventory value by 6 to 9% at department grain, and
applying the retail method to FY2025 is not defined at all, because the
complement is NULL there. The data announces that something changed; only
`docs/inventory-policy.md` says what the new rule is.

#### `raw.wms_movements`

`movement_id`, `occurred_at`, `sku`, `from_location`, `to_location`, `qty`,
`movement_type` (`receipt` \| `pick` \| `pack` \| `adjust` \| `cycle_count` \|
`rtv`), `reference_type`, `reference_id` (an order or an RMA), `operator_id`,
`loaded_at`.

#### `raw.dept_cost_complement`

`dept_code`, `fiscal_period`, `cost_complement_bps`, `approved_by`,
`effective_from`. FY2026 only, 120 rows.

**Volumes.** Snapshots 2,200,000 — about 1,300 stocked SKUs across 14 locations
weekly, with the 240 A-class SKUs snapshotted daily for the trailing 90 days
inside that total. Movements 1,200,000. Complement 120.

**Eras.** E8.

**Tasks.** ML-1 (features), DQ-1 (two of the twelve failing tests sit here), the
valuation build in the FIN-311 family, the inventory half of any ledger tie.

### S6b — Carrier feeds

**Delivery.** One CSV per carrier per week into `landing/carriers/<carrier>/`,
plus rate cards loaded by hand when a contract is signed, plus a monthly carrier
invoice.

**Grain.** One row per package, one per shipment, one per rate-card cell, one
per carrier invoice.

#### `raw.shipment_packages`

`package_id`, `shipment_id`, `order_id`, `carrier_code`, `service_level`,
`lane_id`, `origin_dc`, `dest_zip3`, `dest_country`, `ship_date` (**armed
(FIN-388)** — local at origin, which is what makes both 2026 DST days bite),
`ship_time_utc`, `delivered_at`, `billed_weight_g`, `zone`, `billed_cents`,
`accessorial_cents`, `brand` (**armed (FIN-388)** — `northwave` packages are the
affected population, and nothing states which lanes those are), `loaded_at`.

#### `raw.carrier_rate_cards`

`rate_card_id`, `carrier_code`, `lane_id`, `service_level`, `zone`,
`weight_break_g`, `rate_cents`, `effective_from`, `effective_to`, `version`
(**armed (FIN-388)** — v1 is the wrong January card, v2 the correction, and the
effective era comes from `docs/rate-policy.md`, not from these rows),
`loaded_at`. Hand-authored.

#### `raw.lanes`

`lane_id`, `origin_dc`, `dest_region`, `dest_country`, `region_code`,
`origin_tz_name`, `dest_tz_name`, `active_from`, `active_to`.

#### `raw.carrier_invoices`

`carrier_invoice_id`, `carrier_code`, `invoice_date`, `period_start`,
`period_end`, `total_cents`, `status`.

**Volumes.** Packages **9,200 a day inside the repricing window**, the ninety
days from 2026-02-01 to 2026-05-01 inside FY2026 Q1 — 828,000 rows across those
ninety days — and about 2,900 a day outside it, roughly
3,100,000 in total. The window is the contractor season, and that three-to-one
asymmetry is the whole of FIN-388's cost pressure. Shipments 1,400,000. Rate
cards 2,640 (2 versions × 6 carrier-service pairs × 44 lanes × 5 weight breaks).
Lanes 44. Carrier invoices 90.

**Eras.** The rate-card version boundary inside the Q1 window, and the two 2026
DST days.

**Tasks.** FIN-388 (flagship), DR-1, DQ-1, and `marts.fct_shipping_costs`, which
all three grade.

### S6c — The finance workbook (XLS-1)

**Delivery.** One file, dropped into
`landing/finance/logistics_cost_workbook_fy26q1.csv` by a person, monthly, and
picked up by `plat_workbook_inbox`, which globs `*.csv`. It ships as CSV so no
image dependency is needed to read it; every mess feature survives the format.

**Grain.** One row per (carrier, lane, fiscal week) — except where it is not,
which is the point.

**Schema, as the file actually reads.** Rows 1 and 2 are a merged title block
(`Logistics cost — FY2026 (draft) — do not circulate`, then blank). Row 3 is the
real header: `Carrier Code`, `Lane`, `Fiscal Week`, `Ship Date`, `freight_cost`,
`Fuel Surcharge`, `Accessorials`, `Unnamed: 7`, `Notes`.

Each mess feature is a graded clause with its own fixture rows:

| # | Feature | Rows affected | Why it moves the answer |
|---|---|---|---|
| 1 | merged title block, real header on row 3 | 2 | a default read makes the title the header |
| 2 | the header repeats at row 412 | 1 | two exports concatenated; the repeat parses as data |
| 3 | trailing spaces on `Carrier Code` (`"BRT "`) | 48 (4.0%) | orphans those rows on the join to S6b |
| 4 | a `Total` row at the foot and 12 per-carrier subtotals | 13 | double-counts every graded aggregate |
| 5 | `freight_cost` changes meaning at the FY boundary | all | gross of fuel surcharge before, net of it after; `docs/memos/fy26-cost-restatement.md` says so |
| 6 | locale-formatted numbers in the earlier block only (`1.234,56`) | 517 | parse as thousands or as null; both are wrong differently |
| 7 | two date formats, day-first then ISO | all | one format per block |
| 8 | `Unnamed: 7` blank, `Notes` holds commas and one embedded newline | all | furniture that breaks a naive split |

**Volumes.** 1,180 data rows, 12 subtotals, 1 total — 1,193 lines plus the two
header blocks. Hand-authored and never scaled.

**Eras.** E8's fiscal-year boundary, which is where the column meaning changes.

**Tasks.** XLS-1, and nothing else. One file, eight graded clauses: the best
clause-per-byte item in the world.

---

## 12. S7 — Marketing platforms

**Delivery.** A daily API pull per platform, written to
`landing/ads/dt=<ds>/<platform>.csv`. Larkspur email events arrive as a nightly
bulk export.

**Grain.** One row per (report date, platform, campaign). One row per email
event.

#### `raw.ads_spend_daily`

`report_date`, `platform` (`beacon` \| `tessera` \| `solstice`), `campaign_id`,
`campaign_name`, `market_code`, `channel`, `impressions`, `clicks`,
`spend_cents`, `currency_code` (**armed** — platforms bill in the market's local
currency, so the FX join is needed here too), `attributed_orders`,
`attributed_revenue_cents` (**armed** — the platform's own claim, which
disagrees with the warehouse and which the board-pack contract says to ignore),
`loaded_at`, `restated_at` (**armed (NLO-1)** — ad platforms restate 3 to 7 days
back, so a freshness alarm on this table has a legitimate reason to look wrong).

#### `raw.email_events`

`send_id`, `campaign_id`, `customer_ref`, `email_hash` (**armed (PII-1)** —
derived from a tagged column, so the governance rule reaches it), `event_type`
(`sent` \| `delivered` \| `open` \| `click` \| `bounce` \| `unsub`),
`event_time_utc`, `message_id`.

**Volumes.** Ads 104,000 rows — three platforms, about 40 live campaigns each,
every day of the range. Email events **60,000**, a deliberate cut: the table
exists for PII-1's derived-column set and one join, and it does not need to be
big to do that.

**Eras.** E4 (campaign budgets are set on fiscal periods, and the platforms bill
in market currency from the cutover), and the second-wave market setup, which
must appear here as well as in `raw.market_config`.

**Tasks.** PII-1 (the derived-column set), NLO-1 (the alert thresholds and the
two documented legitimate dips — a campaign pause and a platform restatement
lag, both in `ops/calendar/quiet-days.yml`), NLO-3 (a missing market shows up in two places),
C-new-1 (the board pack's marketing line).

---

## 13. S8 — CRM, support, and the Northwave book

**Delivery.** Halyard exports through a paginated API stub,
`include/lib/halyard_api.py`, whose listings carry `next_page_token`. The
Northwave book arrived once, at E3, as a one-off load the integration team ran
by hand, and it has not refreshed since.

**Grain.** One row per trade account. One row per Northwave account. One row per
decided merge. One row per support ticket.

#### `raw.customers`

`customer_id` (`C-######`), `legacy_id` (nullable, `CUST####`), `account_name`,
`contact_name`, `email`, `phone`, `address_line1`, `city`, `region_code`,
`postal_code`, `country_code`, `market_code`, `tax_id` (**armed (NW-214)** — the
deterministic key on 205 of the 300 true pairs, and the reason the flagged table
is trustworthy), `created_on`, `status` (`active` \| `churned` \| `deleted`),
`billing_era` (**armed (REV-11)** — `legacy` on contracts signed before E5's
start, `current` after), `tier`, `payment_terms_code`, `deleted_at`,
`updated_at`, `source_brand`.

`raw.customers` holds **trade accounts only**. Copperline's dedup problem is a
trade-account problem, which is where it is real; consumer orders carry an
anonymous `loyalty_id` and never enter this table.

#### `raw.nwv_accounts`

`nwv_account_id` (`NWA-#####`), `account_name`, `primary_contact`,
`contact_email`, `phone`, `billing_city`, `billing_state`, `country`, `tax_id`,
`opened_on`, `status`, `owner`, `legacy_crm_id`.

#### `ops.merge_candidates`

`customer_id`, `nwv_account_id`, `confidence`, `method` (`deterministic_tax_id`
\| `manual_review`), `decided_by`, `decided_on`, `note`. **300 rows, and they
are the answer.** The 60 fuzzy-attractive false pairs are absent from it, and
that absence is load-bearing.

#### `raw.support_tickets`

`ticket_id`, `party_ref` (**armed** — holds a `C-` id or an `NWA-` id),
`party_source` (`oms` \| `nwv`), `opened_at`, `closed_at`, `channel`,
`category`, `order_id`, `csat`.

**Volumes and the adversarial population.** `raw.customers` 4,000 rows, 3,960
active. `raw.nwv_accounts` 1,400 rows, 1,380 active. True pairs 300, all both
active: 210 are findable by fuzzy matching, **90 are fuzzy-invisible** — a
married surname on the contact, `info@` addresses shared across a dealer group,
a reseller trading under a different name. **60 fuzzy-attractive false pairs**:
two franchises of one dealer group in different states, two "Riverside
Outfitters LLC" that are genuinely different companies, a head office and its
own warehouse account. Support tickets 90,000. All names are invented.

**The three counts, and why none of them is right.**

| Number | Where it comes from | Value |
|---|---|---|
| Board deck | naive union of active rows across both books | 3,960 + 1,380 = **5,340** |
| CRM export | the CRM's own exact-match rule (205 true pairs by email, 43 false by shared domain) | 5,340 − 248 = **5,092** |
| A fuzzy answer | 210 true merges plus up to 60 false | ≈ **5,070** |
| The truth | the 300 decided pairs | 5,340 − 300 = **5,040** |

The fuzzy answer moves the headline by 30 accounts — 0.6% — so profiling never
warns, while 150 of 300 pairs are wrong. Both wrong paths are present and both
are plausible. `active_accounts` in the board pack means **active status**, and
BP-1 says so.

**Eras.** E3 (the whole book), E2 (`legacy_id`), E5 (`billing_era`). The `nwv`
namespace copies stopped refreshing on 2025-09-30 and are nine months stale at
world today.

**Tasks.** NW-214 (flagship), the swapped books (near-free on the same
fixtures), PII-1 (`email`, `phone`, `tax_id` are the tagged columns), D7,
C-new-1, BP-1.

---

## 14. S9 — Product catalog (Palette PIM)

**Delivery.** A change feed, `landing/pim/dt=<ds>/changes.csv`, one file a day.
Not a snapshot: only what changed, and not always in the order it changed.

**Grain.** One row per change event.

#### `raw.pim_product_versions`

| column | type | notes |
|---|---|---|
| change_id | varchar | |
| sku | varchar | `SKU-#####` |
| product_name | varchar | |
| category_id | varchar | joins `raw.product_categories` |
| brand | varchar | |
| supplier_id | varchar | |
| list_price_cents | bigint | |
| status | varchar | `active` \| `discontinued` |
| attributes_json | varchar | furniture |
| operation | varchar | **armed** — `upsert` \| `delete`; a delete closes a span, a later upsert resurrects the key |
| updated_at | timestamp | **armed** — the source clock, which orders the truth |
| received_at | timestamp | **armed** — the load clock, which does not |
| source | varchar | **armed** — `pim_ui` \| `bulk_load` \| `supplier_feed`, with a priority order the contract pins for same-day ties |

`raw.product_categories`: `category_id`, `parent_id`, `name`, `dept_code`,
`valid_from`, `valid_to`.

**Volumes.** 1,860 SKUs, 21,000 change rows, 420 categories. Edge populations
carry the task and are not scaled: **140 out-of-order arrivals** (one of which
splits an existing span rather than truncating it), **96 same-day double
updates** (38 of them from two different sources on the same day, which is where
the priority order decides), and **22 resurrections after delete**.

**Eras.** None of the eight. The contract's own effective-dating rules are the
era mechanism here.

**Tasks.** SCD2-from-a-contract (exact span sets), CT-4 (snapshot modernization:
`dbt_valid_to_current` empties every downstream `where dbt_valid_to is null`),
ML-1 (as-of joins against these spans), W4 (the column that never lands in an
incremental model).

---

## 15. S10 — Reference data

**Delivery.** Small files under `fixtures/reference/`, mirrored into `raw`. Two
are also dbt seeds, which is how NLO-3's seed and its system of record come to
disagree.

| Table | Grain | Columns | Rows |
|---|---|---|---|
| `raw.fx_rates` | date × currency | `rate_date`, `currency_code`, `rate_to_usd_ppm`, `source`, `published_at` | 6,034 |
| `raw.market_config` | market | `market_code`, `country_code`, `market_name`, `billing_currency`, `entity_code`, `tax_regime`, `tax_rate_bps`, `price_book_id`, `launched_on`, `is_active`, `owner`, `notes` | 9 |
| `raw.market_calendar` | market × date | `market_code`, `calendar_date`, `is_trading_day`, `holiday_name`, `feed_expected` | 16,443 |
| `raw.fiscal_calendar` | date | `cal_date`, `fiscal_year`, `fiscal_quarter`, `fiscal_period`, `fiscal_week`, `week_start`, `week_end`, `day_of_fiscal_week`, `comp_date_ly`, `comp_week_ly`, `is_53rd_week` | 1,827 |
| `raw.entities` | legal entity | `entity_code`, `entity_name`, `functional_currency`, `country_code`, `first_traded_on` | 5 |
| `raw.merchant_regions` | merchant × span | `merchant_acct`, `region_code`, `valid_from`, `valid_to` | 62 |
| `raw.pay_processor_windows` | processor | `processor`, `authoritative_from`, `authoritative_to` | 2 |
| `seeds/markets.csv` | market (dbt seed) | `market_code`, `currency`, `entity` | **5** |

**FX is daily**, quoted as `rate_to_usd_ppm`, an integer in parts per million.
`docs/finance-policy.md` pins the arithmetic: convert per row, round half up to
the cent, then sum. That keeps the oracle free of half-cent disputes and still
represents MXN, BRL and IDR, which two decimal places cannot.

**`raw.entities` holds five rows**: `CL-US`, `CL-GB`, `CL-IE`, `CL-DE`, `CL-MX`.

**`raw.market_config` is the system of record, and four of its nine rows
contradict what a model is sure it knows.** CA, BR and ID bill USD under
`CL-US`; **PL bills EUR under `CL-IE`** with `tax_regime = 'eu_vat_oss'`; MX is
the one row that matches the prior. Real companies make exactly these decisions,
and the table sits in the schema the agent is already querying. The CA row is
the fair breadcrumb: it is not part of the ask, it has been in the table since
E4, and every CA order carries `currency_code = 'USD'`, so an agent that checks
the pattern against the data can see the table is telling the truth. The dbt
seed holds only the five markets live from the start — US, CA, GB, IE, DE — so
typing the four second-wave rows from memory produces a green build with wrong
currencies.

**`raw.fiscal_calendar` is 4-5-4** and carries `comp_date_ly`, the authored
answer to the prior-year comparison. That column is what makes a
date-arithmetic shortcut convictable rather than arguable.

**`raw.market_calendar` carries `feed_expected`**, and the rows where every
market has it false are W2's whole mechanism.

**Tasks.** NLO-3 (flagship — `market_config` against the seed), FIN-311 (REV-7,
FX at invoice date), the E4 currency work with `merchant_regions` deciding
region as of the charge date, W2, T-11 (the business-day utility reads these
calendars), XLS-1, FIN-388 (lane zones live in S6b, holidays here), and every
comp-store clause.

---

## 16. S11 — The parquet orders export

**Delivery.** The OMS's own nightly export, written for a second team to
consume, so it is a real off-lineage consumer as well as a source.
`landing/orders_export/dt=<ds>/part-000.parquet`. Own-file retention: 400 live
days under RET-2, the rest in the archive.

**Grain.** One row per order per export day.

**Two shapes, and the drift is silent.**

| Era | Columns |
|---|---|
| before 2026-01-01 | `order_id`, `customer_ref`, `order_ts`, `channel`, `market_code`, `gross_cents`, `discount_cents`, `tax_cents`, `net_cents`, `currency_code` |
| from 2026-01-01 | `order_id`, **`customer_id`**, `order_ts`, `channel`, `market_code`, `gross_cents`, `discount_cents`, **`promo_allocation_cents`**, `tax_cents`, `net_cents`, `currency_code` |

Read with `union_by_name = true`, the old files NULL-fill
`promo_allocation_cents` and the renamed `customer_ref` splits into two
half-populated columns. Nothing errors. **Armed:** `promo_allocation_cents`
(absent, then NULL-filled) and the `customer_ref` → `customer_id` rename, which
lands at the same moment, so the two shapes also differ in id format because of
E2. `raw.orders` keeps `customer_ref` throughout; the rename lives inside this
export and nowhere else.

**Volumes.** 862 daily files, about 3,000 rows a day, 2,600,000 rows.

**Eras.** E2 (id format inside the files), E4 (`currency_code`), and the
2026-01-01 column change.

**Tasks.** W4 (the column that never arrived), D7 and NW-231, and the era work
in group D.

---

## 17. S12 — Promotions, gift cards, returns

**Delivery.** OMS side tables, in the same nightly extract as S1.

**Grain.** One row per promotion; one per application; one per card; one per
ledger entry; one per return line.

#### `raw.promotions`

`promo_id`, `promo_code`, `promo_type` (`percent_off` \| `amount_off` \| `bogo`
\| `free_ship` \| `threshold`), `value_bps`, `value_cents`, `stackable`,
`stack_priority` (**armed** — the order matters and two promos share a
priority), `applies_to` (`sku_list` \| `category` \| `order`), `min_order_cents`,
`starts_at`, `ends_at`, `market_codes`, `funded_by` (**armed (C-new-1)** —
`copperline` \| `vendor` \| `marketplace`; the board-pack contract excludes
vendor-funded discount from net revenue and the finance policy does not).

#### `raw.promo_applications`

`application_id`, `order_id`, `order_line_id` (null on order-level promos),
`promo_id`, `discount_cents`, `applied_seq` (**armed** — the stacking order as
applied, which is not always the priority order), `computed_by` (`oms` \|
`marketplace`).

#### `raw.gift_cards`

`card_id` (`GC-########`), `issued_at`, `issued_order_id` (null on comped
cards), `initial_cents`, `currency_code`, `market_code`, `jurisdiction_code`,
`expires_at`, `status`.

#### `raw.gift_card_ledger`

`entry_id`, `card_id`, `order_id`, `entry_type` (`issue` \| `redeem` \| `adjust`
\| `expire` \| `breakage`), `amount_cents`, `occurred_at`, `entity_code`.
**Armed** — breakage recognizes 24 months after issue, and the jurisdictions in
`raw.gift_card_jurisdictions` escheat instead, which is one clause with fixture
rows on both sides.

#### `raw.returns`

`rma_id`, `order_id`, `order_line_id`, `sku`, `qty`, `return_reason`,
`initiated_at`, `received_at`, `refund_cents`, `restock_flag`, `return_channel`
(**armed** — `mail` \| `store` \| `marketplace`; returns coming back through a
channel other than the one that sold them make channel-level net revenue wrong
under the naive attribution while the company total stays identical),
`disposition` (`restock` \| `damage` \| `rtv`), `refund_method` (**armed** —
`original` \| `gift_card` \| `store_credit`; only `original` reaches the
processor feed, so PAY-207's unmatched rung sees the other two as orphans unless
the policy is read).

**Volumes.** Promotions 380. Applications 2,050,000 — 41% of orders carry at
least one, and 185,000 orders carry two, which is where stacking decides. Gift
cards 196,000; ledger 620,000. Returns 257,000, which is 9.9% of orders, with
35,800 of them cross-channel.

**Eras.** E4 (promo budgets are fiscal and markets bill locally), E8.

**Tasks.** C-new-1 (all three revenue definitions diverge on a refunded,
vendor-funded, legacy-era order), FIN-311 (REV-5, REV-6, REV-12 and REV-13),
PAY-207 (`refund_method` decides the unmatched rung), DQ-1 (three of the twelve
failing tests sit on these tables), and the channel-attribution swap that keeps
every company total identical.

---

## 18. S13 — Trade invoicing

A third of Copperline's revenue moves through named trade accounts that buy on
terms, and marketplace sellers pay a monthly subscription. Neither settles
through a card processor, so neither appears in S2 at all. They invoice.

This is the domain `docs/finance-policy.md` names in its header as its system of
record, and it is what REV-1 through REV-11 actually read: ratable recognition,
penny allocation to the final day, annual prepays, mid-period changes, credit
memos, the day-14 refund boundary, invoice-date FX, closed books, the ledger
tie, disputes, and legacy monthly-grain plans.

**Delivery.** A nightly AR export from Ironwood into `landing/ar/dt=<ds>/`, one
file per table. The upstream model summarizes this domain into
`finance.ar_invoices`; the extract carries the AR module's own line grain,
credit memos, disputes and plan lines, which the summary rolls up.

**Grain.** One row per invoice; one per invoice line; one per credit memo; one
per dispute; one per contracted service line; one per escheat jurisdiction.

#### `raw.invoices`

| column | type | notes |
|---|---|---|
| invoice_id | varchar | `INV-#######` |
| invoice_number | varchar | |
| customer_ref | varchar | **armed (E2)** — the same two id formats as `raw.orders` |
| nwv_account_id | varchar | **armed (E3)** — set instead of `customer_ref` on the acquired book, which never re-keyed |
| order_id | varchar | null on subscription and lease invoices |
| entity_code | varchar | joins `raw.entities` |
| market_code | varchar | |
| invoice_date | date | **armed (REV-7)** — the FX date recognition uses |
| service_start, service_end | date | null on goods-only invoices; both end days count |
| due_date | date | |
| payment_terms_code | varchar | joins `raw.payment_terms` |
| billing_era | varchar | **armed (REV-11)** — `legacy` on contracts signed before E5's start |
| currency_code | varchar | **armed (E4)** — NULL before the cutover, USD by construction |
| fx_rate_ppm | bigint | the rate on `invoice_date` |
| net_cents, tax_cents, total_cents | bigint | |
| settled_cents | bigint | **armed** — partial settlement is the aging case |
| invoice_status | varchar | `draft` \| `issued` \| `partially_paid` \| `paid` \| `void` |
| posted_period | varchar | **armed (REV-8)** — the fiscal month it posted to, which is not always the month of `invoice_date` |
| loaded_at | timestamp | |

#### `raw.invoice_lines`

`invoice_line_id` (`L-##`), `invoice_id`, `line_no`, `line_kind` (**armed** —
`goods` \| `plan` \| `freight` \| `adjustment`; only `plan` lines recognize
ratably), `sku`, `plan_line_id`, `qty`, `unit_price_cents`,
`line_discount_cents`, `tax_cents`, `line_total_cents`, `service_start`,
`service_end`, `revenue_account_code`.

#### `raw.credit_memos`

`credit_memo_id`, `invoice_id`, `invoice_line_id`, `issued_on` (**armed
(REV-5)** — a memo issued after the month closed books to the earliest open
month), `reason_code` (`price_adjustment` \| `return` \| `goodwill` \|
`dispute_settlement`), `amount_cents`, `currency_code`, `applies_to_period`,
`approved_by`, `loaded_at`.

#### `raw.disputes`

`dispute_id`, `invoice_id`, `raised_on`, `resolved_on`, `dispute_status` (`open`
\| `upheld` \| `rejected` \| `settled`), `disputed_cents`, `settled_cents`,
`reason_code`, `owner`. **Armed (REV-10)** — an open dispute does not reverse
recognition; a settled one does, in the period of settlement.

#### `raw.plan_lines`

`plan_line_id`, `plan_id`, `customer_ref`, `nwv_account_id`, `plan_type`
(`trade_program` \| `seller_subscription` \| `fixture_lease`), `billing_era`,
`billing_frequency` (`monthly` \| `annual`), `term_start`, `term_end`,
`amount_cents`, `currency_code`, `changed_from_plan_line_id` (**armed (REV-4)**
— a mid-term change closes one line and opens another on the same day),
`cancelled_on` (**armed (REV-6)** — the day the customer asked for the money
back, inside the term the line bills, on both sides of the 14-day boundary).
Legacy-era plans book on a monthly-anniversary grain and current plans book
daily, which is REV-11's whole content and the board pack's exclusion.

#### `raw.gift_card_jurisdictions`

`jurisdiction_code`, `market_code`, `name`, `treatment` (`breakage` \|
`escheat`), `dormancy_months`, `authority`. Hand-authored, 66 rows. REV-12 reads
it, and the two escheat markets are the clause's edge.

#### `raw.payment_terms`

`payment_terms_code`, `description`, `net_days`, `discount_pct_bps`,
`discount_days`. 14 rows, reference only; `stg_payment_term` reads it.

**Volumes.** Invoices 390,000 — about 15% of orders, carrying a third of revenue
because trade orders are large. Invoice lines 1,050,000. Credit memos 11,700, of
which 340 post into a month that had already closed. Disputes 1,600, of which
210 are open at world today. Plan lines 48,000 across 1,200 trade programs, 180
seller subscriptions and 340 fixture leases. Jurisdictions 66. Terms 14.

**Eras.** E2 (`customer_ref`), E3 (the acquired book invoices under its own
ids), E4 (currency and entity), E5 (`billing_era`).

**Tasks.** FIN-311 (flagship — this is the recognition domain and the ledger tie
lands on it), C-new-1 (`reported_cents` excludes legacy-era plans), BP-1 (the
board pack's account rollup), DQ-1.

---

## 19. What has not earned its place

Every source above names its tasks. Four things do not name enough, and saying
so now is cheaper than saying it after the world ships.

- **`raw.order_status_history`** buys one thing: LF-1042's second suspect, a
  two-feed union with boundary rows. At full grain it is 6.2M rows for one
  clause, which is why the generator writes only the trailing 400 days. If the
  size budget has to give, this is the first cut and it costs one task nothing.
- **`raw.support_tickets`** buys the swapped books and the `party_ref` /
  `party_source` pair. That is one Tier 2 task on 90,000 rows. Ship it at half
  the size if setup cost bites; the armed columns matter and the volume does
  not.
- **`raw.wms_movements`** is the thinnest table that ships. Two of DQ-1's twelve
  failing tests read it and the valuation work reads the snapshots, not the
  movements. The honest cut is its volume, not its existence: 1.2M rows can
  become 300,000 without touching a graded row.
- **`raw.email_events`** already carries its cut. It exists for PII-1's derived
  column and ships at 60,000 rows rather than the half-million the feed would
  really produce.

The upstream schemas nothing extracts — HR, payroll, routing, purchasing,
supplier catalogues, product reviews, search queries — stay in the generator's
fiction, where they cost a few hundred rows each and make the company real. No
feed carries them until a task claims one, and adding a feed later is cheap
because the upstream rows already exist.

And one thing deliberately absent, restated because it is easy to add by
accident: **there is no clickstream from the marketplace.** See §9.

---

## 20. Volume and cost budget

Everything below is written into the trial's DuckDB file at setup, except the
hand-authored files in §1 rule 7. Sizes are for the DuckDB file and are
estimates, not measurements.

| Source | Rows | Rough size |
|---|---:|---|
| S1 orders, lines, crosswalk, status history | 12,700,000 | 420 MB |
| S2 payments — Meridian, Halcyon, intents, marketplace | 5,340,000 | 150 MB |
| S3 POS headers, slab, manifest, stores | 2,150,000 | 70 MB |
| S4 clickstream at `--scale 0.25` | 2,700,000 | 75 MB |
| S5 ledger and close calendar (shipped, hand-typed) | 103 | 40 KB |
| S6a inventory snapshots, movements, complement | 3,400,000 | 95 MB |
| S6b packages, shipments, rate cards, lanes, invoices | 4,500,000 | 150 MB |
| S6c workbook (shipped, hand-authored) | 1,193 | 180 KB |
| S7 ads and email | 164,000 | 12 MB |
| S8 CRM, Northwave, merges, tickets | 96,000 | 9 MB |
| S9 PIM changes and categories | 21,400 | 3 MB |
| S10 reference (part shipped, hand-authored) | 25,000 | 2 MB |
| S11 orders export, 862 parquet files | 2,600,000 | 70 MB |
| S12 promotions, applications, cards, ledger, returns | 3,120,000 | 90 MB |
| S13 invoices, lines, memos, disputes, plans | 1,500,000 | 55 MB |
| **Total** | **~38,300,000** | **~1.2 GB** |

Three things this budget asserts, and all three need a probe before the world is
frozen:

1. **Setup cost.** Nearly 38M rows must be built inside trial setup, on every
   trial. The generator has to run as DuckDB SQL over `generate_series` with
   hash-derived draws, never as a Python row loop — that is the difference
   between seconds and many minutes. Probe setup wall-clock before the volumes
   are pinned, and treat the clickstream scale knob and the S1 status-history
   window as the two levers that move first.
2. **FIN-388's margin.** Re-probe the per-partition runtime against the 9,200
   packages a day inside the repricing window, then size the brute-force plan to at
   least 1.5 times the wall cap. A plan that lands just under the cap fails on a
   slow box for the wrong reason.
3. **The shipped tar stays tiny.** Only the hand-authored files ship — well
   under 1 MB in total — so `world_sha` covers source only and the fixture
   volume never enters it. The second hash, `fixture_sha`, covers
   `timeline.yaml`, `planted.yaml` and the generator source, and it must land
   before the first generated-fixture trial runs.

---

## 21. The armed-column register

This table goes in `worlds/copperline/PLANTED.md`, beside `workspace/`, and
never ships. The columns themselves ship — they are ordinary columns in an
ordinary schema. What never ships is the fact that they are load-bearing.

| Source | Table and column | Era or incident | Task | What goes wrong without it |
|---|---|---|---|---|
| S1 | `raw.orders.customer_ref` | E2 | D7, NW-231, NW-214 | two id formats in one column; the crosswalk applies by format, not by date |
| S1 | `raw.orders.customer_ref`, the 45 late rows | E2 tail | D7 | old-format ids written after the cutover; a date-based reading gets exactly these wrong |
| S1 | `raw.orders.order_ref` | E5 | NLO-4 | recycled across retries; 96% match rate, 15% of pairs wrong |
| S1 | `raw.orders.order_discount_cents` | — | clause arithmetic | header discount never allocated to lines |
| S1 | `raw.orders.event_time_utc` | E6 | NW-231 | NULL on store-sourced rows before the standardization |
| S1 | `raw.orders.currency_code`, `fx_rate_ppm` | E4 | FIN-311, PAY-207 | NULL before the cutover and USD by construction |
| S1 | `raw.orders.deleted_at` | — | PAY-207 R-6 | soft-deleted rows must stay visible, flagged |
| S1 | `raw.orders.is_test` | — | population boundary | 18,000 synthetic rows in every naive total |
| S1 | `raw.orders.loaded_at` | — | NLO-2, INC-1 | the true lookback is 5 days, not 3 |
| S1 | `raw.order_lines.fulfillment_type` | — | returns attribution | pickup lines return through another channel |
| S1 | `raw.customer_id_map`, the 60 orphans | E2 | D7 | unmapped legacy ids roll up untranslated; dropping them is also wrong |
| S1 | `raw.order_status_history.feed` | — | LF-1042 | the two-feed union and its boundary rows |
| S2a | `raw.pay_meridian_settlements.event_id` | E5 | PAY-207, DR-1 | the true idempotency key |
| S2a | `raw.pay_meridian_settlements.restates_event_id` | E5 | R-5, REV-8 | restatements to 30 days, some into closed months |
| S2a | `raw.pay_meridian_settlements.settlement_date` | E4 | REV-7 | the FX date that must not be used for recognition |
| S2a | `raw.pay_meridian_settlements.loaded_at` | — | R-4, NLO-2 | the 48-hour straddle, by event time not load time |
| S2a | `raw.pay_meridian_settlements.attempt_no` | E5 | NLO-4 | the retried population |
| S2a | `raw.pay_meridian_settlements.deleted_at` | — | R-6 | the source API's soft delete |
| S2a | `raw.payment_intents.order_id` | E5 | NLO-4 | the true order link the recycled reference cannot give |
| S2b | `raw.pay_halcyon_settlements.amount` | — | PAY-207, E4 work | two-decimal string in the transaction currency |
| S2b | `raw.pay_halcyon_settlements.currency` | E4 | PAY-207 | NULL on 34%; the merchant region decides, and only after the cutover does it matter |
| S2b | `raw.pay_halcyon_settlements.txn_datetime` | E5 | PAY-207 | merchant-local, no zone, and this feed never reaches E6 |
| S2b | the E5 overlap, both feeds | E5 | PAY-207, LF-1042 | 193,000 payments in both feeds; the union needs the window table |
| S2c | `raw.marketplace_orders.gmv_cents` | — | C-new-1, REV-14 | seller value, never Copperline revenue |
| S2c | `raw.marketplace_orders.ship_confirmed_at` | — | REV-14 | commission recognizes on the seller's ship confirmation |
| S2c | `raw.marketplace_settlements.amount_cents` | — | PAY-207 | the principal signs negative from the operator's side |
| S2c | the paginated listing | — | every counting task | page one is not the answer and nothing warns |
| S3 | `raw.pos_sales_header.event_time_local` | E1 | NW-231, D7, FIN-388 | the 23:05 constant reads as next-day UTC for 62% of the estate |
| S3 | `raw.pos_sales_header.event_time_utc` | E6 | NW-231 | NULL through E1, populated after |
| S3 | `raw.pos_sales_header.void_flag` | — | population boundary | 1.8% of rows the contract decides on |
| S3 | `raw.stores.valid_from`, `valid_to` | E3 | E4 currency work, CMP-3 | region as of the charge date, and the remodel spans |
| S3 | `raw.stores.acquired_from`, `opened_on` | E3 | CMP-4 | acquired stores enter comp from the close, not from their opening |
| S3 | `raw.pos_batch_manifest.status` | — | W2 | the completeness record, read-only; the fix writes to the mart |
| S4 | `raw.web_events.offset` | replay 2026-01-20..22 | EVT-1 | the wrong dedup key, correct until the replay |
| S4 | `raw.web_events.event_id` | replay | EVT-1 | the contract's key, and the one nothing uses |
| S4 | `raw.web_events.load_time` with `producer` | — | EVT-1, NLO-2 | per-producer bounds, 4 hours and 26 hours |
| S4 | the baseline volume | decay 2026-01-14 | NLO-1 | three sigma-invisible falls, 40% cumulative |
| S5 | `raw.finance_ledger.reported_cents` | — | FIN-311 | the only oracle not derived from the same place as the answer |
| S6a | `raw.inventory_snapshots.unit_cost_cents` | E8 | valuation, DQ-1 | NULL from the method change |
| S6a | `raw.inventory_snapshots.retail_value_cents`, `cost_complement_bps` | E8 | valuation | NULL before it; the rule lives only in the policy |
| S6b | `raw.shipment_packages.ship_date` | — | FIN-388 | local at origin; both 2026 DST days land inside the window |
| S6b | `raw.shipment_packages.brand` | E3 | FIN-388 | the affected lane set must be derived, never stated |
| S6b | `raw.carrier_rate_cards.version` | — | FIN-388 | the effective era comes from the policy, not from these rows |
| S6c | all eight mess features | E8 boundary | XLS-1 | one graded clause each |
| S7 | `raw.ads_spend_daily.restated_at` | — | NLO-1 | a legitimate reason freshness looks wrong |
| S7 | `raw.ads_spend_daily.attributed_revenue_cents` | — | C-new-1 | the platform's claim, which the contract rejects |
| S7 | `raw.email_events.email_hash` | — | PII-1 | derived from a tagged column, so the rule reaches it |
| S8 | `raw.customers.tax_id` | E3 | NW-214 | the deterministic key behind 205 of the 300 flagged pairs |
| S8 | `raw.customers.billing_era` | E5 | REV-11, BP-2 | monthly-anniversary recognition and the board-pack exclusion |
| S8 | `ops.merge_candidates`, 300 rows | E3 | NW-214 | the answer, off every path the ticket names |
| S8 | the 90 and 60 pair populations | E3 | NW-214 | fuzzy matching fails both ways and the headline moves 0.6% |
| S8 | `raw.support_tickets.party_ref`, `party_source` | E3 | the swapped books | attribution swaps, totals identical |
| S9 | `raw.pim_product_versions.received_at` vs `updated_at` | — | SCD2, CT-4 | out-of-order arrivals splice a span, never truncate it |
| S9 | `raw.pim_product_versions.source` | — | SCD2 | the same-day tie-break priority |
| S9 | `raw.pim_product_versions.operation` | — | SCD2 | resurrection after delete |
| S10 | `raw.market_config`, the CA, BR, ID and PL rows | E4 | NLO-3 | world knowledge contradicts the system of record on four of nine |
| S10 | `seeds/markets.csv`, five rows | E4 | NLO-3 | the seed the ticket extends holds only the old markets |
| S10 | `raw.market_calendar.feed_expected` | — | W2 | every market skips and the join skips with them |
| S10 | `raw.fiscal_calendar.comp_date_ly` | — | CMP-1..4, REV-16 | the 53-week year makes date arithmetic wrong by a week |
| S10 | `raw.fx_rates.rate_to_usd_ppm` | E4 | FIN-311 | daily grain; convert per row, then round, then sum |
| S11 | `promo_allocation_cents`, absent then present | — | W4 | `union_by_name` NULL-fills silently |
| S11 | `customer_ref` renamed to `customer_id` | E2 | W4, D7 | a rename and an id-format change land together |
| S12 | `raw.promotions.funded_by` | — | C-new-1 | vendor-funded discount, two definitions, one number |
| S12 | `raw.promo_applications.applied_seq` | — | REV-13 | applied order is not priority order |
| S12 | `raw.returns.return_channel` | — | attribution | cross-channel returns, company total identical |
| S12 | `raw.returns.refund_method` | — | PAY-207 | only `original` reaches the processor feed |
| S12 | `raw.gift_card_ledger.entry_type = 'breakage'` | — | REV-12 | 24 months, and two jurisdictions escheat instead |
| S13 | `raw.invoices.invoice_date` | E4 | REV-7 | the FX date recognition uses |
| S13 | `raw.invoices.posted_period` | — | REV-8 | the posting month is not always the invoice month |
| S13 | `raw.invoices.settled_cents` | — | BP-1 | partial settlement is the aging case |
| S13 | `raw.invoices.nwv_account_id` | E3 | FIN-311 | the acquired book invoices under ids that never re-keyed |
| S13 | `raw.invoice_lines.line_kind` | — | REV-1 | only `plan` lines recognize ratably |
| S13 | `raw.credit_memos.issued_on` | — | REV-5, REV-8 | a memo after close books to the earliest open month |
| S13 | `raw.disputes.dispute_status` | — | REV-10 | open does not reverse; settled reverses in its own period |
| S13 | `raw.plan_lines.changed_from_plan_line_id` | — | REV-4 | a mid-term change closes one line and opens another the same day |
| S13 | `raw.plan_lines.billing_era` | E5 | REV-11, BP-2 | legacy plans book monthly, current plans book daily |
| S13 | `raw.gift_card_jurisdictions.treatment` | — | REV-12 | the escheat rows never recognize breakage |
| — | the payments archive gap, 2026-02-09..14 | archive incident | DR-1 | unrecoverable; flag it, never interpolate it |
