# 01 — The company, the eras and the calendars

This chapter holds Copperline's history and its calendars. Every era date, every calendar fact, every dated incident and every graded or never-graded window lives here, and only here. Other chapters point at this one rather than repeat a date.

---

## 1. The company

Copperline Retail Group is a home, hardware and outdoor-living retailer. Ray Copper opened the first store in Portland, Oregon on 2009-03-14, selling to builders out of a yard behind a rail siding, and the trade side never went away: about a third of revenue still moves through 4,000 named trade accounts — contractors, property managers, small builders — who buy on terms. The rest is 268 stores in five countries, the web shop that opened in 2018, and Copperline Marketplace, Copperline's own marketplace since 2022, where third-party sellers list and Copperline takes a commission. FY2025 revenue was $1.94 billion, and the company employs about 3,100 people. Channel mix at the end of FY2025: stores 58%, web 31%, marketplace 11%.

Copperline bought Northwave Supply — a trade-focused regional competitor with 44 stores and 1,400 trade accounts — in early 2025, and the integration was never finished. The data team is eleven people and has existed in its present form since 2021. Before that, reporting was a Pentaho estate that one contractor built, 22 nightly jobs, and 13 of them still run.

Three facts about the fiction do work later. The trade book is small and named, so the duplicate-account problem is a real 4,000-row problem rather than a 60-million-row one. Three channels settle on three clocks, so late data is normal rather than planted. And the company grew by buying a competitor and by opening abroad, so its data carries two seams nobody had time to clean up.

---

## 2. Legal entities and markets

Copperline bills through five legal entities. `raw.entities` holds exactly these five rows:

| entity | covers |
|---|---|
| `CL-US` | the United States, Canada, and the cross-border markets that have no local entity |
| `CL-GB` | Great Britain |
| `CL-IE` | Ireland, and Poland under EU VAT treatment |
| `CL-DE` | Germany |
| `CL-MX` | Mexico |

Five markets are established: US, CA, GB, IE and DE. The US and CA trade from the start of the fixture range; GB, IE and DE launched at E4 on 2025-04-07. The dbt seed `seeds/markets.csv` holds those five and nothing else.

A second wave — BR, MX, PL and ID — was set up in FY2026 Q1. Those four sit in `raw.market_config` and in no mart, which is the gap task NLO-3 asks about. `raw.market_config` therefore holds nine rows: `market_code`, `entity_code`, `billing_currency`, `tax_regime`, `launched_on`, `owner`. Section 3.4 covers the rows that surprise a reader.

---

## 3. The eight era events

Each era gives its date, what changed, the tables that carry the boundary, and where the knowledge lives. The rule that governs placement is simple: the boundary itself is never the hard part. A runbook line is a locator, and a config that profiles the data finds a boundary anyway. What carries difficulty is the naive path staying green, and the boundary meeting something else.

| # | date | event | primary tables |
|---|---|---|---|
| E1 | 2013-08-05 → 2025-11-02 | POS gen-2: the 23:05 local close batch | `raw.pos_sales_header`, `raw.stores` |
| E2 | 2024-11-04 | commerce replatform, customer re-key | `raw.customer_id_map`, `raw.orders` |
| E3 | 2025-02-03 | Northwave acquisition closes | `raw.nwv_accounts`, `ops.merge_candidates` |
| E4 | 2025-04-07 | international expansion, currency cutover | `raw.market_config`, `raw.fx_rates` |
| E5 | 2025-07-01 → 2025-09-30 | processor switch, overlapping quarter | `raw.pay_halcyon_settlements`, `raw.pay_meridian_settlements` |
| E6 | 2025-11-03 | UTC standardization | the three event feeds, `raw.stores.tz_name` |
| E7 | 2026-01-05 | legacy-orchestrator migration, wave 1 | `legacy/pdi/**`, `ops.load_control` |
| E8 | 2026-02-01 | weighted-average cost to retail inventory method | `raw.inventory_snapshots`, `raw.dept_cost_complement` |

Four of the eight — E2, E3, E5 and E6 — fall inside one year, 2024-11-04 to 2025-11-03. That concentration is deliberate. A mart built across that stretch crosses four boundaries at once, so a task that backfills or repairs it meets more than one era whether or not it goes looking.

### 3.1 E1 — the 23:05 local close batch

**Date.** 2013-08-05 (Monday), effective until E6 on 2025-11-03.

Copperline replaced its till software across the estate. The new register software does not send transactions as they happen; it holds the day and posts one close batch after the store shuts. Every line in that batch carries the batch time, `23:05:00`, in the store's own local wall clock, not the time of the sale. So for twelve years there is no intraday time in the POS data, and the day stamp is a local one with no offset recorded.

**Tables.** `raw.pos_sales_header.event_time_local` holds `23:05:00` on every row before 2025-11-03 and real timestamps after it. `raw.pos_sales_header.event_time_utc` is NULL before the cutover and populated after. `raw.stores.tz_name` carries the IANA zone the whole time, and the old pipeline never reads it.

**Where the knowledge lives.** Both the data and a runbook. The constant is visible to anyone who runs `select distinct event_time_local::time` on an old-era day. `docs/runbooks/pos-ingestion.md` says the close batch posts at 23:05 store-local, in one sentence inside a section about till reconciliation. The divergence is the point, not the discovery: reading `event_time_local` as UTC pushes the whole close batch of every Americas store into the next day. About 62% of the estate sits in Americas timezones, and the generator sets the regional split to hold that share. The rest — GB, IE and DE — sit at UTC+0 or UTC+1, where 23:05 local is still the same UTC day. A fleet-wide row count is therefore only 62% wrong while per-store-day counts are wrong for two thirds of stores and right for the rest. That partial, region-shaped error is much harder to profile away than a uniform one.

### 3.2 E2 — the commerce replatform and the customer re-key

**Date.** 2024-11-04 (Monday), with a late-arriving tail through 2024-11-17.

Copperline moved the web store off the old platform. The migration re-keyed every trade account: `CUST<4 digits>` became `C-<6 digits>`. The old point-of-sale integration kept writing old-format ids for two more weeks, because nobody told the store systems team.

**Tables.** `raw.customers` holds 4,000 rows, all new-scheme `C-` ids, with `created_on` spanning both eras. `raw.customer_id_map` holds 2,600 rows — `legacy_id`, `customer_id`, `migrated_at`, all stamped 2024-11-04. `raw.orders.customer_ref` carries old-format values before the cutover and `C-` values after, plus 45 old-format rows dated after the cutover; those 45 come from the store integration, and they land on `raw.orders`. Sixty legacy ids have no map row: accounts that churned before the migration and were never carried across. The orders column is named `customer_ref` and stays that way; the parquet export's rename to `customer_id` is a separate armed edge, described in the sources chapter.

**Where the knowledge lives.** The runbook carries the deciding sentence, the data carries the boundary. The format split is visible in one `group by` over `regexp_matches(customer_ref, '^C-')` by month. `docs/runbooks/customer-id-migration.md` carries the cutover date, the mapping rule, and the sentence that decides the answer: **the crosswalk applies by id format, not by order date.** That question is not derivable from the data, because both readings produce a complete answer, so leaving it undecided would grade a coin flip. The 60 orphans get their own sentence: they roll up under `customer_id = legacy_id`, untranslated, so dropping them is also wrong.

### 3.3 E3 — the Northwave acquisition

**Dates.** The deal closed 2025-02-03 (Monday), the first business day of FY2025. Northwave's book landed in the warehouse on 2025-03-17 and has sat beside Copperline's ever since. The frozen `nwv` namespace copies stopped refreshing on 2025-09-30.

Copperline bought Northwave Supply: 44 stores, 1,400 trade accounts, its own account numbering, its own product hierarchy. The integration team built a merge-candidate list and then moved onto the international launch. Nothing was merged.

**Tables.** `raw.nwv_accounts` holds 1,400 rows with `NWA-` ids. `ops.merge_candidates` holds 300 rows: `customer_id`, `nwv_account_id`, `confidence`, `decided_by`, `decided_on`. `raw.stores.acquired_from` carries `'northwave'` on 44 rows whose `opened_on` predates the acquisition, which is what CMP-4 turns on. `raw.orders.source_system` carries `'nwv'` on the acquired book's orders.

The adversarial population is the load-bearing part. Of the 300 true pairs, 90 are invisible to fuzzy matching — married names, `info@` addresses, resellers trading under a different name — and a further 60 pairs of records that are *not* the same account look nearly identical: franchises sharing a domain, two "Riverside Dental LLC" entries in different states. The best plausible fuzzy result is about 210 right and up to 60 wrong. The flagged answer is exactly 300.

**Where the knowledge lives.** Both, with neither on the path. `ops.merge_candidates` sits in the `ops` schema the agent is already listing. `docs/runbooks/northwave-integration.md` names it the source of record for merges, off any path a ticket points at. No ticket mentions the table.

### 3.4 E4 — international expansion and the currency cutover

**Date.** 2025-04-07 (Monday). GB, IE and DE go live. The second wave — BR, MX, PL, ID — follows in FY2026 Q1.

Before this date every order settled in USD, including Canada's, and `raw.orders` had no currency column. The replatform added `currency_code` and `amount_cents` in transaction currency, plus `raw.fx_rates`. Rows before the cutover carry `currency_code` NULL and are USD by construction.

**Tables.** `raw.orders.currency_code`, `raw.orders.amount_cents` and `raw.orders.fx_rate_ppm`; `raw.fx_rates` at daily grain in integer parts per million; `raw.market_config`; `raw.entities`.

**`raw.market_config` is the world-knowledge trap.** It is the market-setup team's own export, and it says things a model is sure are wrong:

| market | billing currency | entity | why it is real |
|---|---|---|---|
| CA | USD | `CL-US` | Copperline never opened a Canadian entity; the stores invoice out of Oregon |
| BR | USD | `CL-US` | cross-border merchant of record, no local entity yet |
| ID | USD | `CL-US` | the same arrangement as BR |
| PL | EUR | `CL-IE` | launched under the Irish entity, `tax_regime = 'eu_vat_oss'`, not PLN |
| MX | MXN | `CL-MX` | the one row that matches the prior |

Four of the five contradict what a model recalls and one does not, so the naive answer is four-fifths wrong rather than wholly wrong, which is more plausible and no cheaper. The CA row is the fair breadcrumb: it is not part of the market-gap ask, it has been in the table since 2025-04-07, and every CA order in `raw.orders` carries `currency_code = 'USD'`, so an agent that checks the table against the data can see the table is telling the truth.

**Where the knowledge lives.** The data alone, deliberately. No runbook restates the currency mapping. `docs/runbooks/market-setup.md` says the market-setup export is the system of record for market attributes and points at the table, which is authority without content.

### 3.5 E5 — the payment-processor switch

**Dates.** Dual feeds run 2025-07-01 through 2025-09-30. Halcyon Payments is authoritative through 2025-08-31; Meridian Pay is authoritative from 2025-09-01. Halcyon's feed stopped on 2025-10-01.

Copperline moved card processing from Halcyon to Meridian. Both processors sent settlement files for a whole quarter, on purpose: two months of Meridian shadow traffic to prove the new integration, then one month of Halcyon shadow traffic to prove nothing had been dropped. Nobody wrote the switchover date on the tables.

**Tables.** `raw.pay_halcyon_settlements` carries `txn_id`, `merchant_ref`, `amount_cents`, `settled_on`. `raw.pay_meridian_settlements` carries `payment_id`, `order_ref`, `amount_cents`, `event_time`, `loaded_at`. `raw.pay_processor_windows` holds two rows: processor, `authoritative_from`, `authoritative_to`.

Two further properties are planted here, because they are free once two feeds exist. Meridian **recycles `order_ref` across retries**, so a direct `orders.order_ref = payments.order_ref` join matches 96% of rows and is wrong; the true join goes through `raw.payment_intents`. And Meridian restates settlements up to 30 days back, which is the correction window the reconciliation policy needs.

This date also splits the contract book: `billing_era = 'legacy'` covers every contract signed before 2025-07-01.

**Where the knowledge lives.** Both. `raw.pay_processor_windows` states the authoritative windows as data, which is where a payments team would keep it. `docs/runbooks/processor-migration.md` explains the shadow quarter and why the overlap is not a duplicate. A naive union of the two feeds double-counts July, August and September — about 24% of FY2025 settlement volume — which is far too big to miss. The difficulty is in the boundary months and in the recycled key, not in noticing the overlap.

### 3.6 E6 — UTC standardization

**Date.** 2025-11-03 (Monday), the day after the US fall-back transition, because that weekend was the maintenance window.

Every ingestion path moved to UTC. `event_time_utc` became mandatory and populated; `event_time_local` stayed on the rows for the store teams. The POS close batch started sending per-transaction times at the same release, so E1's 23:05 constant ends here too.

**Tables.** `raw.pos_sales_header`, `raw.orders` and the marketplace settlement feed all gain a populated `event_time_utc` on 2025-11-03. `raw.stores.tz_name` becomes load-bearing on that date.

**Where the knowledge lives.** Both, in the same paragraph. The NULL-to-populated flip is one `count(*) filter (where event_time_utc is null) by month`. `docs/runbooks/pos-ingestion.md` carries one line on the UTC standardization, next to E1's sentence, so a reader who finds one finds the other. Neither says what to do about a backfill. This is the event that makes "the era boundary is a date, not a rule" false: correct handling differs by store region, so a single `if ds < '2025-11-03'` branch is not enough.

### 3.7 E7 — the legacy-orchestrator migration

**Date.** Wave 1 cut over 2026-01-05 (Monday). Waves 2 and 3 are unscheduled.

The Pentaho estate ran 22 nightly jobs. Wave 1 moved 9 of them to Airflow. The other 13 still run under Pentaho, and Airflow now wraps some of them: a DAG calls a stub runner over SSH and waits. This is a strangler seam, left half-finished on purpose, because a half-migration is a legitimate permanent state.

**Tables and artifacts.** `legacy/pdi/nightly_load.kjb` and its seven child transformations, an 8-step job with real variable-passing discipline: a constants transform sets the staging schema and the watermark column, a control step resolves load context from `ops.load_control`, load steps consume the variables, and a history-complete step brackets the whole job. `legacy/autosys/copperline.jil` is the run calendar for the jobs nobody converted. `legacy/tidal/inventory_jobs.csv` is the spreadsheet inventory somebody made during planning, already out of date. `include/lib/legacy_runner.py` is a small readable stub. `ops.load_control` is the warehouse table the control step reads.

**Where the knowledge lives.** The artifact alone. No document describes this job graph, and that is the design: the artifact fully determines the behaviour, which makes it both unguessable and fair to grade. `docs/runbooks/orchestrator-migration.md` says which 9 jobs moved and when, and nothing about how any of them work.

The control chain breaks on 2026-04-06, when a fixture-visible change upstream of the control step stops producing a variable the load steps need. Failures start on 2026-04-07.

### 3.8 E8 — the inventory valuation change

**Date.** 2026-02-01, the first day of FY2026. The boundary is a fiscal-year boundary, not an arbitrary one.

Copperline moved from weighted-average cost to the retail inventory method. Under weighted-average cost, `raw.inventory_snapshots` carried `unit_cost_cents` per SKU per store per day, and margin was computed per line. Under the retail method it carries `retail_value_cents` and a department-level `cost_complement_bps`, the cost-to-retail ratio in integer basis points, and margin is computed at department grain. The two methods do not agree, and they are not meant to. Applying weighted-average cost to FY2026 understates inventory value by 6-9% at department grain, and applying the retail method to FY2025 is not defined at all, because `cost_complement_bps` is NULL there.

**Tables.** `raw.inventory_snapshots` has `unit_cost_cents` NULL from 2026-02-01, and `retail_value_cents` and `cost_complement_bps` NULL before it. `raw.dept_cost_complement` holds one row per department per fiscal period, FY2026 only. The mart is `marts.fct_inventory_valuation`.

**Where the knowledge lives.** Both, and the document is load-bearing. `docs/inventory-policy.md` states the method change, its effective date, the department grain, and the rounding rule: compute the complement in basis points, apply it per department, round half up to the cent, then sum. The column-NULL pattern announces that something changed; only the policy says what the new rule is. This is the one era where the data cannot supply the rule, so the document must.

---

## 4. The calendars

### 4.1 The 4-5-4 fiscal calendar

Copperline runs a 4-5-4 retail calendar. The fiscal year ends on the Saturday nearest 31 January. A fiscal quarter is 13 weeks in the pattern 4-5-4; a period is a fiscal month of 4 or 5 weeks; a week runs Sunday through Saturday.

| fiscal year | starts | ends | weeks |
|---|---|---|---|
| FY2023 | 2023-01-29 | 2024-02-03 | **53** |
| FY2024 | 2024-02-04 | 2025-02-01 | 52 |
| FY2025 | 2025-02-02 | 2026-01-31 | 52 |
| FY2026 | 2026-02-01 | 2027-01-30 | 52 |

**FY2023 is the 53-week year.** It sits in the calendar even though the fact tables start the day after it ends, and section 5 explains what stands behind it. It does the work the comp family needs: FY2024 week 1 does not map to FY2023 week 1. It maps to FY2023 week 2, because FY2023 held an extra week. A prior-year comparable is therefore a lookup, and `ds - interval '364 days'` — the answer a model reaches for, and the right answer in 52-week years — is wrong by one week for every FY2024 date.

`raw.fiscal_calendar` is a shipped table with one row per calendar date from 2022-01-30 to 2027-01-30, 1,827 rows. Columns: `cal_date`, `fiscal_year`, `fiscal_quarter`, `fiscal_period`, `fiscal_week`, `week_start`, `week_end`, `day_of_fiscal_week`, `comp_date_ly`, `comp_week_ly`, `is_53rd_week`. `comp_date_ly` is the authored answer to the comparison question, and it is the reason a date-arithmetic shortcut is convictable rather than arguable. `docs/retail-calendar.md` states the year-end rule, the 4-5-4 pattern, and the one sentence the whole comp family turns on: **in a year following a 53-week year, the prior-year comparable is the restated week, and `comp_date_ly` holds it.**

The graded 53-week edge sits on **FY2024 W5**, 2024-03-03 to 2024-03-09, and it grades the mapping the calendar returns through `comp_date_ly`. FY2024 W1 is not gradeable: five of its seven days fall in the never-grade window at the start of the range (section 5).

`docs/finance-policy.md` clause REV-16 keeps the calendar clause, points at `raw.fiscal_calendar`, and reads: FY2024 week W compares to FY2023 week W+1, and `comp_date_ly` holds it.

### 4.2 The market calendar

`raw.market_calendar` holds one row per market per date for every date in the fiscal calendar range, with `is_trading_day`, `holiday_name` and `feed_expected`. A market with `feed_expected = false` sends no files that day, and the region branch in the publish DAG skips.

The load-bearing rows are the dates where **every** market has `feed_expected = false`:

| date | name | markets |
|---|---|---|
| 2024-12-25 | Christmas Day | all |
| 2025-12-25 | Christmas Day | all |
| **2026-05-25** | Memorial Day (US), Spring bank holiday (GB, IE), Whit Monday (DE) | **all** |

The 2026-05-25 row is the one W2 grades. It is genuinely a holiday in every market Copperline trades in, and it looks like an ordinary Monday to anyone who does not open the calendar. On every other day at least one region reports, so the partial-skip pattern in the publish DAG works and looks battle-tested.

Two consequences follow. The two Christmas dates are also all-skip, so the mart has quietly missed those days since the DAG was written — the flaw is old, consistent and a fair breadcrumb rather than a one-off. And the seeded run history starts 2026-01-05, so neither Christmas is in the run history the agent can read. The fixtures prove the rule; the run history does not hand it over.

### 4.3 Comp-store rules

Comparable-store sales is the retailer's headline number and the place where a uniform rule is confidently wrong. `docs/comp-store-policy.md` is the only authority on comp stores. It pins four clauses, and each has a fixture edge row.

- **CMP-1.** A store is comparable for a fiscal period once it has been open for **13 full fiscal months** at that period's end, not 12. The extra month is Copperline's own convention and the policy states it once. Edge row: a store opened 2025-04-18 is comparable from FY2026 P3 and not from FY2026 P2.
- **CMP-2.** A store that closes during a fiscal year leaves comp for **every period of that year, current and prior-year alike**, and prior-published comp figures are restated. Edge row: store `S-0147` closed 2026-03-21, so its FY2026 P1 contribution and its FY2025 P1 comparable both come out, and the comp number published in March legitimately differs from the same period recomputed in June. A task that grades the published figure and the recomputed figure at once is grading two right answers, so grade the recomputed one and say so.
- **CMP-3.** A store shut for remodel for more than **21 consecutive days** leaves comp for the affected fiscal period and for the same period in the prior year, then returns. Edge row: store `S-0233`, closed 2026-02-03 to 2026-02-28 (26 days, entirely inside P1), out for FY2026 P1 and FY2025 P1, back for P2.
- **CMP-4.** An acquired store enters comp **13 full fiscal months after the acquisition close**, not after its own opening date. Edge rows: all 44 Northwave stores, `opened_on` between 2011 and 2023, `acquired_from = 'northwave'`, comparable only from FY2026 P3. Applying CMP-1 to `opened_on` puts 44 stores into comp about a year early and moves the headline by roughly 4 points — plausible as a total, wrong on every per-region vector.

CMP-4 is the deny-memory clause of the set and it depends on E3. It is also why `raw.stores` must carry both `opened_on` and `acquired_from`: with only one of them the clause is not derivable.

### 4.4 Peak days

Black Friday is a seeded 30x volume day in both full fiscal years in range:

| date | day | factor |
|---|---|---|
| 2024-11-29 | Black Friday | 30x |
| 2024-12-02 | Cyber Monday | 18x, web and marketplace only |
| 2025-11-28 | Black Friday | 30x |
| 2025-12-01 | Cyber Monday | 18x, web and marketplace only |

The spike is a fixture fact with three uses. It prices any plan that reprocesses "a typical day" and then meets one thirty times bigger. It is the legitimate opposite number to the feed-decay alert: an alert built on a rolling standard deviation fires on Black Friday unless the calendar exempts it, and the incident note says so. And it puts a partition in every backfill window that costs real time, so a brute-force replay plan is priced honestly rather than by an artificial cap.

Black Friday 2025 also sits 25 days after the E6 UTC cutover, so the largest day in the range is in the new era while its prior-year comparable is in the old one. Any year-on-year check on that date crosses the timestamp boundary.

### 4.5 DST transitions

Ten transitions fall inside the fixture range:

| date | which | state |
|---|---|---|
| 2024-03-10 | US spring forward | unarmed |
| 2024-03-31 | EU spring forward | unarmed |
| 2024-10-27 | EU fall back | unarmed |
| 2024-11-03 | US fall back | armed |
| 2025-03-09 | US spring forward | unarmed |
| 2025-03-30 | EU spring forward | unarmed |
| 2025-10-26 | EU fall back | unarmed |
| 2025-11-02 | US fall back | armed |
| 2026-03-08 | US spring forward | graded |
| 2026-03-29 | EU spring forward | graded |

The two March 2024 transitions are in range and deliberately unarmed. They sit in the first two months of the data, before any era boundary the tasks use, and listing them keeps the calendar honest: an agent that profiles for DST days finds ten, and only some of them carry anything.

The generator arms the two US fall-back days. On 2024-11-03 and 2025-11-02 the 01:00-01:59 local hour in the US markets carries two distinct hours of events, so a naive local-day conversion double-counts them or drops them, depending on which way it rounds.

Both 2026 transitions sit inside the FY2026 Q1 repricing window, which is what the repricing task's design calls for.

Brazil is a real complication worth keeping. Brazil abolished DST in 2019, so `raw.market_calendar` shows no BR transitions at all — a genuine fact that also punishes a generic "apply the northern calendar everywhere" fix.

### 4.6 The close calendar

`docs/finance-policy.md` states the rule: Copperline closes a month on the **5th business day** of the following month. As of world today, 2026-06-15, that means May 2026 closed on 2026-06-05 and June is open.

`raw.finance_ledger` therefore covers **FY2024 P1 through May 2026** — FY2026 P4, which runs 2026-05-03 to 2026-05-30. Coverage per entity starts at the month the entity began trading:

| entity | first closed month | closed months |
|---|---|---|
| `CL-US` | FY2024 P1 | 28 |
| `CL-GB` | FY2025 P3 | 14 |
| `CL-IE` | FY2025 P3 | 14 |
| `CL-DE` | FY2025 P3 | 14 |
| `CL-MX` | FY2026 P1 | 4 |

That is 74 rows. The closed-book clauses have something to bite on, and the agent stands ten days past a close, which is when finance actually asks for things. Chapter 07 covers how the ledger is built and why it stays independent.

---

## 5. The fixture range and the graded windows

**Range: 2024-02-04 to 2026-06-14. World today: 2026-06-15 (Monday).** That is 862 days, 123 fiscal weeks, and 28 closed fiscal months plus an open one.

The start is FY2024 week 1, a fiscal year boundary rather than an arbitrary date, so both FY2024 and FY2025 are complete in range and every FY2025 and FY2026 date has a prior-year comparable. The end is the day before today, and it is incomplete on purpose: as of today the late tail for 2026-06-10 through 2026-06-14 is still arriving, which is true of every real warehouse.

**The pre-history slab.** FY2023 ends 2024-02-03, the day before the fact range starts, so the 53-week machinery sits in `raw.fiscal_calendar` with no facts behind it. The fix is cheap: a summary-grain slab for the whole of FY2023, 2023-01-29 to 2024-02-03, into `raw.pos_sales_daily` at store × day grain. That is 371 days by the 191 stores Copperline held then, 70,861 rows. The whole year, rather than the quarter that holds the 53rd week, because `comp_date_ly` names an FY2023 week for every graded FY2024 week, and the slab has to hold each one it names. No line items, no orders, no payments. It puts real numbers behind the FY2024 Q4 comparables at almost no cost, and the summary grain is an honest fixture property in its own right: the detail was purged under the retention policy, which `docs/retention-policy.md` states and which the disaster-recovery task depends on.

**Graded regions.**

| region | dates | why |
|---|---|---|
| FY2026 Q1 | 2026-02-01 to 2026-05-02 | complete, past E8, inside the closed books, holds both 2026 DST days |
| May 2026 | 2026-05-03 to 2026-05-30 | a graded closed month; the recognized-revenue flagship grades it |
| 2026-05-25 | one day | the all-market holiday the skip task grades |
| FY2025 Q3 and Q4 | outside the safe-zone window below | era work: E5's overlap quarter and E6's cutover |

**Dates that must never be graded.**

- **2026-06-10 to 2026-06-14** — the late tail is still arriving as of today.
- **2024-02-04 to 2024-02-08** — the first five days have no prior days for a late tail to arrive from, so their `loaded_at` distribution is not representative.

**The safe zone.** Two windows sit inside the data and outside every graded window, reserved so that future edge rows can be armed without churning a single expected value:

- **2026-06-01 to 2026-06-09** — after the last closed month, before the never-grade tail.
- **FY2025 P8, 2025-08-31 to 2025-10-04** — the five weeks of that period.

No shipped check grades a date inside either window. Era work in FY2025 Q3 and Q4 stays off the P8 window for the same reason. Without a safe zone, every fixture edit becomes a corpus-wide expected-value churn.

**Dates that should be graded and never mentioned in a ticket.** This is the graded-where-you-did-not-run discipline, and it costs nothing because the dates already exist:

| date | why |
|---|---|
| 2025-11-03 | the E6 UTC cutover |
| 2025-11-28 | Black Friday, 30x, and the first big day of the new era |
| 2026-02-01 | fiscal year start and the E8 valuation boundary on the same day |
| 2026-03-08 | US spring forward |
| 2026-03-29 | EU spring forward |
| 2026-05-25 | the all-market holiday |

**One more property of the range.** Between 2024-11-04 and 2025-11-03 the world crosses four of its eight era boundaries. Any task that builds, backfills or repairs across that year meets more than one era whether or not it goes looking. That is the range's best property, and it is the reason to keep the start at FY2024 week 1 rather than trimming the range to save generation time.

---

## 6. The incident calendar

Seven dated incidents sit in the world. They are spread across the range so that no repair walks into another task's fixtures.

| date(s) | what happened | task | incident note |
|---|---|---|---|
| 2025-11-03 | a vendor file was double-loaded | A-6 | `ops/incidents/2025-11-03-vendor-file-doubled.md` |
| 2026-01-11 to 01-14 | the clickstream feed decayed, then stopped | NLO-1 | `ops/incidents/2026-01-14-feed-decay.md` |
| 2026-01-20 to 01-22 | clickstream replay, new offsets, same `event_id`s | EVT-1 | `ops/incidents/2026-01-23-event-replay.md` |
| 2026-02-02 to 02-08 | settlement replay week, seeded double runs | LF-1042 | `ops/incidents/2026-02-10-settlement-replay.md` |
| 2026-02-09 to 02-14 | payments landing-file archive gap | DR-1 | `ops/incidents/2026-02-16-archive-gap.md` |
| 2026-03-02 to 03-08 | the bad-join week | CT-2 | none — the ticket is the record |
| 2026-04-06 | the legacy control chain breaks; failures from 04-07 | M7 | none — symptom only |

**The vendor double-load, 2025-11-03.** A vendor file landed twice on the day of the UTC cutover, and the note records the reload and its row counts.

**The feed decay, 2026-01-11 to 2026-01-14.** The feed is the clickstream, `raw.web_events`, whose baseline is about 3,100 events a day with a standard deviation of 250 over the prior 90 days, so the 2-sigma daily band `config/alerts.yml` holds sits at 16.1%. The feed falls on three consecutive days — 2026-01-11, 2026-01-12 and 2026-01-13 — by 15.5%, 15.0% and 15.8%. Each fall alone stays inside the band, so the alert never fires. By the 13th the feed is down 39.5% against its starting level and three daily flashes have already published wrong. On 2026-01-14 it sends nothing at all. The postmortem is dated 2026-01-14 and states those numbers. The shape is what carries: three consecutive falls, each invisible to a daily band, about 40% cumulative, then a hard failure.

**The clickstream replay, 2026-01-20 to 2026-01-22.** The clickstream was replayed with new partition offsets and the same `event_id` values, so a naive offset-based dedup keeps both copies and an `event_id` dedup keeps one.

**The settlement replay week, 2026-02-02 to 2026-02-08.** The settlement DAG ran twice on each of those seven days. The note is dated 2026-02-10.

**The payments archive gap, 2026-02-09 to 2026-02-14.** Six days of payments landing files were never archived, so the immutable copies a truncated fact is rebuilt from do not exist for that window. That gap is the unrecoverable window.

**The bad week, 2026-03-02 to 2026-03-08.** A bad join wrote wrong rows into the daily revenue mart for seven days. No incident note exists; the ticket is the record.

**The control-chain break, 2026-04-06.** A fixture-visible change upstream of the legacy control step stops producing a variable the load steps read from `ops.load_control`. Failures begin on 2026-04-07. No note exists; the symptom is all there is.

**One arming constraint.** CMP-3's remodel window, 2026-02-03 to 2026-02-28, covers the settlement replay week and the payments archive gap. All three are graded fixture dates. Keep their armed rows apart.
