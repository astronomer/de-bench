# The copperline answer key

This file never ships. The half above the marker is rendered by
`python -m gen_copperline --render-planted`; edit timeline.yaml or
planted.yaml instead of editing it.

## Eras

| era | when | note |
|---|---|---|
| E1_pos_local_stamp | 2013-08-05 to 2025-11-02 | POS headers stamp store-local time (23:05 batch close), no UTC column |
| E2_customer_rekey | 2024-11-04 | trade accounts re-key CUST#### -> C-######; crosswalk ships with 60 orphans; 45 old-format rows land after the cutover |
| E3_northwave_acquisition | 2025-02-03 | Northwave Supply acquired; its account book lands as raw.nwv_accounts and the nwv schema freezes without ever merging |
| E4_halcyon_non_usd | 2025-04-07 | non-USD volume begins settling through Halcyon (~30% of its volume after this date), in a feed format that never learned about currency |
| E5_processor_overlap | 2025-07-01 to 2025-09-30 | both processors feed settlements for the same payments during the migration quarter |
| E6_utc_cutover | 2025-11-03 | POS event times become UTC; E1 ends the day before |
| E7_orchestrator_migration | 2026-01-05 | the current scheduler deployment begins; run history exists only from this date |
| E8_valuation_change | 2026-02-01 | inventory valuation method changes at department grain |

## Clause-to-row map

No rows planted yet.

<!-- generated-half-ends: everything below is hand-written -->

## Library traps (include/lib), each stated in the docstring that owns it

| Trap | Where | The bite | Catalog task |
|---|---|---|---|
| Append-by-default loader | loaders.CsvToWarehouseOperator (`mode="append"`; replace without partition_col falls back with an INFO log) | a rerun doubles rows and looks fine | A-6 |
| nwv search-path order | warehouse.connect() attaches the frozen book first ('nwv,copperline') | an unqualified name reads September 2025's Northwave numbers; believable, not obviously wrong | G3 |
| Non-interval-scoped manifest read | pipeline.lake_task — `.output`/xcom gives the newest entry BY INTERVAL, whichever run wrote it; unknown kwargs (e.g. retries=3) become inert manifest tags | right on forward runs, wrong on replays; retry policy silently not applied | A-2 |
| No sensor timeout default | blueprint sensor_wait (the shipped TODO in docs/blueprints.md) | a rendered sensor waits forever unless its yaml says otherwise | W-family |

## Known world faults, not planted

- `projects/commerce/dags/blueprints.py` contains neither the word "airflow" nor a
  DAG definition of its own, so the DagBag's safe-mode heuristic skips the file and
  none of commerce's five blueprint DAGs is ever in the bag. Every copperline score
  on record was measured with this in place. No check names those DAGs
  (`task-copperline-0ee9f9` explains why), so fixing it changes no verdict, but it
  does change the world hash.

## Structural facts tasks lean on (built straight; the misuse is planted per task)

- channel_daily_intake's join is `none_failed_min_one_success`, with the docstring
  saying why `none_failed` alone publishes a day of zeros — W2
  (`task-copperline-7c41ab`) plants the misuse in its overlay, the world ships it
  correct. The observable difference is the partition file, not the mart: the
  build writes `marts.channel_daily` through `delete_insert` and publishes the
  same rows through `write_partition`. Under `none_failed` an all-market closure
  runs the build on an empty set, writes no mart row and still lands
  `include/data/marts/channel_daily_<ds>.csv` with nothing under the header.
  Under `none_failed_min_one_success` the build skips and no file lands.
  The all-market closures in the calendar are 2022-12-25, 2023-12-25, 2024-12-25,
  2025-12-25, 2026-05-25 and 2026-12-25; the task grades the last two of the past
  ones. Partial-skip days in the graded range: 2026-03-20/21, 2026-04-03,
  2026-04-05/06, 2026-05-01, 2026-05-03, 2026-05-14, 2026-05-24.
- `revenue.build_daily` takes the channel from the order and joins nothing — CT-2
  (`task-copperline-d38a7f`) plants the misuse in its overlay, the world ships it
  correct. The overlay joins the day's orders to `raw.stores` on
  `store_id AND is_current` and relabels any sale rung at a store with
  `acquired_from = 'northwave'` as `trade`. The relabelling is one row in, one row
  out, so the day's total does not move a cent: `tie_to_orders` ties,
  `check_cents` passes, `check_channels` still finds all four, and every run goes
  green. Only the split moves — `store` down and `trade` up by the same amount.
  Only store-channel orders carry a `store_id`, so the money moves in one
  direction. `raw.stores` holds 290 rows over 268 stores, 44 of them acquired, so
  the join needs `is_current` to stay 1:1 — without it 22 stores fan out and the
  tie breaks, which is a different (and loud) bug.
  The published partitions the task ships as evidence are real output of both
  builds: 2026-02-23 to 2026-03-01 from the correct one, 2026-03-02 to 2026-03-08
  from the overlay's. Graded days: 2026-03-04, 2026-02-25 and 2026-05-19, none of
  them named in the ticket.
- The OE- processor reference recycles every 50 days by construction. RULED FURNITURE
  after the wave-A build: `raw.payment_intents.order_id` carries the processor's own
  internal key and never resolves against `raw.orders.order_id` (0 rows on any cast) —
  a processor feed exporting PSP-side ids is everyday mess, and the honest order link on
  the Meridian side is `(order_ref, the attempt's own date)`, exact because the recycle
  spacing is 50 days and an attempt raises 1-2 days after its order. PAY-233
  (task-copperline-4b7c19) grades exactly that clock choice. Second-order furniture the
  same ruling covers: `projects/commerce/sql/recon_match.sql` matches nothing
  (`try_cast('ORD-…' AS BIGINT)` is NULL), so `payments_recon_daily` has filed every card
  order on the exception list every night without raising — a recon that never matched
  and nobody noticed. PAY-249 (task-copperline-e7c2a4) now takes that as its premise and
  grades the rewired match. Two measurements that task's author took, worth knowing
  before anything else touches this seam:
  * **No cast repairs the column, because the two ranges do not touch.** The digits in
    `raw.orders.order_id` run 1 to 3,436,578 and are dense; `raw.payment_intents.order_id`
    runs 8,840,127 to 181,202,656. `where order_id <= 3436578` is 0 rows, and joining on
    `try_cast(substr(order_id, 5) as bigint)` returns 0 rows over the whole range. So the
    prefix-strip repair is convictable on data, not only on the ruling.
  * **The date window is wide.** Every window from 2 to 45 days either side gives the
    identical match, row for row. At 49 the recycle collides and the row count rises by
    half again; with no window it is five to six times the honest answer.
  * `payments_recon_daily` cannot complete in the shipped warehouse — see the flaw
    inventory below for the two reasons.
- PAY-288 (`task-copperline-b45dd9`) is the written audit beside those two: it quantifies
  the recycle rather than repairing anything, and nothing about it is planted either. Every
  figure below was read off the baked warehouse by a throwaway `pytest_verifier`, so they
  are the shipped world's and not a local build's. What it measured, for whoever writes on
  this seam next:
  * **The book: 3,436,578 orders over 570,523 references.** 3,107,760 of the orders (90.4%)
    share a reference with at least one other; 328,818 hold one of their own; the most any
    single reference names is 18. The book runs 2024-02-04 to 2026-06-14, 862 days, which is
    a little over seventeen 50-day cycles — 133,787 references name 17 or 18 orders.
  * **The feed under a reference-alone join.** `raw.pay_meridian_settlements` holds
    2,058,581 rows; joined to `raw.orders` on `order_ref` alone it returns 29,968,433 (14.56
    rows out per row in), touching 3,096,290 distinct orders and 1,530,264,361,359 cents
    against the feed's own 105,596,982,367. 227,041 events reach exactly one order,
    1,831,540 reach more than one, **and none reach zero** — which is why nothing
    downstream has ever failed on it.
  * **§B-6's "about 96% of rows" is not a figure this data can produce.** 11.0% of the feed
    resolves to one order and 100% matches something. The clause also describes the wrong
    recycle: it has the within-payment retry reuse and misses the 50-day cross-order one.
    Both halves of that sentence are wrong and it is furniture, not a defect to repair.
  * **Every unambiguous event in the feed comes off one trading day, 2025-11-28.** The OMS
    hands out a 200,000-wide key block a day and an ordinary day uses about five thousand of
    it, so every reference an ordinary day issues is issued again 50 days later. 2025-11-28
    placed 146,721 orders and ran to offset ~130,000, past anything another day reaches, and
    141,941 of its orders carry a reference used nowhere else. Their settlements land in
    FY2025 P10 (198,260), P11 (27,554) and P12 (1,227) and in no other period, which is what
    makes the per-period column a gradeable find. The days either side resolve nothing at
    all. Same shape on 2025-12-01 (25,445 orders, 20,431 unique) and on 2024-11-29 /
    2024-12-02, which sit before the Meridian feed opens and so reach no settlement.
  * **The honest pairing's cardinality is corroborated by the dead column.** The
    reference-and-date match reaches 918,830 distinct orders, and `raw.payment_intents.
    order_id` carries exactly 918,830 distinct values behind the same events. The bridge is
    right about how many orders there are and useless about which. Counted over the whole
    attempt log rather than over the attempts the feed names it gives 920,929 — a near miss
    worth knowing before anyone grades that number.
  * One pair of orders in the book shares a reference at a gap that is not a multiple of 50:
    `OE-8840127`, 2024-11-29 and 2026-04-14, 501 days. Everything else is a multiple.
- The 12 dbt test failures: 9 stale against the April 2026 quality memo, 3 real (store-sales tie, dual-processor overlap, marketplace GMV reconstruction) — the split lives in worlds/copperline/DBT-NOTES.md.
- fin_ledger_tie assembles its table name from config; the spec's giveaway comment was deliberately NOT shipped.
- config/alerts.yml: three subjects owned by retired team names (the contract's open item is true).
- The two account books add up three ways, and NW-214 (task-copperline-9eabaf) grades the
  third. 3,960 active on raw.customers plus 1,380 active on raw.nwv_accounts is 5,340, the
  board pack's number. All 300 rows of ops.merge_candidates are active on both sides, so the
  defensible count is 5,340 - 300 = 5,040, split 3,960 Copperline and 1,080 acquired. Matching
  the books on a shared e-mail domain gives 248 pairs (205 true, 43 decoys), and 5,340 - 248 is
  5,092, the CRM's number — the gap is exact by construction, not by chance.
- The April control break is generated, and M7 (`task-copperline-be94e2`) grades it as it
  stands — nothing is planted for it. In `ops.load_control`: `watermark_column` is
  `loaded_at` on every row to 2026-04-05 and NULL from 2026-04-06; `load_status` is
  `complete` to 2026-04-06 and `failed` from 2026-04-07, for thirteen job names. The cause
  is one step earlier than the symptom: `legacy/pdi/trn/set_constants.ktr` takes the column
  from `${COPPERLINE_WATERMARK_COL}`, a kettle.properties setting on a box this tree cannot
  reach, and `resolve_load_control.ktr` coalesces the previous night's row with it, which is
  why 2026-04-06 still loaded and 2026-04-07 did not. The decoy in the same table is the
  wave 1 cliff: twenty-two jobs a night to 2026-01-04, thirteen from 2026-01-05.
- `sc_autosys_bridge.calendar_days()` has never read the AutoSys run calendar, and MIG-63
  (`task-copperline-a3c8e1`) grades the repair. The shipped reader skips a line only when it
  starts with `*`, so `/*` and `calendar:` both land in the answer; it takes
  `line.split()[0]`, so five dates in six are dropped; and it never turns `MM/DD/YYYY` round
  into the `YYYY-MM-DD` a `ds` is written in. Fifty entries, none of them an ISO date, so
  `is_due` has answered no to every date there is. The export names 251 days, 2026-01-02 to
  2026-12-31, and `raw.market_calendar` at `market_code = 'US'` names the same 251 for 2026
  — the two agree exactly, which is what makes the "read the file, do not re-derive the
  rule" line in the ticket a fair one to draw. The same ticket grades the four things
  `legacy/tidal/inventory_jobs.csv` is wrong about, all of them by construction and none of
  them planted: it marks `nightly_stock_ledger` and `nightly_customer_master` as wave 1
  against `sc_stock_ledger_daily` and `cus_customer_master_daily`, two dag_ids that exist
  nowhere, while both boxes are still `insert_job` lines in `copperline.jil` and still write
  `ops.load_control` rows after the cutover; and it lists `nightly_edi_852`,
  `cpl.stage.erp.costing` and `nwv_planogram_feed`, which appear in that sheet and in no
  other file in the world. `cpl.util.archive.logs` carries `run_calendar: retail_2025`, a
  calendar the estate does not hold, under `status: OI`.
- `sc_autosys_bridge.trigger_nightly` hands the runner `variables={"BUSDATE": ds}`, and MIG-96
  (`task-copperline-a6d2f8`) grades the repair and the audit. `copperline.jil`'s header says
  `BUSDATE` "is yesterday"; `ops.load_control` says the same from the data, with `started_at`
  in the 01:00 hour of `business_date + 1` on all 15,577 rows and no exception on either side
  of the cutover; and `CONVENTIONS.md` makes `{{ ds }}` the day the run FIRES. So the bridge has told
  the estate to work a night that has not closed, on every night since the deployment began.
  The audit is exact and derived: 161 business dates from 2026-01-05 to 2026-06-14, the last
  the table holds; 112 of those nights are `retail_2026` days, so the tie-outs and the
  distribution packs started on them; the shift takes 23 business dates off the front
  (20 Sundays and the three Mondays the calendar leaves out — 2026-01-19, 2026-02-16,
  2026-05-25) and puts 23 Fridays on the back, 2026-01-09 to 2026-06-12. That inverts what
  MIG-84 (`task-copperline-e4b7c2`) reports, and both are true: MIG-84 answers for
  `nightly.profile` on the old scheduler, this one for the bridge that has supplied `BUSDATE`
  since the cutover. Nothing has gone red because `check_jobs_wrote` reads the control table
  at the same `ds` the trigger passed, so the two are out by the same day; MIG-96 pins that
  step to `{{ ds }}` and asks for it in the write-up instead, because moving it changes what
  supply-chain read every morning.
- `sc_pdi_nightly_load.REQUIRED_VARIABLES` now names the four variables
  `resolve_load_control.ktr` actually exports — `LOAD_ID`, `STREAM_NAME`,
  `LOAD_FROM_COLUMN`, `LOAD_FROM`. It shipped naming `WATERMARK_VALUE`, which nothing
  exported; fixed in the second platform batch, world and M7 solution together.
- raw.market_config disagrees with world knowledge on four of its nine rows, and NLO-3
  (task-copperline-eebd1f) grades the disagreement. BR and ID bill USD under CL-US, PL bills
  EUR under CL-IE on eu_vat_oss, MX is the one second-wave row that matches the assumption.
  CA is the breadcrumb: USD under CL-US since 2014, in seeds/markets.csv already, and not
  part of any ask. docs/runbooks/market-setup.md MKT-1 makes the table the record and says
  no document restates it, which is what makes the typed-from-memory answer convictable.
  seeds/markets.csv ships with the five first-wave markets and gro_market_seed_build's
  `where` pins the same five, so the seed and the job have to move together.

- FIN-311 (task-copperline-c03aad) grades the FY2026-P04 build-up, and nothing about it is
  planted: it is the policy applied to the built data. May 2026 is FY2026-P04, 2026-05-03 to
  2026-05-30, closed 2026-06-05. The eight steps of the worked month, all entities, in cents:
  3,712,115 / 4,503,826 / 9,006,594,420 / 79,621,294 / 44,755,895 / -217,620,158 / -5,329,569
  / -118,166, which add to 8,916,119,657 — the sum of the five FY2026-P04 rows of the shipped
  ledger. The ledger holds those five totals and no decomposition, which is what makes the
  build-up underivable from it. Both books agree on every step per entity:
  `tools/copperline_oracle` and `tools/copperline_ledger_workings/close.py --month FY2026-P04`.
  The task's checks.yaml carries the figure each wrong reading produces.
  One adjudication after the first matrix run: for a line REV-6 voids mid-month, the policy
  fixes what steps 1 and 8 net, not the gross split. The gross reading books the voided
  lines' May days in step 1 (3,740,503) and reverses recognized-to-date in step 8
  (-146,554); the net reading is 3,712,115 and -118,166. Same 28,388 cents, same month,
  same total — the checks accept both pairs.

- CA-2 (`task-copperline-a41d9e`) plants the alerting runbook's own prescription in its
  overlay: `plat_secret_rotation_check` loses `on_failure_callback=notify(...)` and the
  `include.lib.notify` import, and gains `email`, `email_on_failure` and `email_on_retry`
  in `default_args`. The world ships the DAG correct. Airflow takes a `default_args` key
  only where the operator signature carries a parameter of that name, so the three are
  accepted, dropped and never acted on — measured, not assumed: the task's graded run
  fails and writes nothing under `include/data/_notifications/` on the overlay tree, and
  writes the record on the fixed one. `ops/runbooks/alerting.md` is unchanged and stays
  the trap; the ticket points straight at it, which is the bet 00-overview line 114 asks
  to probe. Nothing else in the world is touched.
- PAY-207 (task-copperline-a41d7e) grades FY2025-P08 on the cards, and nothing about it is
  planted: it is the three payment documents applied to the built data. The month runs
  2025-08-31 to 2025-10-04, so the switchover in `raw.pay_processor_windows` cuts it after
  one day — Halcyon owns 2025-08-31 and Meridian owns the other thirty-four. The month holds
  87,475 payments (2,542 Halcyon, 84,933 Meridian) worth 3,100,537,937 USD cents,
  771,950,330 GBP and 952,225,300 EUR; 46,946 rows inside the month are shadow traffic and
  are dropped (44,404 Halcyon rows after its window, 2,542 Meridian rows before its own);
  1,455 of the month's payments a later event restates; 261 carry `deleted_at` and stay in
  under R-6; 80,225 orders sit behind the payments, 77,904 of them Meridian's.
  Two properties of the built data make the month gradeable and are worth knowing before any
  other task touches this seam:
  * **The two feeds agree exactly on 2025-08-31** — the same 2,542 payments, the same cents
    in each of the three currencies — because `sim_ext` builds both from one captured-payment
    set. So a reply that ignores the windows table and works the whole month off Meridian
    lands the same total. Only the Halcyon-side figures separate it, which is why the task
    grades the 849 blank-currency rows: that column exists on no other feed.
  * **A restatement never moves an amount.** `upstream.external` gives the restating event
    the capture's own amount, so R-5's date rule changes the count and never the money. The
    task grades the count for that reason.
- LF-1042 (`task-copperline-e7c30b`) grades the settlement publish, and nothing about it is
  planted: `projects/commerce/sql/settlement_publish.sql` ships as a bare
  `INSERT INTO copperline.marts.settlement_weekly` under a comment saying it replaces the
  week, which `CONVENTIONS.md` "Writing to the warehouse" and
  `contracts/settlement-summary.md` SS-3 both call a defect in as many words. The second
  candidate on the same path is `apply_processor_boundary`, and it is a real dead step:
  `settlement_week_lines.sql` stamps every row `'marketplace'` and
  `settlement_boundary.sql` deletes only `feed = 'processor'`, so it has never removed a
  row. It must survive — the overlap it is written for ended 2025-09-01 (B-8) and the week
  that doubled is February 2026. Three facts of the built data the task rests on:
  * Every `raw.marketplace_settlements.payout_date` is a **Monday**
    (`upstream/external.py` says Friday and its own arithmetic says Monday; refunds land 21
    days on, so also Monday). A fiscal week runs Sunday to Saturday, so a week holds exactly
    one payout batch per seller.
  * `week_bounds(ds)` is `(week_start(ds - 1), ds - 1)`, so a **Monday** `ds` covers the
    Sunday alone and summarises nothing. The two graded run dates, 2026-02-08 and
    2026-05-16, are deliberately not Mondays.
  * All seven dates seller-ops re-ran, 2026-02-02 to 2026-02-08, resolve to the one fiscal
    week that opened 2026-02-01 — FY2026 W1. That is the find the RESPONSE grades, and it
    needs no run history to check.
- LF-1103 (`task-copperline-d0b9e3`) grades the same summary's held-back columns on the dbt
  side, and nothing about it is planted either:
  `dbt/copperline_analytics/models/commerce/settlement_weekly.sql` ships as
  `sum(s.fulfilment_fee_cents + s.refund_cents) as fee_cents`. `int_settlement_matched`
  pivots on `magnitude_cents`, so the credit arrives with its direction stripped off, and
  adding it puts it on the wrong side: `commission_cents + fee_cents` comes out over by two
  credits on the 4,806 of 22,266 seller-weeks that hold one. Three facts of the built data
  the task rests on:
  * A `refund` line is minted as `-commission_amount` (`upstream/external.py`), so it is a
    reversal of the commission and signs negative like the principal. Which of the two
    contract columns it comes off is not settled anywhere, so the task grades the pair
    together and lets either reading through.
  * The summary is cut on `placed_date` and a refund rides a payout run 21 days after the
    one that carried the commission, so a credit restates a week that closed a month
    earlier. That is the third line the RESPONSE grades.
  * The pivot's double count of a refunded order is DQ-91's, not this task's. The
    order-value check takes the week's GMV counted once an order or once a payout, so a
    reply that repairs both passes and a reply that repairs neither is not convicted for it.
- `raw.pay_halcyon_settlements.txn_datetime` is merchant-local and the offsets are +8 (GB,
  IE) and +9 (DE) against UTC, but every capture the simulation mints falls between 03:10
  and 13:00 UTC, so no Halcyon row anywhere in the feed sits on a different local date from
  its UTC one. The zone is recoverable and real; it moves nothing across a day boundary.
- D7 (task-copperline-4b1e7c) asks for a backfill of `ops.customer_ref_resolved` across the
  E2 re-key, and nothing about it is planted: it is the rule applied to the built data. In
  2024-09-01..2024-12-31 there are 25,456 orders carrying a `customer_ref` — 13,314 in the old
  scheme and 12,142 in the new. All 45 late old-format orders sit in the window (44 distinct
  refs, 43 of which also have `C-` orders, so a date-based reading splits 43 accounts in two);
  so do 206 of the 812 orders belonging to the 60 untranslatable legacy ids. Measured against
  the built world, a date-gated crosswalk puts 44 orders on the wrong account, dropping the
  fallback empties 206, and doing both misses 44 + 205. The rule is written down four times —
  `docs/runbooks/customer-id-migration.md` CID-2/CID-3, `projects/customer/lib/identity.py`,
  `projects/customer/dags/cus_crosswalk_apply_daily.py` and
  `dbt/.../int_customer_resolved.sql` — and the ticket names none of them, so finding it is
  the task. `ops.customer_ref_resolved` is not in the shipped warehouse, so the backfill
  creates it.
- PAY-251 (task-copperline-b2f9c4) grades the Meridian feed's late tail, and nothing about it
  is planted: it is `late_tail.default` applied to the built feed. `date_diff('day',
  event_time_utc, loaded_at)` reads the drawn lag back exactly — 0.924 / 0.041 / 0.018 /
  0.009 / 0.005 / 0.003 over days 0 to 5 — because `_boundary.loaded_at` holds a row for
  whole days and `meridian.py` lands it on an hour drawn across the arrival day, never
  earlier than the hour after the event. So a settlement day is complete on its fifth night
  and no sooner; a three-day window loses about 1.7% of every day's rows, one-directionally,
  which is about a per cent of the day's money. Graded settlement days 2026-04-20 to
  2026-04-26, over the delivery nights 2026-04-20 to 2026-05-01, and no other date.
  Two properties of the built feed the task leans on:
  * The event vocabulary in the data is `authorized`, `captured`, `refunded` and
    `chargeback`. Spec 03 §7 also lists `void` and `chargeback_reversed`; no row carries
    either.
  * `amount_cents` signs negative on `refunded` and `chargeback` and positive on the other
    two, so a day's three money columns do not need a sign convention imposed on them.

- DR-1 (task-copperline-b7d3e1) grades a restore out of `landing/meridian/`, and nothing
  about it is planted: the restore path ships broken and the ticket asks for it working.
  The facts a sibling author should know before touching this seam:
  * **The Meridian landing tree is 90 days of ARRIVAL dates, `dt=2026-03-17` to
    `dt=2026-06-14`** (`extracts/meridian.py::_landing`, RET-1). It takes no key dates.
    `dt` is `loaded_at::DATE`, and `settlement_date` runs from three days before the file
    date to more than forty days after it, so a settlement window is never a set of
    directories. The tree holds 75,067 events settled between 2026-02-01 and 2026-03-31
    against 318,039 in `raw.pay_meridian_settlements` for the same window — the difference
    is the archived days, and the 75,067 are the late tail that arrived after the tree's
    edge. 144 of them are settled inside the archive gap's six days, which is not a
    contradiction: the gap is six days of FILES, and those events arrived on other days.
  * `docs/retention-policy.md` and `ops/incidents/2026-02-16-archive-gap.md` are the only
    record of the archive and the gap. Nothing in the tree distinguishes "archived" from
    "gone" — the generator says so on purpose (`extracts/_landing.py`), and the three-way
    classification is what the task's written half grades.
- `raw.pay_meridian_settlements` and the landing tree agree exactly over the tree's 90 days:
  486,282 rows on both sides, one row per `event_id`, no duplicates and no NULL
  `settlement_date`. A rebuild from the files reproduces the table's rows for any window the
  files cover.
- STO-419 (task-copperline-e7c3b1) asks for a store trading-day pack across the E6
  standardization, and nothing about it is planted: it is the two POS era facts applied to
  the built feed. It is the spec's NW-231 rebuilt on the seam D7 leaves alone. Spec 03 gives
  NW-231 three rows — `raw.orders.customer_ref` over E2, `raw.pos_sales_header.
  event_time_local` over E1 and `event_time_utc` over E6 — and D7 (task-copperline-4b1e7c)
  already grades the first whole. So this one takes the other two: a different consumer, a
  different window and the timestamps rather than the crosswalk. Spec 03 §8 ("backfills into
  E1 shift whole days") is what it is built from. The window is FY2025 P09 and P10, 2025-10-05 to 2025-11-29, which holds
  14,317 store-days over 197,823 transactions. 59,789 of those predate 2025-11-03 and carry
  a NULL `event_time_utc`; 37,319 of them (62%, over 163 of the 260 stores that trade before
  the cutover) sit on a night the 23:05 close batch reads as the next day once converted through
  `raw.stores.tz_name`; and 24,319 of the 138,034 after the cutover carry a UTC date that is
  not the trading day. So no timestamp gives the day on either side and `business_date` gives
  it on both. Measured against the built world: keying on `event_time_utc` loses 7,473
  store-days, invents 283 and gets 4,126 wrong; converting the local stamp loses 186, invents
  307 and gets 8,639 wrong, and `coalesce(utc, converted local)` lands the identical answer
  because after the cutover the two are the same value; joining `raw.stores` without picking
  a version doubles 1,161 store-days silently (the market is the same on both rows, so
  nothing else moves); building off `raw.pos_batch_manifest` invents 74 store-days that no
  register reported; dropping the 36 store-days whose claimed count disagrees with what
  loaded loses them. The rule is written down three times — `docs/runbooks/pos-ingestion.md`
  POS-2 and its "things that look like problems and are not" list, the docstring on
  `stg_store__pos_sales_header`, and `agg_daily_store_sales`, which groups the till on
  `business_date` — and the ticket names none of them.
  One property of the built feed the task leans on: `event_time_local::date` equals
  `business_date` on every row of the feed, before the cutover and after it, so a job that
  reads the local stamp's date lands the same answer and is not convicted for it.
- DQ-96 (`task-copperline-d4f60b`) measures `tie_channel_daily_store_pos`, and nothing about
  it is planted: it is the shipped tie meeting the shipped feeds. The finding is that the
  header on `agg_daily_store_sales` is two thirds wrong about its own model, and anyone
  building on that model should know it. The tie reports
  `till_net_cents - (booked_cents - order_discount_cents)`. `raw.pos_sales_header.net_cents`
  is `gross - discount + tax` and `booked_cents` is `subtotal_cents`, which is before tax,
  so the two sides are not the same quantity. Measured on the full-profile build over the
  whole range, on the 213,408 store-days that carry both sides:
  * the gap is +7,389,678,723 cents and the tie fails on 213,232 of the 213,408, so 176
    store-days pass. It is estate-wide, not a tail.
  * **tax +8,390,741,710**, on the transactions the till kept. Not named in the header.
  * **voids -1,596,054,634**, over 33,463 voided transactions. The one reason the header
    gives that carries money.
  * **the orders the spine drops +594,991,647**, over 12,481 of them — 9,659 `is_test`
    orders worth 459,552,258 and 2,822 `deleted_at` rows worth 135,439,389.
    `stg_sales__orders` drops both and the registers sent both. Not named in the header.
  * the three add to the gap to the cent, and the verifier asserts that identity before it
    grades anything.
  The header's other two reasons put in nothing, and both are provable from the feed:
  `raw.pos_sales_header.business_date` equals `raw.orders.local_order_date` on all
  1,776,598 till transactions that carry an order id, so the 23:05 trading day moves nothing
  here (it moves plenty in STO-419, which converts a stamp; this tie converts none); and
  neither side of the comparison reads `event_time_utc`, so the E6 seam never enters the
  arithmetic. The emulation the verifier grades against was checked with `dbt run` at the
  small profile, where it reproduces the model's own 69,313 store-days, 64,433 failing rows
  and 352,844,153-cent total to the cent. Found by the DQ-96 author.
- SC-1204 (`task-copperline-f2a9c1`) grades weekly shrink across E8, and nothing about it is
  planted: it is the shipped job meeting the shipped feed.
  `sc_shrink_weekly` values a counted delta at `coalesce(s.unit_cost_cents, 0)`, and E8
  stopped `unit_cost_cents` arriving, so `marts.shrink_weekly.shrink_cents` has been 0 on
  every row of every week from 2026-02-01 and every run has gone green — rows land,
  `counted_delta` moves, `check_counts_posted` finds a row for every location that counted
  and `report_worst_locations` prints five zeros. The facts the task rests on, all of them
  measured against the full-profile build:
  * **A fiscal week never straddles E8.** 2026-02-01 opens FY2026 and it is a Sunday, so
    a week is wholly on one method or wholly on the other, and the DAG's own
    `week_covered` picks the rule. The 2026-01 weeks are the ones INV-4 forbids restating.
  * **`retail_value_cents` is the position's retail, not a unit price.** The generator
    writes `qty_on_hand * unit_retail` and `fct_inventory_valuation` puts it against
    `on_hand_units * unit_cost_cents` with no units multiplier. So the retail method needs
    `retail_value_cents / on_hand_units` before a counted delta means anything, and reading
    the column as a price is out by about 800x. That inference is the substance of the
    ticket, and the ticket carries one line saying the new columns describe the position.
  * **The complement is written down twice and the two drift.** The snapshot row carries the
    department base (`DEPT_COMPLEMENT_BPS`, no drift) and `raw.dept_cost_complement` carries
    the period's approved value, base plus a drift of -10 to +10 bps. At most 0.14% apart on
    any graded row, so the task admits either and grades at 1% (500 cents floor).
  * **The nearest wrong method is 5.7 to 8.1% out.** Carrying a SKU's last FY2025
    `unit_cost_cents` into FY2026 lands inside `DEPT_UPLIFT_BPS` of the retail answer on
    every row — which is the spread INV-4 says the two methods differ by — so the tolerance
    has a five-fold margin over it.
  * Every cycle_count `qty` in the world is negative, so a week's shrink is negative money
    and a positive figure is the position valued instead of the loss.
  * The match is sparse and the ticket puts it out of scope: `s.snapshot_date =
    m.occurred_at::DATE` matches 9,423 of 59,712 counts over the whole range, because the
    network snapshot is weekly (Sundays) and only the trailing ninety days are daily. The
    graded weeks match 72 of 478 (2026-01-11), 61 of 473 (2026-02-15) and 149 of 515
    (2026-05-10). Widening the match moves every figure, which is why the ticket names it
    as somebody else's ticket.
  * Graded weeks 2026-01-11, 2026-02-15 and 2026-05-10, run through the Thursdays
    2026-01-22, 2026-02-26 and 2026-05-21. The ticket names none of them. Weekly totals
    under the shipped build over 2025-11-02..2026-01-25 run $170,321 to $278,078, which is
    the derivation of the one figure the ticket states.

- RTN-266 (task-copperline-c8f42d) grades returns netted onto the day that sold them, and
  nothing about it is planted: it is the returns feed applied to the built data. What a
  sibling author should know before touching this seam:
  * `raw.returns` holds 154,341 rows, **one per RMA, one RMA per order and one per order
    line**. Every row resolves to an order and to a line on (`order_id`, `order_line_id`),
    with no orphans on either side.
  * An RMA is raised **3 to 60 days** after its order — 80.5% inside 21 days, the rest
    spread thinly over the two months after. So a night's returns file names order days
    across two months, and a 30-day window holds about half of them. The reach is the
    number the task is really about, and it is nothing like the 5-day tail PAY-251 measures
    on the settlement feed.
  * `order_line_id` is a line ordinal: **12 distinct values over 9,430,789 lines**. That is
    what makes the ordinal joins in the flaw inventory below catastrophic, and the pair is
    the key `_sources.yml` and `stg_sales__returns` both name.
  * `received_at` is NULL on 15,219 RMAs — the ones the simulation left unapproved. 961
    carry a receipt after `WORLD_TODAY`, none of them against an order day in RTN-266's
    span, so "open" has to mean NULL and a future-receipt reading comes out zero.
  * `refund_method` splits 82 / 11 / 7 over `original`, `gift_card` and `store_credit`.
    `disposition` and `restock_flag` agree exactly (restock ↔ true), so neither is a trap.
  * **14% of returns come back through a channel that did not sell them** — the built
    cross-channel population the armed register names. `int_return_linked` already takes
    the channel off the line, so the rule is embodied. RTN-284
    (`task-copperline-f7a3d0`) grades it.
  * **The tree disagrees with itself on soft-deleted orders.** `enrich_base` keeps them
    and cites `docs/reconciliation-policy.md` R-6; the dbt view `stg_sales__orders` drops
    them in the same `where` that drops the test rows. `NOT is_test` is unanimous
    everywhere; `deleted_at` is not. RTN-266 states R-6 in the ticket for that reason and
    leaves the disagreement standing. Any task grading an order population has to settle
    this in its own ticket text or it is grading a coin flip.
  * Graded nights 2026-03-20 to 2026-03-26; the order days they name run 2026-01-19 to
    2026-03-23, and the task grades all 64. The staff rows move 39 of them and the soft
    deletes move 20.
- CUS-402 (task-copperline-b6a1c4) is spec 05's A-3 rebuilt on the shipped world: it asks for
  `marts.dim_location`, which `04-transformations.md` declares and never builds, off
  `cus_address_normalize_daily`, whose own docstring says the dimension would come from it.
  Nothing about it is planted. **The DAG has never completed a run** and it stops three times
  before the end, each raise loud and each one a house rule the world writes down:
  * `addresses.collect` runs `CREATE SCHEMA IF NOT EXISTS ops` unqualified, and
    `warehouse.connect` puts the read-only `nwv` attach first on the search path, so the
    CREATE lands on a read-only database. Same shape as the restore path's raise that DR-1
    fixes.
  * `collect` writes a bare `NULL` for the parts neither book carries (`unit` on both legs,
    `postal_code` and `street_line` on the acquired leg). A bare `NULL` has no type, so the
    columns land as INTEGER in the `CREATE TABLE AS` and `normalize` dies on
    `trim(INTEGER)` — DK-1, the loud anaesthetic, biting a shipped DAG.
  * `key_addresses` calls `warehouse.delete_insert`, which replaces a partition and does not
    create a table, and `ops.address_keyed` is in no generator and has no DDL anywhere in the
    workspace, so the DELETE raises.
  Three facts of the built data the task rests on:
  * `raw.nwv_accounts` carries `billing_city`, `billing_state` and `country` and nothing
    finer, so the acquired book's accounts key coarsely and many of them fall on one place.
    That is what makes the place grain and the per-account grain different answers on the
    graded day, and the verifier refuses the day if they ever agree.
  * `addresses.key_problems` can never fire. `parts_present` is a function of the same six
    parts the key hashes, so two rows with one key always have one `parts_present`, and
    `address_key` is an md5 of six non-null strings and is never null or empty. The
    reject branch is furniture.
  * `collect` gathers both books whole rather than a day of them, so no day is special and
    the run's own date only stamps the partition. Graded day 2026-05-12, run twice.

- `marts.fill_rate_daily` (spec 05's A-4) cannot be built as the chapter states, and the row
  should be treated as spent. `sc_fill_rate_daily` ships built, produces `marts.lane_fill_daily`
  rather than `fill_rate_daily`, and already guards the zero denominator the chapter reserved
  for it — the `CASE WHEN ordered_units > 0` in `PUBLISH` and the `check_denominator` task
  are both in the shipped file, with the docstring saying why. What is left of the DAG is
  unrunnable in both directions: it reads `marts.int_po_receipt_matched`, which is in no model
  file, and writes `marts.lane_fill_daily`, which has no DDL anywhere in the workspace. So it
  is a third "cannot run" DAG, and a task that wants it needs the intermediate model written
  first. Found by the CUS-402 author.

- RTN-284 (task-copperline-f7a3d0) grades the channel attribution RTN-266 leaves alone,
  on the same feed, and nothing about it is planted either. What a sibling author should
  know:
  * **The two channel columns are not the same vocabulary.** `raw.orders.channel` sells
    through `store`, `web`, `marketplace` and `trade`; `raw.returns.return_channel` is the
    door and holds `mail`, `store` and `marketplace`. `extracts/promos.py` maps a
    same-door return `marketplace→marketplace`, `store→store` and everything else
    (`web` and `trade`) to `mail`, then draws the 14% cross-channel population uniformly
    over `mail` 40 / `store` 45 / `marketplace` 15. So `mail` is ambiguous by
    construction — the selling channel cannot be recovered from the door — and a
    door-attributed table carries a `mail` row and carries no `web` or `trade` row at
    all. That makes the wrong attribution loud in the channel column, which is what
    RTN-284 grades on.
  * A task that wants to grade "did the goods come back through a door that did not sell
    them" has to state the `mail` ↔ (`web`, `trade`) mapping in its own ticket text. It is
    in the generator and in nothing the agent can read. RTN-284 avoids the mapping: its
    `counter_credit_cents` column is `return_channel = 'store'` alone, which needs none.
  * Graded order days 2025-11-10 to 2025-11-21, given to the run rather than found from a
    night, so the reach question RTN-266 owns is not graded twice. The days discriminate
    the door attribution, the staff rows and the soft deletes — the verifier's first test
    asserts all three and passes on the built world.
- PAY-264 (`task-copperline-f3b8d6`) asks for the blank `currency` on
  `raw.pay_halcyon_settlements` to be filled for the whole feed, into
  `ops.halcyon_settlement_currency`, and nothing about it is planted: it is E4 met by the
  route home. Six facts of the built feed, all measured on a full-profile build:
  * **1,481,038 rows, 2024-02-06 to 2025-10-03, and `txn_id` is unique on every one** —
    the table carries no primary key and does not need one. 504,054 rows arrive with no
    currency (34%, `halcyon.py NULL_CURRENCY_PCT`), of which 47,150 are pounds or euros
    and 456,904 are dollars. So reading a blank as USD is right for 91% of the blanks and
    wrong for the rest.
  * **`merchant_acct` → `raw.merchant_regions` → `raw.market_config.billing_currency`
    reproduces the feed's own currency on every one of the 976,984 rows that carry one**,
    account by account. That is the route home and it is checkable from inside the feed,
    which is what makes the task fair.
  * **The Canadian account is the discriminator.** `HAL-CA-0204` holds 152,534 blank rows
    and the CA market bills USD. Three dbt models carry a hand-written map that reads CA
    as CAD — `int_gl_postings_unified` (in the Halcyon branch itself),
    `int_orders_enriched` and `dim_geography` — so the copy-paste answer misfiles three
    times the population that the blank-means-dollars answer does. See the flaw row below.
  * **The currency taken off the order agrees on every row.** `merchant_ref` joined to
    `raw.orders.order_ref` inside a seven-day window round `txn_datetime` matches all
    1,481,038 rows to exactly one order, and `coalesce(o.currency_code, 'USD')` — REV-7's
    rule, since `raw.orders.currency_code` is NULL before 2025-04-07 — gives the same
    currency the merchant account does, every time. PAY-264 passes that reading and says
    so in its checks.
  * **A float parse of `amount` lands the same cent on every row.** The values are small
    enough that `cast(amount AS DOUBLE) * 100` never falls on the wrong side of a rounding
    boundary. It breaks CONVENTIONS rule 5 and it is not convictable here. Reading the text
    as a number of dollars is: 1,466,102 rows carry a fractional part.
  * Halcyon amounts are never negative — `REVERSED` and `PENDING` rows carry the positive
    amount, and the status is the only thing that says which way the money went.

- FIN-455 (task-copperline-c4e1b8) grades REV-12 over the card book, and nothing about it is
  planted: it is the clause applied to what the tender module writes. What a sibling author
  should know before touching gift cards:
  * **The breakage is already in the ledger, and half of it must not recognize.**
    `raw.gift_card_ledger` carries about 16,200 `breakage` entries, one per card still
    holding value 24 months after issue, dated at the age-out instant. Joined through
    `raw.gift_cards.jurisdiction_code` to `raw.gift_card_jurisdictions`, a little over half of
    them sit in a jurisdiction where `escheat_applies` — so a revenue figure that recognizes
    the lot comes out near three times the honest one. 46 of the 66 jurisdictions allow
    breakage at 24 months; the other 20 escheat at 36 or 60, and `breakage_allowed` and
    `breakage_after_months` agree with `escheat_applies` on every row, so neither is a decoy.
    (Row counts here and below were measured on a full-profile build; the jurisdiction split
    is the shipped fixture and is exact.)
  * **REV-12 dates a breakage to the last day of the fiscal month it ages out**, which moves
    every one of them off its own day. FY2026 P02 is 2026-03-01..2026-04-04 and P03 is
    2026-04-05..2026-05-02, so in both the fiscal month end and the calendar month end are
    different days and a calendar reading is convictable. P01 ends 2026-02-28 and is the one
    FY2026 month where the two agree — do not grade a month end there.
  * **The card's own issue-date rate governs every amount on it.** `raw.fx_rates` is dense
    (every day, seven currencies, no USD row) so nothing is lost to a gap, but a rate read at
    the entry date instead moves both converted columns by thousands of cents. USD is about
    four fifths of the redemptions and takes 1,000,000, as `stg_reference__fx_rates` says.
  * **The time zone moves nothing.** Every ledger entry falls between 09:00 and 20:59 UTC, so
    no row sits on a different Portland date from its UTC one, on either of the two UTC
    columns the policy names. Real, recoverable, and worth nothing to grade.
  * `raw.gift_cards.status` calls about 11,800 cards `expired` while their `expires_at` is
    2029 or later, and those cards carry about a ninth of the breakage. FIN-455's ticket says
    the policy does not read `status` for that reason; a task that leaves it unsaid is
    grading a coin flip.
  * `to_base_cents` in `dbt/copperline_analytics/macros/money.sql` does not round the way its
    own comment says. DuckDB returns a DOUBLE from a decimal divide and `cast(x AS BIGINT)`
    then rounds an exact half to EVEN, so the macro and a plain `round()` disagree by up to a
    cent per row — on the base currency, where the rate is exactly 1,000,000, they disagree on
    every second row. FIN-455's verifier computes both and accepts either. Anyone grading a
    converted figure to the cent has to do the same or fix the macro.
  * Graded recognition days 2026-03-30, 2026-03-31, 2026-04-03, 2026-04-04, 2026-04-05,
    2026-04-30 and 2026-05-02. The ticket names no date at all.

- TRE-206 (task-copperline-b83ccf) is the written audit beside FIN-455: the escheat
  exposure, the jurisdiction table, and the OMS status column. Nothing about it is
  planted either. What it measured, over the whole card book:
  * **The whole escheat exposure is USD and books to CL-US.** The 20 escheating
    jurisdictions are 18 US states plus Quebec and Alberta, and both markets bill in USD,
    so no rate and no entity split moves any escheat figure. The FX and entity traps
    FIN-455 lives on are worth nothing on this side of the split.
  * **No card in the book is redeemed after its 24-month age-out.** The generator draws a
    redemption 7 to 406 days after issue, so the latest is about 13 months. The value at
    36 and at 60 months is therefore the value at 24, and `dormancy_months` cannot move a
    cent — only the date a balance becomes reportable. `escheat_applies` is the only
    column on that table that moves money.
  * **Not one card in the book has passed its own `expires_at`.** Every card expires 60
    months after issue and the book starts 2024-02-04, so the earliest expiry anywhere is
    2029-02-04. The 11,770 the OMS calls `expired` are all in that state, which makes the
    status column false rather than merely unhelpful.
  * **The stub case has population, and it is convictable.** 1,122 of the 9,949 escheating
    cards that have aged out were spent down first, so their breakage is the stub. Reading
    the exposure off `initial_cents` gives 118,242,500 rather than 106,935,742 — a gap of
    11,306,758 cents of money that was already redeemed.
  * `breakage_allowed` and `dormancy_months IS NOT NULL` agree with `escheat_applies` on
    all 66 rows, so all three tests give the same population. Only the clause says which
    is the test.
  * The ledger has written breakage for the first four months of issue cohorts only —
    18,286 entries over 2026-02-04..2026-06-14, against 195,674 cards. 106,935,742 cents of
    that escheats and 90,784,332 recognizes; the escheating cards that have NOT aged out
    still hold 644,089,266, and the escheating half of the book carries 751,025,008 in all.
    So a figure summed off `entry_type = 'breakage'` prices about a seventh of the escheat
    book.
  * **The figures in the FIN-455 entry above are from an older generator.** They were
    measured before a card spent down to a stub broke for the stub, so that build gave
    16,205 breakage entries and 172,097,500 cents where a build at this tree gives 18,286
    and 197,720,074. The jurisdiction split, the status counts and the 66 rows are
    unchanged. Re-measure any cent figure from this seam before reusing it.

- MER-311 (task-copperline-d1c7e4) grades the order-level discount coming down to the
  line, and nothing about it is planted either. What a sibling author should know about
  that seam:
  * **`subtotal_cents` is the sum of the order's lines, exactly, on all 3,413,378 live
    orders, and `order_discount_cents` is not inside it.** So any line-grain money summed
    from `line_total_cents` prices the order before its own discount: 12,848,977,962 cents
    of header discount against 449,382,777,688 of subtotal, 2.86% over the whole feed and
    2.85-2.86% in every one of the four channels. The gap carries no channel signal.
  * **The allocation rule is written in exactly one place**, the header of
    `models/shared/intermediate/int_order_lines_discounted.sql`: pro rata by tax-exclusive
    line total, integer cents, largest remainder, and no allocation against an order whose
    lines sum to zero. `docs/finance-policy.md` REV-13 is promotion STACKING at line grain
    and states no allocation, and there is no promotions policy document. A task that wants
    the rule found has that one file to be found.
  * **No order carries a header discount and a line discount at once** — 752,148 have one,
    1,016,948 have the other, none have both. So pro rata by `line_total_cents` and pro
    rata by `qty * unit_price_cents` give the identical answer on every day measured, and
    no check can tell the two bases apart.
  * **The largest-remainder tie-break decides real cents.** Two lines of one order can hold
    the same fraction. Swapping the model's `line_total DESC, line_no` for `line_no` alone
    moves 12 to 44 SKU rows a day, one cent each, and never moves a day total. Grade a
    per-SKU allocation within a cent or a day total exactly, never a per-SKU figure to the
    cent.
  * `raw.order_lines.unit_cost_cents` is never null over all 9,430,789 lines and `qty` is
    always a whole number from 1 to 30, so `int_order_lines_costed`'s NULL-cost branch and
    its half-up rounding are both unpopulated furniture.
  * MER-311 settles the order population in its own ticket text, naming
    `stg_sales__orders`, because of the soft-delete disagreement recorded above.
  * Graded days 2026-04-09, 2026-02-17 and 2026-05-19, none of them named in the ticket.

- PRO-1140 (task-copperline-c2b6f8) grades a promotion cost page built from
  `raw.promo_applications`, and nothing about it is planted. What a sibling author should
  know about the promotion surface, all of it measured on the shipped warehouse:
  * **REV-13 has no population and cannot be graded.** The clause states a four-step
    ladder — employee discount, trade-program discount, promotional markdown, coupon — and
    a per-line floor at zero. None of the four kinds exists in `raw`: `promo_type` holds
    `bogo`, `free_ship`, `threshold`, `percent_off` and `amount_off`, and there is no
    employee, trade-program or coupon discount column anywhere in the extract. Nothing
    stacks on a line either: every one of the 1,322,609 line-level applications is the only
    application on its line, `sim_sales.order_line_discounts` writes one row per line by
    construction, and no order carries a header row and a line row at once. The floor is
    unreachable as well — `line_total_cents = qty * unit_price_cents - line_discount_cents`
    on all 9,430,789 lines, no line comes out at or below zero, and the largest header
    discount is 25.1% of its order's lines. So "a stacked promotion that came out at zero",
    the question the policy document teases in its own "who reads this" section, is a
    question the data cannot ask. Treat REV-13 as furniture: cite it, never grade it.
  * **What the register says about a promotion does not reproduce what it took.**
    `promo_id` is drawn from a day-and-channel pool, so a `percent_off` promotion's
    `value_bps` against the line's gross matches the recorded `discount_cents` on 21,325 of
    779,829 rows and `amount_off`'s `value_cents` on 49 of 242,717. There is no rule that
    recomputes an application, and a task that asks for one is asking for a number the
    world does not hold.
  * **The feed restates money and never adds any.** A row with `order_line_id` NULL carries
    the whole of the order's `order_discount_cents`; a row with one carries that line's
    `line_discount_cents`, to the cent. So a day's applications add up to that day's
    order-level plus line-level discounts exactly, over the same order population. That is
    the tie PRO-1140 states, and it is also what makes `close_apply_promotions` a double
    count on top of its ordinal join.
  * **`int_promo_exposure` reports what a promotion was allowed to take.** Its `eligible`
    CTE inner-joins the register to the orders inside the window, in the market list and
    over `min_order_cents`, and hangs the applications off that join, so an application the
    register cannot account for is dropped with its money. Over the whole feed that is
    1,720,182,976 cents of 19,736,595,750 — 8.7% — and it reaches `dim_promotion` and
    `agg_promo_performance`, which are the world's only "what did this promotion cost"
    figures. No promotion goes all the way to zero on the page, so the symptom is a page
    that is short by a different amount on every row. PRO-1140 grades a fresh page against
    the feed and leaves the three models as they are.
  * **`funded_by` is armed in the generator and landed in no document.** A quarter of the
    applications are `vendor` (503,237 rows, 5,247,353,918 cents) and 4% are `marketplace`.
    `extracts/promos.py` says the board-pack contract was meant to exclude vendor-funded
    money from net revenue while the finance policy keeps it; neither sentence reached the
    shipped tree, so the column appears in three dbt models and in no policy. A task that
    wants to grade it has to put the disagreement into a document first. PRO-1140 says so
    in its checks and scopes the funding question out of its ticket.
  * `computed_by` is `marketplace` exactly when the order's channel is, so it carries no
    information the order does not. It is not a second axis.
  * Graded days 2026-05-12, 2026-03-17 and 2025-11-19. The ticket names no date at all.
- **Grading a mart-reading DAG from cold.** The `marts` schema is empty in every
  trial, so a check on a DAG that reads a mart has nothing to read. A verifier can
  stand one up without touching the shipped warehouse: build a scratch DuckDB file
  NAMED `copperline.duckdb` in a directory of its own, `ATTACH` the real warehouse
  read-only to fill it out of `raw.*`, then point `DUCKDB_PATH` at it.
  `warehouse.warehouse_path()` reads that variable on every open, `qualify()` takes
  the database name from the file stem, and with no `northwave.duckdb` beside it the
  `nwv` attach and the search-path reorder are both skipped. A fix that opens the
  warehouse through `include/lib` follows the variable; one that hard-codes the path
  does not, which is a conviction `CONVENTIONS.md` already supports. SC-1512
  (`task-copperline-f1c8b2`) does this to poke a sensor twice — once for a week it
  published and once for a week it did not — and the whole verifier runs in about
  twenty seconds. Measured on the shipped image.
- DQ-93 (task-copperline-c2b7f1) grades the dual-processor overlap — the second of DBT-NOTES's
  three real ties — and nothing about it is planted: it is `int_payment_matched` meeting
  `raw.pay_processor_windows`. Everything below was measured on a full-profile build, and the
  verifier recomputes all of it in session.
  * **The flag has no date in it.** `is_shadow_quarter` is `seen by both books`, and
    `settled_net_cents` adds the two books together, so about 181,300 orders carry the same
    payment twice and about 163,000 of them exceed their own total. (DBT-NOTES's 9,111 is the
    small build.) The counts wobble by a handful between runs; see the tie-break row below.
  * **The authority is per settlement row, not per order.** `raw.pay_processor_windows` gives
    Halcyon everything to 2025-08-31 and Meridian everything from 2025-09-01, so the boundary
    sits in the MIDDLE of the 2025-07-01..09-30 overlap and the tie's own header assumes the end
    of it. `docs/runbooks/processor-migration.md` and `docs/billing-integration.md` B-8 both say
    a ROW outside its processor's window is shadow traffic. Gating on the order's date instead
    moves the money on 1,415 orders (739 of them inside the population a per-order check can
    grade); gating a Meridian row on `event_time_utc` rather than `settlement_date` moves 7,036
    and leaves 10 orders settling above their own total, so that reading cannot go green either.
  * **After the windows are applied, no order in the world settles above its own total** — and
    that holds for every attempt the recycled reference could have matched, not only the one the
    model picks (the maximum over all candidates is exactly 0). That is what lets the rewritten
    tie be one-sided and green. An equality tie cannot be: about 21,100 dual-fed orders settle for
    LESS, mostly because the Halcyon reference-and-date match misses.
  * **45 orders draw an authoritative half from each book** — a capture split across the
    boundary, Halcyon's half in August and Meridian's in September — and the two halves add to
    the order exactly. So "no order draws on both books" is a wrong tie, and the runbook's last
    paragraph is about these.
  * **The two feeds agree to the cent on the payments they share** (PAY-207 says why), so moving
    the boundary a month changes the money on only 2,440 orders. That is the mutation DQ-93 uses
    to convict a model with the dates typed into it: a signal, not a landslide.
  * `int_orders_enriched`'s order-to-attempt pick is **nondeterministic**: 206,992 orders have
    more than one candidate intent in the seven-day window and about 59,900 of those have a tied
    top rank, so the same order can match either of two intents from one run to the next. Any
    check grading payment money per order has to leave the multi-candidate orders out. DQ-93's
    verifier grades the other 3,206,386.
  * **The authority is not a dbt source and has no staging view**, though `int_payment_matched`
    and `marts.cash_recon_daily` both name it as the record in their headers. Landing it is part
    of the ticket's work, which is why the staging layer is left open to that task.
  * 2,163 orders lose every cent of settled money once the windows are applied: their only
    claims are shadow rows. 1,946 orders look dual-fed from outside the overlap and all of them
    were placed on 2025-06-30, one day before it, because the first Meridian attempt raised in
    July. Nothing is dual-fed after the quarter.
  * Over the whole card book, the added-together reading totals about 127.67 billion cents and
    the windowed one about 118.63 billion — the double count is about 9.04 billion cents, near
    seven per cent of the card money in the estate (the last digits move with the tie-break above,
    so the figure is quoted to three places and no further). The same
    doubling is in the ledger: `int_gl_postings_unified` posts both card books whole, and
    `marts.cash_recon_daily` says in its own header that it reports the overlap and cannot
    resolve it. DQ-93 leaves both as they are and grades naming them.

## Flaw inventory (unplanned defects found in review, ACCEPTED as world furniture)

| Flaw | Where | Why it stays |
|---|---|---|
| The stock valuation multiplies by the number of open periods | `sc_inventory_snapshot_daily.VALUATION` joins `raw.dept_cost_complement` on `c.effective_from <= s.snapshot_date` and picks `max(c.cost_complement_bps)`. The join names no period, so every FY2026 period that has opened by the day matches and `sum(s.retail_value_cents)` fans out by that count. Measured: `marts.fct_inventory_valuation` totals 1.00x the one-complement answer on 2026-02-15, 3.00x on 2026-04-15 and 4.01x on 2026-05-12 | a complement joined without picking a period is the everyday shape of this mistake, and INV-2 ("one row per department per fiscal period") is the sentence it ignores. a7f3c9 (`task-copperline-a7f3c9`) grades that DAG on a different question and its oracle reproduces the same join, so the fan-out is what the world means by that mart today; changing it would move a7f3c9's expected result. SC-1204 (`task-copperline-f2a9c1`) convicts a copy of the join in `sc_shrink_weekly` — the May week comes out 4x — and says so in its checks. Found by the SC-1204 author |
| `docs/inventory-policy.md` names a column the feed does not carry | its "What each column means" table lists `dept_id`; `raw.inventory_snapshots` carries `dept_code`, and every model and DAG reads `dept_code` | harmless drift in a document written by finance-eng rather than by the team that lands the feed, and the row's own text ("populated, and load-bearing from here on") is true of `dept_code`. Nothing joins on the name in the document. Found by the SC-1204 author |
| Acquired-book status mapping defaults to active | include/lib-adjacent identity.build_dim and dbt dim_customer: `'closed'→closed, 'dormant'→suspended, ELSE 'active'` — but the acquired book spells its 20 inactive accounts `'inactive'`, so the dimension counts 5,060 | a real post-acquisition mapping bug of exactly this shape; NW-214's check 2 convicts the 5,060 path, and the ticket steers to the books. Found by the NW-214 author, not planted |
| The board pack DAG cannot run | fin_board_pack_weekly selects `account_status` from marts.dim_customer, whose column is `status` — a binder error on execution | explains in-world why the pack's number is hand-carried and stale; never in any task's replay list. Found by the NW-214 author, not planted |
| The board pack's headline is not the measure its header names | `fin_board_pack_weekly.company_revenue` selects `sum(recognized_cents)` from `marts.revenue_recognized_monthly`, and `write_pack` prints it under the header `reported_cents`. §SD-2 keeps the two names apart and puts `reported_cents` in `marts.account_rollup`, which is where the companion top-twenty page in the same deck reads it from — so the pack's headline and its account page have never been the same measure. The gap is the legacy-era book (53.9% of the net cents in `raw.invoices`, excluded by §BP-2) plus the credit memos, so the headline runs at about twice the rollup | a mislabelled column in the oldest contract's oldest report, on a deck nobody has been able to run end to end. FIN-457 (`task-copperline-a9d4f2`) is the ticket that fixes it; FIN-472 and NW-214 grade the same DAG and neither touches the measure. Found by the FIN-402 author, ruled by the FIN-457 author, not planted |
| Secret rotation check is red | plat_secret_rotation_check compares `rotates_on` in config/iac/connections.yaml against WORLD_TODAY: meridian_pay lapsed 2026-05-30, halcyon_pay 2025-12-01, so `record_and_flag` raises every run. CORRECTION from the CA-2 author: it never reaches that raise. `record_and_flag` writes `ops.secret_rotation` first, no generator or DDL file creates that table, and `warehouse.delete_insert` dies on the DELETE. The run is red either way and stays red however the tree is patched, since the warehouse is rebuilt from the image at scoring | rotation negligence on a deprecated processor and a two-week-late live one is real furniture; the red began after the week W2's ticket calls clean, so no fiction contradicts it. Found by the G1 author |
| Every commerce publish appends | `projects/commerce/sql/` — `settlement_publish`, `economics_publish`, `close_publish_economics`, `returns_publish`, `refunds_publish`, `cart_abandon_publish`, `recon_publish` and `payments_restore_day` are all a bare `INSERT INTO copperline.marts.*` with nothing to remove the partition first, and three carry a comment saying they replace it | a team that wrote its publishes before anyone replayed one, which is the world's own story. LF-1042 (`task-copperline-e7c30b`) grades `settlement_publish` and its RESPONSE grades naming the rest; the other seven stay as they are and are the honest answer to "how far does this reach". Found by the LF-1042 author |
| Two DAGs build dbt models nothing ships | fin_budget_variance_weekly selects `marts.plan_vs_actual_weekly`; gro_pricing_mart_daily selects three pricing models — none exist in any model file | a stalled project with its orchestration merged ahead of its models; both builds fail loudly on selection, neither is in any task's replay list. Found by the NLO-3 author |
| The nightly crosswalk apply cannot run | `identity.resolve_refs` filters `raw.orders` on `o.order_date`; the column is `local_order_date` (only `int_orders_enriched` renames it), so `cus_crosswalk_apply_daily` raises a binder error every night and `ops.customer_ref_resolved` is not in the warehouse at all | a job that has been red since the scheduler moved is furniture the E7 fiction already supports, and it is why D7 has a hole to fill. The SQL beside the bad column is correct on both re-key rules, so an agent that reuses the function has to run it before it works — which is the copy-paste conviction, not an unfair one. Never in any task's replay list. Found by the D7 author |
| The restore path cannot run | `fct_payments_restore`, four ways. `params` carry no default, so a plain trigger dies before the first step. `check_source_present` looks for a `dt=<settlement date>` directory, and the directories are arrival dates. `payments_restore_day.sql` writes event rows into `marts.fct_payments`, which is dbt's, is at order grain and is not on disk. `CREATE SCHEMA IF NOT EXISTS ops` is unqualified, and `warehouse.connect` puts the read-only `nwv` attach first on the search path, so it raises | the incident note says the path "was written after this and assumes the archive is complete", and a restore nobody has ever run is exactly the shape of that sentence. DR-1 (task-copperline-b7d3e1) is the ticket that runs it; its solution fixes all four. Found by the DR-1 author |
| The line ordinal is joined as if it were a key | `projects/commerce/sql/` — `close_line_economics` sums every refund in the whole feed grouped by `order_line_id` and joins the result back on `order_line_id`; `close_apply_promotions` does the same with the day's promotion applications; `economics_lines` joins the two int models on it; `returns_orphan_check` looks for orphans through it | the ordinal has 12 values over 9.4M lines, so the close subtracts every `L-01` refund in history from every `L-01` line. `booked_cents` is untouched, so `tie_to_orders` still ties and `check_economics_grain` still passes — a misjoin that leaves every check green, which is this world's own story. `returns_orphan_check` matches everything and has never found an orphan. The pair is stated as the key on the source (`_sources.yml`) and on `stg_sales__returns`, so the tree convicts the copy and saves the reader. RTN-266 (`task-copperline-c8f42d`) grades a fresh build over the same seam and leaves these four as they are. Found by the RTN-266 author |
| The nightly close cannot run | `close_price_book` reads `marts.dim_product`, which is dbt's and is not in the shipped warehouse; `close_revenue_tie` compares a booked total that includes `is_test` orders against one that excludes them; `close_publish_economics` is a bare INSERT | never in any task's replay list. It is the second producer of `marts.order_economics` — `order_economics_daily` is the first, and that one reads `marts.int_order_lines_discounted` and `marts.int_order_lines_costed`, which are not on disk either, so neither producer has ever run. Found by the RTN-266 author. CLS-208 (`task-copperline-c756fd`) now grades standing it up, and confirmed three more faults on the way: the grain check and revenue tie read `marts.order_economics` BEFORE `publish_economics` writes it; the revenue tie also holds `line_total_cents` totals against `grand_total_cents` (tax, shipping and the discount inside); `close_pricing_tie` returns its break count (3,230,670 of 3,436,578 orders) and stops nothing; `close_price_book` binds two placeholders and is given one; and `marts.dim_product` carries no `valid_from`/`valid_to` on any version, which also blocks `enrich_product.sql` and `snapshot_open_rows.sql`. MER-618 (`task-copperline-d6c419`) now grades the first of those two: `enrich_product.sql` rewritten off `raw.pim_product_versions`, plus a written ruling. `snapshot_open_rows.sql` is left to MER-612's history and is protected there — once that history exists the statement is right as written and returns 0 doubled rows and 1,718 open ones. Measured by the MER-618 author: the order book uses 23,147 distinct SKUs against the feed's 1,860, 1,724 in both, and 142 SKUs whose newest change is a `delete` carry no open row |
| `marts.fct_returns` has two producers and no DDL | `returns_daily`'s `returns_publish.sql` writes it at RMA grain keyed on `initiated_at`; the dbt model `models/commerce/fct_returns.sql` writes the same name | the same shape as `settlement_weekly`, and `returns_publish` is one of the eight bare-INSERT publishes already on this list. Found by the RTN-266 author |
| The Meridian intake deletes by settlement date and loads by file date | `payments_intake.load_meridian_events` reads `landing/meridian/dt={{ ds }}/hr=*` with `mode="replace", partition_col="settlement_date"`, and `partition_value` defaults to `{{ ds }}`. So an hourly run deletes every row already loaded with `settlement_date = ds` and inserts the rows that ARRIVED that day, whose settlement dates run from three days before it to more than forty after | the same arrival-versus-settlement confusion as the restore path, one DAG upstream, and the honest reason a hand-written intake gets written twice. `raw.pay_meridian_settlements` is generated, so nothing in the world has ever run it — but do not put `payments_intake` in a replay list without fixing it first, because a replay corrupts the table every other payments task reads. Found by the DR-1 author |
| The reconciliation ladder's working tables have no DDL | payments_recon_daily writes `staging.recon_found`, reads `staging.closed_periods` and appends to `marts.recon_exceptions`; no file in the tree and no generator creates any of the three, so the first finder dies on a missing table. The baked warehouse carries no `staging` SCHEMA either — `raw`, `ops` and nothing above them — so even the first step's `CREATE TABLE` needs a `CREATE SCHEMA` first | same shape as the two rows above — a job whose landed half the world does not ship. PAY-249 grades the first step directly and no task replays this DAG. Found by the PAY-249 author |
| The three exception finders bind their parameters backwards | `find_kind` passes `[ds, kind]` and each of recon_amount_mismatch.sql, recon_state_mismatch.sql and recon_unmatched.sql spells `? AS exception_kind … WHERE ds = ?`, so the kind lands in the date and DuckDB raises `invalid date field format: "unmatched"` | the DAG cannot reach these steps anyway (row above), so the second fault is invisible from the tree and contradicts nothing. PAY-249's ticket claims nothing about what the nightly run produces, for this reason. Found by the PAY-249 author |
| The POS manifest is written in a word nothing else knows | `raw.pos_batch_manifest.status` holds `ok`, `late` and `missing` — the generator writes no fourth value — but `projects/commerce/sql/pos_manifest_build.sql` and `pos_manifest_build_store.sql` stamp `loaded`, and `pos_loaded_stores.sql` and `pos_silent_stores.sql` both filter on `loaded`. On the landed table nothing matches: the silent-store escalation reads every store's last delivery as NULL and names all 268, and the 04:00 catch-up treats every store-day whose file is on disk as still owed | one team's private word for a column three other readers share, which is how a vocabulary drifts apart in a real estate. POS-402 (`task-copperline-2b8d47`) is the ticket that aligns it. Two things to know before touching these DAGs: `pos_manifest_build*.sql` write six of the manifest's nine columns through `INSERT OR REPLACE ... BY NAME`, so a replay of `store_pos_intake` or `store_pos_late_catchup` NULLs `claimed_row_count`, `stamped_at` and `received_at` for every store-day it touches — keep both out of every replay list; and the landed manifest holds no `loaded` row anywhere, so the fiction is that these statements say what they say and the landed table is what landed, not a record of them having run. Found by the POS-402 author |
| The silent-store escalation cannot run | `store_pos_late_catchup.escalate_silent_stores`, two ways before it ever reaches the query above. `CREATE SCHEMA IF NOT EXISTS ops` is unqualified and `warehouse.connect` puts the read-only `nwv` attach first on the search path, so the first statement raises — the same shape as the restore path. Past that, `pos_silent_stores.sql` holds three `?` and the call site binds two, so the prepared statement raises. `ops.pos_escalations` is in no world image | a last step that has been red since it merged, with the alert muted, is the honest reason nobody noticed the query behind it was useless. POS-402 (`task-copperline-2b8d47`) is the ticket that runs it; its solution fixes both. The unqualified `CREATE SCHEMA IF NOT EXISTS ops` is estate-wide — sixteen more DAGs and lib modules carry it, while finance and platform qualify theirs. Found by the POS-402 author |
| The Kestrel vendor share waits on a file nothing writes | `sc_partner_share_kestrel.wait_for_sell_through` is a `FileSensor` on `SELL_THROUGH_GLOB` — `include/data/marts/sell_through_*_<week>.csv` — and no DAG, model, blueprint step or script in the workspace writes a file of that name. `marts.sell_through_daily` is a dbt model and has never been exported as files. The sibling glob in the same module, `SHIPPING_COST_GLOB`, does have a producer (`sc_shipping_cost_daily.export_partitions`), which is what makes this one look like it has one | the lineage document's own why-this-exists story — a name-pattern coupling that stops delivering without failing anything — standing in the tree rather than in the past tense. The DAG docstring, `contracts/partner-share.md` PS-3 and both C-11 rows in the two documents all describe the export as if it existed. SC-1512 (`task-copperline-f1c8b2`) is the ticket that moves the coupling onto the mart; nothing is planted for it. Found by the SC-1512 author |
| No supplier in this estate has a name | `sc_partner_share_kestrel` filters `p.supplier_name = 'Kestrel Outdoor'` on `marts.dim_product`, which carries `supplier_id` and no name, so the share dies on a binder error the first time it is reached. Nor is the name anywhere else: there is no `raw.suppliers`, no supplier master feed lands, `models/shared/dims/dim_supplier.sql` says so in its own docstring and is built entirely from PIM product versions, and the generator names suppliers `Supplier 101` … `Supplier 360` with one exception (118, `Lakeshore Outdoor Co`) — "Kestrel Outdoor" appears in no table in the warehouse | a vendor known to the commercial team and never to the warehouse, which is the everyday shape of a partner integration written against a dimension somebody assumed. It cannot be repaired from inside the repository, so SC-1512 grades it as a written finding and forbids guessing a `supplier_id`; a task that wants the share to actually produce rows has to author the supplier mapping first. Found by the SC-1512 author |
| `pos_silent_stores` reads the whole manifest, not the window | the statement takes `max(business_date)` over every row for a store with no upper bound on the run's date, so a run for an old night answers with the batches that have arrived since. Invisible while the status filter matches nothing, because every store comes back silent anyway | a query written against "tonight" and never run for anything else. POS-402 grades it on a February night whose store starts sending again in March. The world holds exactly two stores whose feed stops inside the fact range, and both are lifecycle edge rows the spec already names: the CMP-3 remodel `S-0233` (last batch 2026-02-02, back on 2026-03-01) and the CMP-2 closure `S-0147` (last batch 2026-03-20, nothing since). No store's batches are `missing` for more than two days running, so those two are the whole of the honest escalation and POS-402's nights turn on them. Found by the POS-402 author |
| The market-to-currency map is written by hand in three dbt models, and it disagrees with the market record | `int_gl_postings_unified` (twice, in the Halcyon currency-recovery branch), `int_orders_enriched.market_currency` and `dim_geography.market_currency` all spell a market's currency from its country: `CA→CAD`, `BR→BRL`, `PL→PLN`, `ID→IDR`. `raw.market_config.billing_currency` says USD, USD, EUR and USD, and `docs/runbooks/market-setup.md` MKT-1 says the market record is the system of record and that a billing currency is a commercial decision rather than a geographical fact | a second copy of a commercial fact, gone stale, which is exactly what MKT-1 forbids and what the runbook's own open item ("nothing checks the seed against `raw.market_config`") is about. It bites hardest on Halcyon: 152,534 blank-currency settlements sit on the Canadian account, and `raw.fx_rates` carries a CAD rate, so the wrong answer converts and looks fine. PAY-264 (`task-copperline-f3b8d6`) grades a fresh table off `market_config` and convicts the copy; it does not repair the three models, which stay as they are. Found by the PAY-264 author |
| The guest-session stitch has never stitched | `projects/growth/lib/sessions.py`, `stitch_orders`, updates `marts.fct_web_sessions.customer_ref` from `raw.orders.customer_ref`. That column is the trade account and is null off that channel — measured: `select channel, count(customer_ref) from raw.orders group by 1` gives 187,731 on `trade` and 0 on `web`, `store` and `marketplace`. Every order a web session names is a web order, so the UPDATE's `o.customer_ref IS NOT NULL` is false on every candidate row and the statement has changed nothing since it was written. The id a consumer order carries is `loyalty_id`. The step also does not return the rows it changed, whatever its docstring says: it returns the day's converting sessions that carry ANY customer, which the sessions that arrived signed in dominate — on 2025-12-11 it reports 44, it changes 0, and the order book resolves 52 | a join written against the one id column its author knew about, on a step whose return value hid the result. GRO-447 (`task-copperline-c1a7f6`) is the ticket that finds it and rewires the stitch to `loyalty_id`; a session whose order carries neither id is a guest checkout and stays null, which is what `int_customer_lifecycle` calls `guest` and `enrich_customer.sql` leaves unresolved. IT COMPOSES WITH GRO-431 (`task-copperline-b41f7c`), which grades the dedup key in `build_sessions` in the same file: b41f7c's overlay and solution both leave `stitch_orders` as the world ships it, b41f7c's verifier passes against c1a7f6's solution, and c1a7f6's verifier passes with b41f7c's stream-position dedup in place — nothing it grades reads a count column. Second-order furniture the same ruling covers: `gro_event_replay_repair` rebuilds a day through `build_sessions` and never stitches it, so a repaired day loses whatever the stitch gave it until sessionize runs again; and the model side has the same gap, since `int_sessions_funnel` and `int_session_attributed` take `max(customer_ref)` off the events and never reach the order book. c1a7f6 puts both outside its ticket and shuts `dbt/**`. Populations on an ordinary day, for whoever writes here next: about 3,000 sessions, about 100 of them converting, about 55 of those arriving signed out, about 45 of those resolvable and the rest genuinely nameless. Found by the GRO-447 author |
| The rate intake cannot have landed a rate | `fin_fx_rates_intake.land` declares `columns: {rate_date, currency_code, base_currency, rate_ppm}` and `raw.fx_rates` carries `rate_date, currency_code, rate_to_usd_ppm, source, published_at`. `columns` types the CSV read, so the read is four columns against a five-column file and DuckDB refuses to sniff it; past that, `INSERT ... BY NAME` would leave three NOT NULL columns unfilled. The `coverage` step behind it groups by `base_currency`, which is a binder error the moment the load ahead of it works | a rendered job merged against a feed spec rather than against the table, on a feed whose rows somebody lands by hand every morning, is the honest reason a table is dense while its intake has never run — `ops.fx_rate_coverage` is in no world image, which is the corroboration. FIN-524 (`task-copperline-a2d915`) is the ticket that fixes both. Never in any task's replay list. Found by the GRO-455 author, ruled by the FIN-524 author |
| The world ships no `landing/fx` tree | the generator writes eleven landing sources — ads, ar, carriers, events, halcyon, meridian, oms, orders_export, pim, pos, wms — and the rate feed is not one of them, while `fin_fx_rates_intake` waits on `landing/fx/dt=<D>/rates.csv` with a three-hour `FileSensor` timeout | it costs nothing while nothing replays that DAG, and the 6,034 rows on `raw.fx_rates` are the world's assertion that the file arrives. It is why FIN-524 grades the job against the table rather than with `dag_runs`: `airflow dags test` cannot reach `land` on any date, fixed or not. Keep `fin_fx_rates_intake` out of every replay list. Found by the FIN-524 author |
| The rate intake's header comment describes another feed | `fx_rates_intake.dag.yaml` says rows before the currency cutover "carry a NULL currency and are USD by construction". `raw.fx_rates` has no NULL currency and no USD row at all; `stg_reference__fx_rates` mints USD at 1,000,000. The sentence is true of `raw.pay_halcyon_settlements` and E4 | a paste from one story into another, which is what a comment does when two feeds are worked in the same week. It points an agent at a base currency the table has never had, so it earns its keep as a wrong turn. FIN-524's solution repairs it and no check turns on it. Found by the FIN-524 author |
| No rollup target exists anywhere | `include/lib/blueprint/kinds/rollup.py` writes through `warehouse.delete_insert`, which deletes before it inserts and never creates a table. No DDL file and no generator makes `ops.fx_rate_coverage`, `ops.comp_price_coverage` or `marts.carrier_invoiced_daily`, so every rendered rollup in the estate would die on its first statement | the same shape as the restore path and the reconciliation ladder — a job whose landed half the world does not ship — and it is estate-wide, so repairing it belongs in the protected library rather than in any one ticket. FIN-524 (`task-copperline-a2d915`) grades the coverage step by running its own query read-only and tells its agent to leave the table alone. Found by the FIN-524 author |
| `marts.churn_scores_weekly` has three writers and three shapes | the dbt model `models/customer/churn_scores_weekly.sql` writes rule-based points out of a hundred with a `churn_risk` band, keyed on `week_of`, and the nightly build materialises it as a table; `projects/customer/lib/churn.py` delete-inserts a weighted score between nought and one, keyed on `ds`, with `source_book` and `history_days`; and `backfill.replay`, reached from `cus_northwave_backfill`, replays account attributes into the same name out of the frozen `nwv` copy with no score on the row at all | the same shape as `marts.fct_returns` and `marts.order_economics` two rows above, with the scale collision on top: the model's own tests state an `accepted_range` of 0..100 and the job's `score_problems` rejects anything outside 0..1, so the two producers convict each other. They have never been in the table together — the model is `materialized='table'` and rebuilds it whole, and the job's delete on `ds` cannot bind against the model's columns. Neither table is in the shipped warehouse. CUS-537 (`task-copperline-0f1a80`) is the ticket that moves the job onto `marts.customer_churn_weekly` and leaves the model its name; it shuts `dbt/**` for that reason. Found by the CUS-476 author, ruled by the CUS-537 author |
| `marts.tax_daily` publishes about 2.3 times the output tax the estate charged, three faults at once | `dbt/copperline_analytics/models/finance/tax_daily.sql` adds `int_orders_enriched`'s order tax to `fct_ar_invoices`'s line tax. (1) **The two are largely the same money**: all 172,674 goods invoices carry an `order_id`, resolve one to one to `raw.orders`, are all `trade` orders, sit on the order's own date, and their line tax equals `raw.orders.tax_cents` on every one — 18,400,194,743 cents on both sides. (2) **The join fans the invoice side out**: `order_tax` groups by day/market/CHANNEL/currency, `invoice_tax` by day/market/ENTITY/currency, and the join is day/market/currency alone; a market has one entity, so the invoice row meets one order row per channel trading and its tax is summed about four times (invoice side 45,886,667,047 against 11,580,190,981 that can join; the mart's own `invoice_count` is 412,587 against 103,783 invoices). The order side is not fanned out in return. (3) **The blank-currency invoices reach no row**: `raw.invoices.currency_code` is NULL on 92,971 invoices (US and CA, 2024-02-04 to 2025-04-06, the pre-E4 book) and `fct_ar_invoices` passes it through unfilled; `days` unions those keys in because a UNION treats two NULLs as one key, and the left join back never matches them because `NULL = NULL` is not true, so 6,846,249,989 cents is in the output nowhere. Measured over the shipped warehouse: the model would publish **81,329,623,679** cents against an honest **35,469,202,859** (35,442,956,632 of order tax over the `stg_sales__orders` spine plus 26,246,227 on the 24,080 invoices that bill no order). Every market is overstated — US +17,185,369,565 at 2.01x, GB 2.68x, DE 2.65x, IE 2.78x, CA 2.07x, and BR/MX/PL/ID about 3.15x, the fan-out with nothing taken back off it | three everyday mistakes stacked on one model — a mart adding two views of one book, a join on a subset of the grouping keys, and an equality against a nullable column — and every one of them leaves the DAG green: `cents_check` wants five rows with money in them and gets thousands. Nothing in the world documents `marts.tax_daily`, and `docs/finance-policy.md` says in as many words that the tax team keeps its own guidance outside this repository, so no document contradicts the mart. TAX-258 (`task-copperline-a8f27d`) is the written audit that measures all three and leaves the model as it stands; nothing is planted for it. Two things a sibling author should know before touching this seam: the mart is **not** in the shipped warehouse (the `marts` schema is empty), so it has to be emulated or built; and the order side of it is exactly `raw.orders` filtered — `int_orders_enriched`'s line rollup and attempt pick are both one row per order, so the nondeterministic attempt pick DQ-93 records cannot move a tax figure. Found by the TAX-258 author |
| The churn score's recency part has no lower bound | `churn.build_features` writes `least(1.0, coalesce(date_diff('day', last_order_date, DATE '<ds>'), 365) / 365.0)`. The bound is above only, so for any week that is not the current one an account that has ordered since counts back a negative number of days and recency's weight is subtracted from the total instead of adding part of it. Measured against a 360 built from `raw.*`: 2,295 of 3,960 accounts score below nought on 2026-04-10, 3,360 on 2026-02-20 and 3,897 on 2025-11-14, and `check_scores` rejects every one | the DAG runs Friday-forward with `catchup=False`, so the fault is invisible until somebody replays a week — which is the honest reason a job can be green for months and unable to rebuild its own history. The module's own docstring states the contract it breaks ("each one comes back scaled to zero-to-one"), so the tree convicts it and saves the reader. CUS-537 (`task-copperline-0f1a80`) is the ticket that bounds it; its verifier grades three past Fridays. Found by the CUS-537 author |
| The account page files a credit memo under its invoice's period | `stg_finance__credit_memos` selects `applies_to_period as fiscal_month`, and `raw.credit_memos.applies_to_period` is the fiscal period of the INVOICE date (`extracts/ar.py` stamps it from `_util.fiscal_month` on `invoice_date`). §REV-5 books a memo in the period its `issued_on` falls in. `account_rollup.sql` groups its credits CTE by the renamed column, so `marts.account_rollup.credited_cents` — and the `reported_cents` it subtracts from — have always carried the invoice's period. `int_gl_postings_unified.sql` reads the same model and posts on `issued_date` at the invoice's `fx_rate_ppm`, which is §REV-5 both ways, so the tree holds one right reader and one wrong one. Measured on the full profile: 4,866 of 11,528 memos are filed to a period other than their issue period, and the two readings total the same 5,331,080,655 cents over the book. `account_rollup` also converts nothing — `sum(m.amount_cents)` across four currencies — which is a second and separate defect on the same CTE | a rename that quietly changes a column's meaning, on the deck's oldest page. FIN-419 (`task-copperline-a6f21b`) is the written audit that sizes it, over FY2025 P1..P6; it leaves the model as it is and rules the missing conversion out of scope. No other ticket grades those six periods — FIN-402 (`task-copperline-e6b3af`) grades FY2026-P02, FY2025-P10 and FY2025-P11, FIN-311 (`task-copperline-c03aad`) grades FY2026-P04. §REV-8 redirects nothing on this book: `loaded_at` is the issue date plus 0 to 5 days and a period closes on the 5th business day of the next one, so the oracle reports `redirected == 0` over all 11,528 rows. Found by the FIN-419 author |
| `int_customer_lifecycle` has no upper bound on the order date | the `orders` CTE reads `int_orders_enriched` whole and every date column downstream of it measures against `var('ds')`, so a build for a past day aggregates the feed to its end and then dates it against a day most of it is after. Measured on the shipped warehouse, for `ds` 2025-10-17: the model publishes 295,148 parties (the same count it publishes on every day, because the count was never a function of the day) against 271,460 that had ordered by then; 281,883 rows carry a negative `days_since_last_order` and 23,688 a negative `tenure_days`; `sum(orders_lifetime)` is 3,413,378 against 2,325,989 orders that had happened; `is_churned` is true on 1,758 parties against 34,906. For `ds` 2026-01-16 the same figures are 12,611 phantom parties, 237,027 negative recencies, 580,807 orders that had not happened, and 4,928 churned against 22,911. The replay's churn series therefore rises across the autumn while the true one falls by a third | the nightly cannot reach it: `ds` is the current day, no order is dated after it, and a build for the world's own `ds` is identical either side of the fix, to the row. Every DAG is `catchup=False`, so the only route to a past day is `plat_backfill_broker` — which is the honest reason a model can be wrong for a year and green every night. CUS-544 (`task-copperline-b028eb`) is the ticket that bounds the CTE and grades the audit; its verifier builds the model for a day taken off the feed and ties the party set, every party's orders and both rolling windows to `raw.orders`. IT COMPOSES WITH CUS-476 (`task-copperline-f5c2e8`) on the same model: the two edits are disjoint — CUS-476 empties the seven life columns on a guest row in the final select, CUS-544 bounds the `orders` CTE — and both verifiers pass with both fixes in place. CUS-544's verifier reads only `orders_lifetime` and the date columns, and guards every date read against NULL, so CUS-476's emptied buckets do not disturb it; against CUS-544's solution alone, CUS-476's verifier fails on its own guest test and passes its other six. The same shape one layer along is CUS-537's, `churn.build_features` two rows above. Found by the CUS-476 author, measured and graded by the CUS-544 author |
