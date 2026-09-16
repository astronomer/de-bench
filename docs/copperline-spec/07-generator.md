# 07 — The fixture generator

The generator builds every row of Copperline's history. It runs on the harness side at trial setup, writes into the trial's DuckDB file before the agent starts, and never ships inside the world.

---

## 1. The property: consistent as of any date

One sentence states the whole design goal. **Every table derives from one timeline config, so the world is coherent as of any date in its history.** Take any date `D` in the fixture range: the tables restricted to `event_time <= D` describe a company that makes sense on `D`. The stores that were open are open, the markets that had launched are live, the ids are in the format that era used, the processor that was authoritative is the one settling, and the ledger closes the months that were closed. Nothing in the data knows about a later date.

This property is what lets the era, backfill and repricing tasks share one world instead of needing one world each. A repricing task replays FY2026 Q1 and must find the second-wave markets absent and the retail inventory method in force. A backfill task rebuilds 2024-10 and must find old-format ids, 23:05 stamps, no `event_time_utc`, no Northwave book and no currency column. Both read the same tables.

Three rules make the property true rather than intended.

1. **One clock.** No generator code reads the wall clock. Every "now" comes from `today` in the config. A test greps the generator package for `datetime.now`, `date.today` and `time.time`, and fails on a hit.
2. **Per-day independent streams.** Each day's rows for each table are drawn from a stream seeded by `hash(seed, table_name, ds)`. Adding a day, widening the range or regenerating one day never moves another day's rows. That makes a generation bug diagnosable and a range change cheap.
3. **Facts derive from entities, never from marts.** The generator writes `raw.*` and `ops.*` and nothing else. It never reads a staging or mart table, and it never derives one fixture from another fixture's aggregate. `staging` and `marts` are what the pipeline and the agent produce, which is the only way differential grading against the pristine tree stays honest.

The full checklist the generator design must satisfy before the first row of history generates is `day-zero-invariants.md`. This chapter states how the generator meets it, not what it says.

---

## 2. The generator's data model is the upstream model

The generator does not draw `raw.*` rows directly. Its internal data model **is** the upstream OLTP model in `02-upstream-model.md`: the application databases Copperline runs on — the order management system, the ERP, the warehouse management system, the e-commerce platform — plus Northwave's own smaller upstream model, and the second systems that live outside the company, namely the two processor settlement feeds and the marketplace settlement, which share transaction identity with the internal payments tables. The generator simulates those systems first, day by day, and then derives the `raw.*` extracts from them. Simulating the application databases is also what makes the consistent-at-any-date property natural to implement rather than something to police: a simulated business on day `D` cannot know about day `D+1`.

**The delivery mess is applied at the extract boundary, not upstream.** The upstream fiction may keep its own conventions — decimal money, a single clock, its own `today` — because it is fiction the generator reads, not data the agent sees. Everything the world is graded on is imposed as the extract is written: `loaded_at` and the late tail, integer cents and `fx_rate_ppm`, the era-conditional id formats, and each feed's own format and vocabulary. A feed is a file somebody else's system wrote, so it looks like one.

Two consequences follow. An upstream schema earns its place by making the company real, and an extract earns its place by serving a task: the human-resources, routing and payroll schemas stay upstream, and no feed extracts them until a task claims them. And the second systems are generated separately from the internal ones, because that separation is what puts a real seam between two sources of record — the seam every reconciliation task grades.

---

## 3. The config

The generator's whole input is two files, and it writes a third. All three sit beside `workspace/`, never inside it.

```
worlds/copperline/timeline.yaml     eras, calendar, rates, volumes, seeds
worlds/copperline/planted.yaml        the armed graded rows — the clause-to-row map
worlds/copperline/PLANTED.md        the answer key; its generated half is rendered from the two above
```

`timeline.yaml` in outline:

```yaml
world: copperline
seed: 20260615                    # one integer; every stream derives from it
today: 2026-06-15
range: {start: 2024-02-04, end: 2026-06-14}
profile: shipped                  # shipped | authoring (see §7)

fiscal:
  pattern: 4-5-4
  year_end_rule: saturday-nearest-jan-31
  emit_years: [FY2022, FY2023, FY2024, FY2025, FY2026]

eras:
  - id: E1_pos_local_stamp
    from: 2013-08-05
    to:   2025-11-02
    effects:
      - {table: raw.pos_sales_header, column: event_time_local, rule: const_time, value: "23:05:00", tz: store_local}
      - {table: raw.pos_sales_header, column: event_time_utc,   rule: null}
  - id: E2_customer_rekey
    at: 2024-11-04
    tail_until: 2024-11-17        # the 45 late old-format rows land here
    ...

rates:
  fx:      {source: raw.fx_rates, grain: daily, unit: ppm, drift_bps_per_day: 3, seed_offset: 11}
  carrier: {source: raw.carrier_rate_cards, versions: [...]}

volumes:
  orders_per_day_base: 3000
  weekday_shape: [0.86, 0.95, 0.98, 1.02, 1.24, 1.48, 1.11]   # Sun..Sat
  annual_growth_pct: 9
  spikes:
    - {date: 2024-11-29, factor: 30, name: black_friday, channels: [store, web, marketplace]}
    - {date: 2024-12-02, factor: 18, name: cyber_monday, channels: [web, marketplace]}
    - {date: 2025-11-28, factor: 30, name: black_friday, channels: [store, web, marketplace]}
    - {date: 2025-12-01, factor: 18, name: cyber_monday, channels: [web, marketplace]}

late_tail:                        # see §5
  default:    {d0: 0.924, d1: 0.041, d2: 0.018, d3: 0.009, d4: 0.005, d5: 0.003}
  marketplace: {d0: 0.71, d1: 0.14, d2: 0.07, d3: 0.04, d4: 0.03, d5: 0.01}
  store_card: {d0: 1.0}      # the settlement leg is never late; the POS batch FILE
                             # can arrive up to 3 days late, a file-level lag
```

The era ids and dates in `timeline.yaml` are the eras in `01-timeline.md` §3, and the calendar block emits the fiscal and market calendars described there. `planted.yaml` holds one entry per graded edge row:

```yaml
- id: REV-6-refund-day14
  clause: docs/finance-policy.md#REV-6
  tasks: [FIN-311]
  table: raw.returns
  ds: 2026-02-14
  action: insert
  changes_row_count: true
  key_block: reserved            # takes an id from the reserved block, see §4
  values: {refund_kind: full, days_since_invoice: 14, amount_cents: 41900}
```

---

## 4. Arming the graded rows

The generator runs three passes in a fixed order: **base draw**, then **edge pass**, then **invariant pass**. The edge pass applies `planted.yaml` top to bottom, and it has two mechanisms and only two.

**Reserved key blocks.** An edge that inserts a row takes its id from a block the base draw never touches — `order_ref` values in `OE-9######`, `payment_id` in `PM-9######`, and so on. Inserted edges are therefore identical at every volume and can never collide with a base row.

**Rank selection.** An edge that must land on an existing row, to keep the day's row count unchanged, names its target by rank: the row with `row_number() over (partition by ds order by order_ref) = 7` on 2026-03-08. The rank is deterministic under the seed and unaffected by any other edge on any other day.

The invariant pass runs last and refuses to write anything on a failure. It runs the data-property and answer-key tests from `day-zero-invariants.md` — the half-cent tie sweep, the NULL-population floor on graded dates, the two clocks, the money units — plus four checks that belong to the edge pass itself:

- no two edges touch the same primary key;
- no edge changes a day's row count unless it declares `changes_row_count: true`, so arming stays invisible to a row-count profile;
- every rank an edge uses is at most the minimum row count for that table on any armed date, so a volume change cannot silently unarm an edge;
- every edge names at least one clause and at least one task, and every load-bearing clause in every shipped policy document names at least one edge.

---

## 5. Money and time

**Integer cents, everywhere.** Every money column in the extracts is `BIGINT` and named `*_cents`. The generator never emits a fraction and never uses a float. Currency conversion happens at generation time with `decimal.Decimal` and `ROUND_HALF_UP`, and the rate used is written onto the row as `fx_rate_ppm`, an integer in parts per million. `raw.fx_rates` is daily grain in the same unit. An expected result can then be re-derived from the shipped fixtures alone, without re-running the generator and without anyone having to agree which day's rate applied. Basis-point columns such as `cost_complement_bps` follow the same rule for the same reason.

**Two timestamps on every fact row.** `event_time` is UTC and says when the thing happened. `loaded_at` is UTC and says when the warehouse saw it. They differ, and the difference has a real tail:

| lag | default feeds | marketplace | in-store card settlements |
|---|---|---|---|
| same day | 92.4% | 71% | 100% |
| +1 day | 4.1% | 14% | — |
| +2 days | 1.8% | 7% | — |
| +3 days | 0.9% | 4% | — |
| +4 days | 0.5% | 3% | — |
| +5 days | 0.3% | 1% | — |

Nothing arrives later than day 5, and something always arrives on day 5. The true safe lookback is therefore a measured fact in the data — five days — and the habitual three-day guess drops about 0.8% of default-feed rows and about 4% of marketplace rows. That 4% is small enough to pass a total and large enough to be the whole graded set for a per-producer check. The per-channel split matters as much as the tail: a uniform five-day lookback is wasteful on the in-store card settlements, which are never late, so "use five everywhere" is not free either. The store's POS batch file is the separate case — it can arrive up to three days late as a whole file, which is a lag on the delivery and not on the rows.

**Two further time rules.** Event timestamps carry zero seconds, so a minute-truncation in any downstream oracle is a no-op. And `loaded_at >= event_time` holds on every row, including across the UTC cutover, so no row is ever loaded before it happened.

---

## 6. The clock

Shipped world content reads the current date: a warehouse-cleanup runbook filters on `current_date - 7`, a decoy model filters on `current_date - 14`, and the board pack counts a trailing window. So the trial pins the clock in three places, and they agree.

- The harness exports **`WORLD_TODAY=2026-06-15`** into the trial environment.
- `include/lib/calendar.py` exposes `today()`, which reads that variable. World code that needs today's date calls it.
- The setup step pins the warehouse's `current_date` to 2026-06-15, so every shipped SQL that uses it resolves against that date.

DuckDB 1.5 has no setting that moves the clock — the probe for one found `TimeZone` and nothing else. What works is a macro stored in the database file: `CREATE OR REPLACE MACRO current_date() AS DATE '2026-06-15'` shadows the built-in of the same name, and a later independent session against that file resolves to the macro. The harness writes eight of them (`current_date`, `today`, `now`, `current_timestamp`, `get_current_timestamp`, `transaction_timestamp`, `current_localtimestamp`, `current_localtime`) in `_prepare_workspace_extras`, gated on `WORLD_TODAY`. One limit to design around: the pin lives in that database, so a query run while another database is the current catalog — an `ATTACH` of the world file from an in-memory session, say — falls back to the real clock.

The generator itself reads `today` from `timeline.yaml` and nothing else. The acceptance test is that a trial graded twice, a day apart, produces identical verdicts.

---

## 7. The two profiles and the volumes

`profile: shipped` is the trial default. `profile: authoring` scales volumes down by roughly 20x, keeps every entity table at full size, and keeps every armed edge — so it crosses every boundary the shipped profile does, on the same dates, at a size a person can check by hand. It exists so that an independent derivation can be written against a slice small enough to reason about.

Volumes at the shipped profile:

| table | grain | rows |
|---|---|---|
| `raw.stores` | store | 268 |
| `raw.customers` | trade account | 4,000 |
| `raw.nwv_accounts` | acquired trade account | 1,400 |
| `raw.customer_id_map` | legacy id to new id | 2,600 (60 legacy ids unmapped) |
| `ops.merge_candidates` | flagged pair | 300 |
| `raw.market_config` | market | 9 |
| `raw.entities` | legal entity | 5 |
| `raw.fiscal_calendar` | calendar date | 1,827 |
| `raw.market_calendar` | market × date | 16,443 |
| `raw.orders` | order | ~2.6M over 862 days |
| `raw.order_lines` | order line | ~7.1M |
| `raw.pos_sales_header` | POS transaction | ~1.9M |
| `raw.pos_sales_daily` | store × day, the FY2023 slab | 70,861 |
| `raw.pay_halcyon_settlements` | settlement | ~0.9M |
| `raw.pay_meridian_settlements` | settlement | ~1.6M |
| `raw.inventory_snapshots` | SKU × store × week | ~2.2M |
| `raw.web_events` | clickstream event | ~2.7M at the shipped `--scale 0.25`; the scale knob is the wall-clock lever |
| `marts.fct_shipping_costs` source rows | package | ~9,200 per day, 828,000 over the ninety-day repricing window, 2026-02-01 to 2026-05-01 |
| `raw.finance_ledger` | entity × closed fiscal month | 74 — hand-built, see §8 |

Two notes on those figures. The customer numbers are small because Copperline's dedup problem is a **trade-account** problem, which is where it is real: consumer orders carry an anonymous `loyalty_id` and never enter `raw.customers`. And row generation must run as DuckDB SQL over `generate_series` with hash-derived draws, not as a Python row loop. At 2.6M rows that is about two seconds one way and several minutes the other, and trial setup pays it on every trial.

---

## 8. The finance ledger stays independent

`raw.finance_ledger` is the oracle the recognized-revenue flagship ties against. It is worth nothing if it shares a process with the thing it checks.

**The ledger is hand-built.** It is 74 rows at entity × closed fiscal month grain — the coverage and the per-entity counts are in `01-timeline.md` §4.6 — typed by a person from `docs/finance-policy.md` and the raw fixtures, and committed as a CSV beside the other hand-written world content. The generator does not write it, cannot read it, and contains no reference to it. A test asserts both halves: the file is byte-identical across generator runs, and `grep -r finance_ledger tools/gen_copperline/` returns nothing. If the ledger tie needs account grain, that is a second hand-built file under the same rules, not a generated one.

**Three books, three authors.**

- **Book one** is the `expect_sql` over the shipped fixtures, written by the task author.
- **Book two** is the solution, written from the ticket.
- **Book three** is an exact-`Decimal` Python recognition model, written from `docs/finance-policy.md` alone by an implementer who has not seen book one, and run against the `authoring` profile — small enough to check by hand, and crossing every boundary.

The ledger is reconciled against book three, cent for cent, before the world ships. Any disagreement blocks the world. The resolution is a change to the fixtures or a change to the policy text, **never an edit to the ledger to make it agree.**

---

## 9. PLANTED.md has two halves

`PLANTED.md` sits beside `workspace/` and never ships. It has a generated half and a hand-written half, and only the generated half is tested for drift.

**The generated half** is rendered from `timeline.yaml` and `planted.yaml` at the end of every run: the clause-to-row map, the armed-column register, and the drift inventory. It cannot disagree with the fixtures, because it is the same input rendered a second way. Rendering it also means an edge with no clause and no task shows up as an empty column in the answer key, which is the cheapest way to find an ungraded decision before it becomes a grading dispute. A test regenerates this half and fails on any difference.

**The hand-written half** is the part no config holds: the per-trace-path defect pairs, the trap-conservation measurements, the flaw inventory, and the off-lineage register. The generator never touches it. It starts empty and present, and it grows as tasks are authored.

---

## 10. The generator stays out of the shipped world

The generator is an answer key. Anything it ships leaks every era boundary at once, and every arming date with them.

```
tools/gen_copperline/            the generator package — repo only, never shipped
worlds/copperline/timeline.yaml  input, beside workspace/
worlds/copperline/planted.yaml     input, beside workspace/
worlds/copperline/PLANTED.md     answer key, beside workspace/
worlds/copperline/workspace/     the tree the agent gets — source only
```

Four rules, each with a test.

1. Nothing under `workspace/` imports `gen_copperline`, and no file under `workspace/` contains the strings `timeline.yaml`, `planted.yaml` or `gen_copperline`.
2. `workspace/` holds source only: no generated CSV, no parquet, no `__pycache__`, no DuckDB file.
3. The generator writes into the trial's DuckDB file at setup, from the harness side, before the agent starts. The agent sees tables, never a generation script.
4. The three files beside `workspace/` are excluded from the shipped tar by the same mechanism that excludes `PLANTED.md`.

**One harness consequence, and it costs money to get wrong.** `world_sha` hashes `workspace/`, and the trial cache keys on it. With generated fixtures, changing `timeline.yaml` changes every expected result while leaving `world_sha` untouched, so stored trials would be reused against fixtures they never ran on. The cache key extends to a second hash, **`fixture_sha`**, over `timeline.yaml`, `planted.yaml` and the generator package source. Changing one line of `planted.yaml` must invalidate the affected trials and nothing else. This lands before the first generated-fixture trial runs.
