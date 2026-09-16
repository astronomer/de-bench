# Copperline — the world specification

This directory is the complete, self-consistent specification for the Copperline world: the data platform of a fictional omnichannel retailer, designed so that every task family in `../../hard-task-ideas.md` has a home. Read the chapters in order the first time; after that, each chapter owns its facts and the others reference it rather than repeat it.

| Chapter | Owns |
|---|---|
| `00-overview.md` | the company, the name registry, the task-to-world map, the build order, open questions |
| `01-timeline.md` | the eight era events, the calendars, the dated incidents, the graded and never-grade windows — every date in the world lives here |
| `02-upstream-model.md` | the simulated truth: the 79-table application model, the Northwave upstream, the external systems, the hazard adjudication |
| `03-extracts.md` | the landing layer: every `raw.*` source as an extract with the delivery mess applied at the boundary; the armed-column register |
| `04-transformations.md` | the dbt architecture: layers, conformed dims, shared `int` models, dependency rules as world policy, test placement |
| `05-platform.md` | the six team projects, the DAG estate, the house library, the placed structural items, the version pins |
| `06-consumers-and-docs.md` | the consumer registry, the documents layer with its clauses, the ops surface, authority placement |
| `07-generator.md` | the fixture generator: how the upstream model becomes fixtures, edge arming, PLANTED.md, the cache key |
| `../day-zero-invariants.md` | the nineteen invariants that must hold before the first row of history generates |

**Terms settled after this spec was written.** The harness landed with these
names; where the chapters say the word on the left, build the thing on the
right.

| Spec says | Build says |
|---|---|
| `edges.yaml` | `planted.yaml` (already swept in this text) |
| edge, armed edge, edge row | planted row — planted means live; there is no separate "armed" state |
| edge pass, arming | planting |
| safe zone | reserved window |
| `fixtures:` block, fixture generation | `generated:` block, generated data (`fixture_sha` → `generated_sha`) |
| profile `shipped` / `authoring` | profile `full` / `small` |
| differential grading | the `unchanged` check kind |
| pristine tree / pristine arm | original tree / original arm |
| `seed_runs` | `history:` in task.yaml (the cache-key field keeps the old name) |
| `seed:` in checks.yaml | `replay:` |

**What binds, when sources disagree.** For the Copperline build, this spec governs design. `docs/authoring.md` governs the harness contracts — the `world.yaml` format, the check kinds, the workspace layout, the prove flow — and nothing else. The existing worlds (`worlds/lodestone`, `worlds/upgrade` and their variants) were built under practices this spec deliberately replaces — shipped CSV fixtures, single clocks, author double-entry oracles, worked figures in prompts — and are format reference at most, never design precedent. Where a contract the spec assumes is demonstrated only in an existing world, copy the format and not the practice; where this spec and any repo document genuinely conflict on design, this spec wins and the conflict is worth reporting.

One design rule governs the whole build, and it answers where complexity belongs: **the substrate is day-zero, the traps are task-time.** Everything in `day-zero-invariants.md` — the two clocks, integer cents, the eras, the second systems, the calendars, the generator hygiene, the documents skeleton — must exist before the first row generates, because none of it can be added to generated history later. Planted defects, seeded incidents, and armed edges are added per task, in the reserved safe zone, gated by the trap conservation test and the probe-first rule. The world ships correct and coherent; the tasks make it hard.

## The company

Copperline is a home, hardware and outdoor-living retailer. Founded in Portland in 2009 as a builders' yard, it still moves about a third of revenue through ~4,000 named trade accounts that buy on terms — which is why a trade invoicing and receivables domain exists beside the order flow. The rest is 268 stores across five countries, the web shop (since 2018), and Copperline Marketplace — Copperline's own marketplace, running since 2022, where third-party sellers list and Copperline takes a commission. FY2025 revenue $1.94B; channel mix stores 58%, web 31%, marketplace 11%. Copperline bought Northwave Supply — a trade-focused regional competitor, 44 stores, 1,400 trade accounts — in early 2025, and the integration was never finished. The data team is eleven people; before 2021, reporting was a Pentaho estate one contractor built, and 13 of its 22 nightly jobs still run.

Three facts about the fiction do work later: the trade book is small and named, so the dedup problem is a real 4,000-row problem; three channels settle on three clocks, so late data is normal rather than planted; and the company grew by acquisition and by opening abroad, so the data carries seams nobody had time to clean up.

## The name registry

Every named system, one place. Chapters use these names without introduction.

| Thing | Name |
|---|---|
| Order management (system of record) | Copperline OMS |
| ERP / general ledger | Ironwood |
| Store POS | Northgate |
| Warehouse management | Corvid |
| Product information | Palette |
| CRM and support | Halyard |
| Clickstream collector | Driftwood |
| Payment processor, legacy ("processor-A") | Halcyon Payments |
| Payment processor, current ("processor-B") | Meridian Pay |
| Marketplace | Copperline Marketplace |
| Acquired company | Northwave Supply |
| Ad platforms | Beacon Ads, Tessera Social, Solstice Search |
| Email platform | Larkspur |
| Partner-share vendor | Kestrel Outdoor |
| Propensity model | Compass |
| Carriers | Brightline Freight, Pallas Parcel, Merriweather Logistics |
| Legal entities | CL-US, CL-GB, CL-IE, CL-DE, CL-MX |
| Markets, live from the start | US, CA, GB, IE, DE |
| Markets, second wave (FY2026 Q1, in no mart yet) | BR, MX, PL, ID |

## Task-to-world map

Where each tier-1 task from the catalog lands. Tier-2 mappings appear in the chapters' host columns.

| Task | Lands on |
|---|---|
| CT-2 clear the bad week | `fin_revenue_daily`, the March bad week (01), Airflow 3.3 bundle versioning (05), `plat_backfill_broker` |
| NW-214 two customer counts | `cus_dim_customer_daily`, the acquisition fixtures and `ops.merge_candidates` (03), the board pack (06) |
| FIN-311 recognized revenue | the trade invoicing extracts (03), the finance policy (06), the hand-built ledger (07), `fin_close_monthly`; graded month May 2026 |
| FIN-388 reprice ninety days | `sc_shipping_cost_daily` → `marts.fct_shipping_costs` (05), rate-card versions + `docs/rate-policy.md` (06), both 2026 DST days (01) |
| CT-1 the quiet 1.10 | the dbt pins and their silent behavior boundaries (05), `tools/check_upgrade.py` as the vacuous green (06), the `northwave_reporting` blast radius (05) |
| NLO-3 the seed | `gro_market_seed_build` (05), `raw.market_config` with the CA breadcrumb (03) |
| DQ-1 green without blinding | `plat_dbt_test_nightly` (05), the twelve failing tests framed by the layer-test placement (04), the quality contract revision memo (06) |
| G1 exclusive owner | `plat_config_sync` and the `config/iac/` surface (05, 06) |
| W2 holiday skip | `channel_daily_intake`'s `none_failed` join (05), the all-market holiday (01), `raw.market_calendar` (03) |
| M1 / M7 PDI | `legacy/pdi/` and `include/lib/legacy_runner.py` (05), `ops.load_control` (03), the April control-chain break (01) |

## Build order

1. **Harness first:** differential grading against the pristine tree, the `fixture_sha` cache-key extension, the history-manifest extension of `seed_runs`, harness-side fixture generation. The first two gate tier 1; the second two gate the diagnosis family and FIN-388.
2. **The generator** (07): simulate the upstream model (02), derive the extracts (03), with the invariant pass and the trap conservation test in place.
3. **The documents layer in one batch** (06) — every edit after tasks ship retires their trials.
4. **The base tree** (04, 05): the six team projects, the house library docstring-first, the placed items, `legacy/`.
5. **The hand-built ledger** (07) and its independent reconciliation.
6. **Then tasks**, catalog tier 1 first, probe-first gate unchanged.

## Open questions

1. Whether `world.yaml` can carry an environment delta — gates the `copperline-modern` variant and CT-4 (05).
2. Shipped-fixture size and clickstream scale — the size budget in 03 needs a setup-cost probe before it is frozen.
3. FIN-388's runtime margin — probe the per-partition runtime and size the brute-force plan to at least 1.5x the wall cap.
4. Whether the fixture API stub may bind a loopback port under the trial harness — gates the pagination lever (05).
5. Agents keep unrestricted network in trials — the standing question from the catalog; it caps every verification-skipped bet.
6. CA-2's ticket points at the trap runbook — a deliberate bet against the catalog's never-point rule; probe it before authoring at scale (06).
7. Era count: if eight eras read as contrived, cut from the least-armed edges inward; the four boundaries concentrated in one year (the re-key, the acquisition, the processor overlap, the UTC cutover) are load-bearing and are not cuttable (01).
