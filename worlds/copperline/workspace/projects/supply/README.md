# supply-chain

Owner: K. Duffy. On call: the supply rotation.
Mail: `supply-data@copperline.example`.

Supply-chain owns availability, inbound flow, supplier performance, freight and
delivery. One physical flow: stock is bought, it arrives, it sits somewhere, it
moves. The team also carries what is left of the old estate — thirteen Pentaho
jobs, an SSIS package and an AutoSys calendar that nobody has ported.

## What we publish

| Mart | Grain | Who reads it |
|---|---|---|
| `marts.inventory_position` | day × SKU × location | the replenishment feed to Ironwood, the merch dashboards |
| `marts.sell_through_daily` | day × SKU × store | the Kestrel share, the merch dashboards |
| `marts.fct_shipping_costs` | package, one partition a day | `fin_accrual_freight`, the freight accrual |
| `marts.freight_accrual_monthly` | fiscal period × carrier | finance |
| `marts.lane_performance_daily` | day × lane | merchandising, the carrier reviews |
| `marts.supplier_scorecard_weekly` | week × supplier | merchandising, the quarterly reviews |
| `marts.dc_transfers_daily`, `marts.dc_capacity_daily`, `marts.backorder_daily`, `marts.shrink_weekly` | day or week × location | the DC managers |

Two decisions carry the whole layer, and both are older than anyone on the team.

**The snapshot and the movement replay disagree.** Corvid writes the snapshot
from its own count and movements post asynchronously, so replaying the movements
to a date does not reproduce the snapshot. Which one is authoritative is decided
once, in `int_inventory_position_daily`, and every downstream team reads the
position, never the movements.

**On-time-in-full is computed, not read.** No scorecard table exists upstream on
purpose. The computation is finance-analytics' three-way match as well, so it is
one model — `int_po_receipt_matched` — serving both.

## The DAGs

Twenty-two. Seventeen hand-written, five rendered from `.dag.yaml` by the
blueprint factory.

| Group | DAGs |
|---|---|
| inventory | `sc_inventory_snapshot_daily`, `sc_inventory_movements_hourly`, `sc_dc_transfer_daily`, `sc_shrink_weekly`, `sc_dc_capacity_daily` |
| carriers and freight | `sc_carrier_scan_intake`, `sc_carrier_api_poll`, `sc_carrier_invoice_intake`, `sc_rate_card_intake`, `sc_shipping_cost_daily`, `sc_lane_performance_daily`, `sc_freight_accrual_workbook` |
| service | `sc_fill_rate_daily`, `sc_backorder_daily` |
| suppliers | `sc_supplier_scorecard_weekly` |
| out | `sc_replenishment_daily`, `sc_partner_share_kestrel`, `sc_tidal_inventory_report` |
| the old estate | `sc_pdi_nightly_load`, `sc_pdi_history_complete`, `sc_ssis_supplier_master`, `sc_autosys_bridge` |

## The old estate

Wave 1 of the orchestrator migration moved nine nightly jobs here on the
cutover; thirteen did not move. `docs/runbooks/orchestrator-migration.md` is the
plan and `legacy/tidal/inventory_jobs.csv` is the inventory it was written from.
The spreadsheet is from November 2025 and it has not been kept up: two of the
target DAGs it names do not exist, and three of its rows are for jobs that were
already dead. Read it as a record of what somebody meant, not of what is there.

`ops.load_control` is the estate's own record of itself, one row per job per
night, and it is read-only from here. The nine that moved stopped writing to it
on the cutover; their run history is this deployment's from that date and there
is none before it.

## Reading one

`CONVENTIONS.md` holds the style. Four things catch people out and none of them
is style:

- The valuation columns on `raw.inventory_snapshots` change meaning at the
  FY2026 boundary. `docs/inventory-policy.md` INV-1 is the only place that says
  what the change was; the data announces it by going null.
- Movement words are Corvid's, not ours. A sale is a `pick`, a shrink is what a
  `cycle_count` found, a return goes out as an `rtv`.
- A rate card is chosen on the **ship date**, never on the day the rating runs.
  `docs/rate-policy.md` RATE-5.
- The carriers' `ship_date` is local at origin and `ship_time_utc` is not. The
  two disagree for packages dispatched near the ends of the origin's day, and
  the size of the disagreement changes on the two 2026 clock changes.

## The documents that govern us

| Document | What it decides |
|---|---|
| `docs/rate-policy.md` | which card is in force, how the lookup and the rounding go |
| `docs/inventory-policy.md` | the valuation method, its date, and its grain |
| `docs/late-data-policy.md` | how late rows land, and the publication holdback |
| `docs/retention-policy.md` | how long each landing tree keeps its files |
| `contracts/replenishment.md`, `contracts/partner-share.md` | what leaves the building, and in what shape |
| `contracts/inventory_position.yml`, `contracts/sell_through_daily.yml` | the published grain and columns |
| `docs/lineage.md` | every reader of every mart, including the two sensors that find their input by name |

## Two sensors wait on a name

`sc_freight_accrual_workbook` waits for the freight-cost partitions and
`sc_partner_share_kestrel` waits for the sell-through ones, both by file-name
pattern rather than by task. The patterns are in `lib/paths.py`, which is the
only place they are written down. Rename either output or change its layout and
the sensor waits until it times out: no error, no failed task, and in Kestrel's
case a vendor who stops receiving a file. `docs/lineage.md` lists both.

## Layout

```
projects/supply/
  README.md          this file
  CONVENTIONS.md     the team's style rules
  dags/              one file per DAG, plus blueprints.py and kinds.py
  dags/*.dag.yaml    the five rendered DAGs
  lib/paths.py       every file name and partition pattern this team uses
```
