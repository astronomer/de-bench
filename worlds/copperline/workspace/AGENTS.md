# Copperline data platform

Copperline Retail Group sells home, hardware and outdoor-living goods through
268 stores in five countries, copperline.com, and Copperline Marketplace, where
third-party sellers list and we take a commission. Copperline Trade is the
wholesale arm, and its named trade accounts are the customer grain every finance
and board report counts. This is the monorepo the eleven-person data team works
in.

Read `CONVENTIONS.md` first, then the `CONVENTIONS.md` of the team whose folder
you are working in.

## Where to start

| You want | Go to |
|---|---|
| the house rules | `CONVENTIONS.md` |
| what a report is and who owns it | `docs/report-registry.md` |
| who reads a mart before you change it | `docs/lineage.md` |
| what a number means | `docs/semantic-definitions.md`, then the policy it names |
| what a consumer is promised | `contracts/<consumer>.md` and the `.yml` beside it |
| who decides when two of those disagree | `docs/change-management.md` |
| the shared helpers | `include/lib/`, and their docstrings |
| the DAG factory | `docs/blueprints.md` |
| what broke before | `ops/incidents/` |

## The teams

Six teams share one warehouse and one Airflow deployment. They meet through the
warehouse and through published partition files, so "who owns this table" is a
real question with an answer.

| Team | Project | Owner | Holds |
|---|---|---|---|
| data-platform | `projects/platform/` | P. Vance, H. Oyelaran | `include/lib/`, the factory, the staging layer, the conformed dims, the shared `int` models, the contracts, the deployment config |
| finance-analytics | `projects/finance/` | N. Brandt | recognition, the ledger tie, the fiscal calendar, restatements, the daily flash |
| commerce | `projects/commerce/` | J. Mwangi | orders, order lines, payments, settlement, reconciliation, store sales |
| supply-chain | `projects/supply/` | K. Duffy | inventory, carriers, freight cost, the workbook, the partner share, the legacy estate |
| growth | `projects/growth/` | S. Rasmussen | clickstream, marketing spend, pricing, reverse ETL, alerting |
| customer | `projects/customer/` | L. Baptiste | the 360, the Northwave merge, support, loyalty, the feature table |

The platform team publishes no business table. It owns what every other team
imports, and the deployment config that overwrites theirs.

A project never reaches into another project's `dags/`. Shared code goes to
`include/lib/`; a team's own code goes to `projects/<team>/lib/`.

## The warehouse

One DuckDB file, `include/data/copperline.duckdb`, and it takes one writer. A
scheduled run holds it, so anything that only reads takes
`include.lib.warehouse.connect(read_only=True)` and works against the snapshot.
`raw.*` is landed source and is read-only to everything. `ops.*` is bookkeeping.
`marts.*` is what the company reads.

The snapshot carries the landed half only — `raw.*`, `ops.*` and the attached
`nwv`. `staging.*` and `marts.*` are derived, so the snapshot ships without
them and the runs rebuild them from source. To see what a mart held on a day,
run that day.

The frozen `nwv` database is attached beside it, first on the search path, and
it stopped refreshing when the Northwave namespace was frozen. Qualify every
name you write. `include/lib/warehouse.py` explains why in full.

## What runs

About 115 DAGs across the six projects. The daily shape:

```
01:00  retention, snapshots, the legacy nightly load
02:00  the intakes that are not hourly
03:00  enrichment, the legacy history bracket
04:00  the dbt build (plat_dbt_analytics_daily), the config sync
05:00  the morning marts, the flash at 05:45
06:00  contracts enforced, the revenue and replenishment marts
07:00  the 360, tax, loyalty, close on the 1st
08:00  the ledger tie, the GL export, exports
09:00  lineage, the board pack on Mondays, the partner drops
```

Hourly intakes run on their own minute so they do not all land at once.

## The seams

Four of them, and half the odd behaviour in this tree comes from one of them.
Each has a runbook under `docs/runbooks/`.

- **The Northwave acquisition.** An acquired customer book, a crosswalk between
  the two id formats, and a frozen namespace that stopped refreshing.
- **The processor migration.** Halcyon Payments to Meridian Pay, with a shadow
  quarter where both feeds ran.
- **The UTC standardisation.** Store-local close batches before it, UTC after.
- **The legacy estate.** Thirteen Pentaho jobs, an SSIS package and an AutoSys
  calendar that were never ported. Airflow drives them over SSH, one step per
  task.

## Running things locally

The platform exports `DUCKDB_PATH` and `WORLD_TODAY`. A local shell sets them
by hand. `WORLD_TODAY` is the business date and
`include.lib.calendar.today()` is the only thing that reads it — nothing in this
tree reads the wall clock.

`dbt` runs from `dbt/copperline_analytics`. The warehouse takes one writer, so a
`dbt run` started while the 04:00 build is in flight fails on the lock; work
against the snapshot instead. `dbt/README.md` says how.

A DAG runs one day at a time with `airflow dags test <dag_id> <ds>` from the
checkout root. It runs the real tasks against the real warehouse, so point
`DUCKDB_PATH` at a copy first if you only mean to look.
