# The warehouse

Read this before you run anything. It is the answer to "why will dbt not
start", "which file am I writing to", and "why did my model land in the wrong
schema", and it is meant to be read once rather than searched.

Maintained by the data platform team. Last reviewed 2026-05-18.

## Two projects, and only one of them is alive

| Project | What it is |
|---|---|
| `dbt/copperline_analytics/` | The warehouse. Every team's models. Build this. |
| `dbt/northwave_reporting/` | Inherited with the acquisition. Read its own README before touching it. |

`northwave_reporting` builds against a different file, under a different
profile, with a different package pin. Some of it still runs nightly and most of
it does not, and the tree does not say which. It is not part of the analytics
graph and `dbt ls` in one project cannot see the other.

## Run dbt through `/opt/dbt-venv/bin/dbt`

dbt is not in the Airflow environment and cannot be: the scheduler moved to
Airflow 3 during the orchestrator migration, and the two dependency sets stopped
resolving together the same week. So the platform team put dbt in a virtualenv
of its own and left it there.

```sh
/opt/dbt-venv/bin/dbt run --project-dir dbt/copperline_analytics
```

Cosmos calls the same path, and so does every shell task that shells out to dbt.
A bare `dbt` on the PATH is either not there or is not the pinned one; either
way it is not what the nightly runs.

## The pins, and why they are where they are

| Package | Pin |
|---|---|
| `dbt-core` | `==1.6.14` |
| `dbt-duckdb` | `==1.6.2` |
| `dbt-utils` | `==0.8.6` |

Held there until the finance close moves off the old snapshot blocks — see
`docs/change-management.md`. Two things in `models/shared/` break on the newer
versions and neither breaks loudly:

- **`surrogate_key`.** At 0.8.6 it exists and treats NULL as the empty string.
  At 1.x it is renamed `generate_surrogate_key` and hashes NULL as the literal
  string `NULL`. Every key in the shared layer changes value on that move, which
  reshuffles every incremental table keyed on one. The `surrogate_key_treat_
  nulls_as_empty_strings` var in `dbt_project.yml` is what holds the old
  behaviour; do not turn it off to "clean up".
- **`deduplicate`.** The 0.8.6 signature is `(relation, partition_by,
  order_by)`. 1.x adds a relation alias.

`docs/runbooks/upgrade.md` states the order the pins move in, and
`tools/check_upgrade.py` is the pre-flight. Read what the checker actually
checks before you trust a green result from it.

## Two files, and one writer

The warehouse is one DuckDB file and **DuckDB takes one writer**. Every profile
in this project is single-threaded on purpose. Raising `threads` does not make a
run faster; it makes it fail partway through with a lock error, usually on
whichever model was going to take longest.

| File | What for |
|---|---|
| `include/data/copperline.duckdb` | The warehouse. The nightly writes it. |
| `warehouse/snapshot.duckdb` | A copy for read-only work. Query this one. |

If you want to explore, point at the snapshot. If you run a query against the
live file while the nightly is building, you will either block it or be blocked
by it.

The path is assembled from two environment variables and there is no absolute
path anywhere in this project:

```sh
COPPERLINE_HOME=/usr/local/airflow          # the checkout root
DUCKDB_PATH=include/data/copperline.duckdb  # relative to it
```

Run dbt from the project directory and both have working defaults.

## The clock is pinned. Do not read the wall clock

Every model that needs "today" reads `{{ var('ds') }}`, which defaults to the
`WORLD_TODAY` environment variable. Nothing in this project calls
`current_date`, `now()`, or a Python clock.

That is not a style rule. Every backfill this team has run is a replay, and a
model that reads the wall clock gives one answer on the night it ran and a
different one on the replay. `plat_backfill_broker` passes the date it is
replaying as `--vars '{ds: ...}'`, and a model that ignores it silently produces
today's numbers stamped with a date from two years ago.

`{{ ds() }}` and `{{ ds_minus(n) }}` in `macros/schemas.sql` are the two forms
to use.

## Money is an integer

**Every monetary column in a landed or published table is an integer in minor
units.** A money column is `*_cents` and it is `bigint`. No float, ever.

A fact that converts currency carries three things beside the result: the amount
in its own currency, the rate as `fx_rate_ppm` — an integer, parts per million —
and the `fx_rate_date` that rate came from. That is so a reader can redo the
arithmetic from the row without going back to the rate table and guessing which
day's rate was used.

The upstream applications keep decimal amounts, because that is what an ERP
holds. The conversion to integer minor units happens at the extract boundary and
nothing downstream of it undoes that.

`macros/money.sql` has `to_cents`, `to_base_cents` and `apply_bps`. Use them
rather than writing the rounding again: they all multiply first, divide once,
and round half up, which is what `docs/finance-policy.md` REV-2 asks for.

## The four layers

| Layer | Where | Materialisation | What it is |
|---|---|---|---|
| `raw` | the extracts | tables | the landed feeds. Read-only. |
| `stg` | `models/shared/staging/` | views | one view per landed table, 1:1 |
| `int` | `models/shared/intermediate/`, `models/<team>/intermediate/` | views | shared definitions, and grain collapses |
| `marts` | `models/<team>/` | tables | each team's published contract |

Staging is `stg_<source>__<table>`, one view per landed `raw.*` table. It casts,
renames, dedupes and filters. **It does not join.**

`int` is earned, not mandatory. A model reaches it when two teams use its
definition or when it collapses a grain. Support has neither, so its tickets go
`stg` straight to a mart with no `int` step at all — that thin case is the
example, not the exception.

`projects/platform/README.md` holds the inventory of what the platform team
owns: every staging view, every shared `int` model and every conformed
dimension, with the teams that read each one.

## The six rules

These are Copperline's stated norms. They are the same six in
`projects/platform/README.md` and every contract file assumes them.

1. **Marts read `stg` and `int` only. Never `raw`.** A mart that reaches past
   staging into a landed extract has bypassed every cast, rename and dedupe the
   staging layer exists to apply.
2. **Marts do not read other teams' marts.** Cross-team logic moves to `int`. A
   team that needs another team's number needs the definition behind it, not the
   table.
3. **An aggregate may read its own team's base fact.** That is the one exception
   to rule 2 and it does not extend across a team boundary.
4. **A definition two teams use belongs in `int`.** The first team to need it
   does not own it; platform does.
5. **Conformed dimensions are built once and versioned.** A team that wants a
   different `dim_product` opens a conversation, not a model.
6. **Every monetary column is an integer in minor units.** See above.

Two models break rule 2 today and both are known: `models/finance/category_margin.sql`
reads commerce's `marts.order_economics`, and `models/growth/channel_roi_daily.sql`
reads commerce's `marts.gmv_daily`. Both say so in their headers. Two conformed
dims break rule 5: `dim_product` is built by commerce and `dim_customer` by the
customer team, under a platform contract they both satisfy. Neither move ever
finished.

## `dbt ls` is not the list

`docs/lineage.md` is the list of every consumer of every mart, and about half of
them are invisible to dbt: dashboard SQL that names tables as strings, exports,
config-assembled readers, two sensors that match on a file-name pattern, and
reverse-ETL jobs that push a mart's output to systems outside the building.

Before you change a mart's grain, its columns or its partition key, work
`docs/lineage.md`. `plat_lineage_publish` regenerates `ops/lineage.json` from
the manifest every morning and that file holds the `ref` rows only — six of the
twelve consumers on `marts.order_economics`. It is useful and it is not the
list.

## Tests, and what a failure means

Tests are placed by layer so a failure says *where* the problem is.

| Layer | What it tests |
|---|---|
| `stg` | key uniqueness, not-null on what the source guarantees |
| `int` | grain, fan-out, referenced keys exist |
| `marts` | the rules a consumer's contract states, and the reconciliation ties |

A failing `stg` test means the source changed. A failing `int` test means a
grain broke. A failing mart test means two things that should tie no longer do.
"Make the suite green" has three different right answers and none of them is
deleting the test.

**Twelve tests fail on this tree right now.** Read
`docs/memos/2026-04-quality-contract-revision.md` before you touch any of them:
QC-3 says a test written before that memo is stale until it has been re-derived
from QC-1, and several of the twelve are exactly that. Some of the others are
real. Telling them apart is the work; muting the selection is not.

## Running it

```sh
DBT=/opt/dbt-venv/bin/dbt
$DBT deps      --project-dir dbt/copperline_analytics
$DBT snapshot  --project-dir dbt/copperline_analytics
$DBT run       --project-dir dbt/copperline_analytics
$DBT test      --project-dir dbt/copperline_analytics
```

Snapshots first. `dim_product` is built from `snap_product_price` and
`snap_product_attrs`, and on a warehouse that has never snapshotted there is
nothing for it to read.

Then `dbt run`, then `dbt test`, in that order, and that is what the nightly does:
`plat_dbt_analytics_daily` builds the models and `plat_dbt_test_nightly` runs
the test selection. **Do not use `dbt build` here.** It interleaves tests with
models and skips everything downstream of a failing test, so the twelve known
failures take about a fifth of the warehouse down with them and you get a run
that looks far worse than it is.

`ops_audit_log` is written by the run hooks — `on-run-start` opens a row before
the graph starts, `on-run-end` closes it. An invocation with an open row and no
close is a run that died, which is the row worth alerting on.

## Two things that will catch you

**Schema names are used verbatim.** `macros/schemas.sql` overrides
`generate_schema_name` so that a model configured into `marts` lands in `marts`
and not in `main_marts`. Every consumer outside this project names its tables
`marts.<model>` as a string. Restore dbt's default prefixing and all of them
break, and none of them breaks loudly.

**No full refresh on the shared warehouse.** Any model over a million rows is
incremental with a `unique_key`. A full rebuild of one locks the file for
minutes while every other team's DAG waits. `models/commerce/fct_order_line.sql`
is the one that matters; its incremental window is the trailing fourteen days of
**order date**, not of load time, because an order can be restated after it
lands and a load-time window misses every restatement.
