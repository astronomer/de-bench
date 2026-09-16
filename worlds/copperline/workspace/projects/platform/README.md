# data-platform

P. Vance, H. Oyelaran. `data-platform@copperline.example`.

We own what every other team imports and nothing anybody reads. No mart in this
project has a business owner, and that is the point: five teams depend on this
one and this one depends on none of them.

| We own | Where |
|---|---|
| the house library | `include/lib/` |
| the DAG factory | `include/lib/blueprint/`, and `docs/blueprints.md` |
| the staging layer, one view per landed table | `dbt/copperline_analytics/models/shared/staging/` |
| the conformed dimensions | `models/shared/` |
| the shared intermediate models | `models/shared/intermediate/` |
| the two operational models | `models/shared/ops/` |
| the contracts, and the DAG that enforces them | `contracts/`, `plat_contracts_enforce` |
| the deployment config | `config/iac/`, applied by `plat_config_sync` |
| the listener plugin and the cluster policies | `plugins/`, `config/airflow_local_settings.py` |

Two conformed dimensions never finished moving here. `dim_product` is still
built by commerce's snapshot pair and `dim_customer` by
`cus_dim_customer_daily`, both under a contract in `contracts/` they have to
satisfy. It is on the list to close and has been for a year.

## The dependency rules

These six are world policy, not this team's preference. Every team's models are
meant to satisfy them, and `dbt/README.md` repeats them in its warehouse
section.

1. **Marts read `stg` and `int` only. Never `raw`.** A mart that reaches past
   staging into a landed extract has bypassed every cast, rename and dedupe the
   staging layer exists to apply.
2. **Marts do not read other teams' marts.** Cross-team logic moves to `int`. A
   team that needs another team's number needs the definition behind it, not the
   table.
3. **An aggregate may read its own team's base fact.** That is the one exception
   to rule 2, and it does not cross a team boundary.
4. **A definition two teams use belongs in `int`.** The first team to need it
   does not own it; this team does.
5. **Conformed dimensions are built once and versioned.** A team that wants a
   different `dim_product` opens a conversation, not a model.
6. **A model that changes a mart's grain or drops a column amends
   `contracts/<mart>.yml` first.**

The rules are also how a change is assessed. They say what the lineage is meant
to cover, so a reader outside it is a finding rather than a surprise — and this
estate has several. `docs/lineage.md` names every one of them, by table.

## The warehouse

One DuckDB file, one writer, and everything below follows from that.

- **No full refresh on the shared warehouse during the working day.** A full
  rebuild holds the single writer for as long as it takes and every other team's
  run queues behind it. Where a refresh is genuinely needed, it goes in the
  Saturday window beside `plat_warehouse_vacuum`, and the owning team says so
  first.
- **Any model over one million rows is incremental, with a `unique_key`.** Below
  a million a table rebuild is a few seconds and the simpler model wins. Above
  it, a rebuild is minutes of held lock. The `unique_key` is not optional: an
  incremental model without one appends, and a re-run of a day doubles it.
- Anything that only reads takes `connect(read_only=True)`, which works against
  a snapshot and never queues behind a build.

`models/finance/` states the opposite rule for its own folder, and
`docs/change-management.md` says which one governs where they meet. Two finance
marts have crossed a million rows, so this is not hypothetical.

## The two surfaces outside `dags/`

- `plugins/listeners.py` — the run ledger. Every task instance that finishes
  writes a row the ops dashboard reads.
- `config/airflow_local_settings.py` — the cluster policies. Every task gets at
  least two retries and an owner that is a team.

## `config/iac/` is the deployment

`plat_config_sync` applies `config/iac/` every morning at 04:00. It PUTs the
full declared set: the files are the whole truth, and a variable, connection or
pool that is not in them is removed on the next pass. That is what "declarative"
means here and it is the reason a change made in the UI does not survive the
night. Make the change in the files.

## What breaks when we break

Everything. A change here crosses every team boundary in the estate, which is
why the staging layer and the shared intermediates move at the pace they do.
Work `docs/lineage.md` before a mart's shape changes, and read
`docs/change-management.md` before a contract does.

## Running the build by hand

`plat_dbt_analytics_daily` holds the writer from 04:00. A `dbt run` started
against the live file while it is in flight fails on the lock. Take a snapshot
first — `scripts/warehouse_snapshot.sh`, or `connect(read_only=True)` — and work
against that. `dbt/README.md` has the commands.
