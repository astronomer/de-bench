# customer — how we write DAGs and models

Owner: L. Baptiste. These rules add to the platform `CONVENTIONS.md` at the
repository root and override it inside `projects/customer/`. They are style.
Read them before you open a file here, then match the file you are in.

## Naming

Every `dag_id` starts `cus_`. A `dag_id` is a contract with everything
downstream and is never renamed.

## Style: mixed, follow the file

This team has no single style. Some DAGs are TaskFlow, some are classic
operators, and several are both because the operator *is* the work — a house
loader, a sensor, a bash step. Match the file you are editing rather than the
file you last wrote. A DAG that changes style halfway is harder to read than
either style would have been.

## Schedule: cron, and one asset

Everything here runs on cron. `cus_360_publish` is the exception: it fires on
the 360 table's asset, because the model it publishes is the world's slowest
and a clock guess would either wait too long or publish half a table.

## Both books, always

Copperline's book and the acquired Northwave book are separate and stay
separate.

- Every customer-facing model carries `source_book`, `copperline` or
  `northwave`, and it stays on the row after a merge.
- A join across the two books goes through `stg_crosswalk`. Never on a name, an
  address, a domain or any other natural key.
- Anything that reads dates before the acquisition states the era in the model
  description, because the acquired book's history predates our ownership of
  it.

`docs/runbooks/northwave-integration.md` is the state of the integration and
`docs/runbooks/customer-id-migration.md` is the state of the re-key. Both are
advisory documents; this file is the style rule.

## Personal data

A model that touches personal data declares it in `meta.pii`, and a column
declared there never lands in an export. Published contact detail is hashed;
the unhashed value stays in the warehouse.

## Surrogate keys

Surrogate keys come from `dbt_utils.surrogate_key`, with the columns listed in
the same order as the dimension they key. The order is part of the key: two
models that list the same columns differently produce two different keys for
one row, and nothing tells you which is which.

## Fanning out

Map over the two books rather than writing the same task twice. One mapped task
per book keeps the two legs in one place and makes the difference between them
visible in the code rather than in a diff.

## Paths and the warehouse

Warehouse work goes through `include.lib.warehouse`: `connect`,
`delete_insert`, `write_partition`. Nothing in this project opens the DuckDB
file itself or builds a partition path by hand.
