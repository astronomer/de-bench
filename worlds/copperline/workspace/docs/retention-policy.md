# Retention and deletion policy

Maintained by the data platform team with legal. Last reviewed 2026-01-20. Applies to the landing tree, the warehouse and every export.

Copperline keeps landing files for as long as it is useful to be able to reload them, and no longer. Personal data is deleted on request across every surface it reached. The two rules meet in RET-3, which is the one people argue about.

## RET-1 Vendor landing files: 90 days

A landing file sent by a third party — the payment processors, the carriers, the ad platforms — keeps 90 days in the live landing tree. After 90 days it moves to the archive, and the archive copy is the only copy. `plat_partition_retention` runs nightly and does the move.

## RET-2 Our own landing files: 400 days

A landing file Copperline produces — the OMS extracts, the POS batches, the clickstream, the WMS files, the PIM change feed, the parquet orders export — keeps 400 days live, then archives. The number is thirteen months and a bit, so that a full year is always live along with the period being compared against.

## RET-3 Aggregates are immutable

A published aggregate partition is immutable. Deletion under RET-4 is a targeted DELETE against the row-level tables plus a documented restatement, never a rebuild of the aggregate. Rebuilding an aggregate to satisfy a deletion is a restatement of published history and needs finance sign-off.

## RET-4 Deletion requests: 30 days, every surface

A deletion request is honoured within 30 days of receipt, across every surface listed in `contracts/privacy.md`. The contract's list is the full list, and it includes surfaces dbt cannot see: the partner-share extract, the feature exports and the audience files. Requests arrive in `ops/privacy/deletion_requests.csv` and `plat_privacy_sweep` works them.

## RET-5 Exemptions: 7 years

`raw.finance_ledger`, the invoice and credit-memo tables behind it, and any account under legal hold are exempt from RET-1 through RET-4 and keep 7 years. Where a deletion request covers a record that is exempt, the exemption holds and the record stays. Record the exemption on the request; do not delete and do not silently skip.

## RET-6 Soft deletes stay

A row deleted at source is written to `audit.deleted_rows` with the row as last seen and the date it disappeared. That table is exempt from RET-1 and RET-2 and is itself in scope for RET-4. It is how a source that deletes rather than cancels stays reconstructable.

## What the archive is, and what it is not

The archive is object storage in the same region, write-once, with no index and no query path. Reading it is a restore job, not a query, and a restore of a month of payment files takes a couple of hours. It is a copy of last resort and it has one gap, which is in `ops/incidents/2026-02-16-archive-gap.md`.

There is no second archive. When a file is not in the live tree and not in the archive, it does not exist, and nothing downstream can be rebuilt from it.

## Retention by source

| Source | Class | Live |
|---|---|---|
| Meridian Pay settlements | vendor | 90 days |
| Halcyon Payments settlements | vendor | 90 days, frozen since 2025-10-01 |
| carrier feeds and rate cards | vendor | 90 days |
| ad platform exports | vendor | 90 days |
| OMS orders and order lines | own | 400 days |
| POS store batches | own | 400 days |
| clickstream | own | 400 days |
| WMS inventory files | own | 400 days |
| PIM change feed | own | 400 days |
| parquet orders export | own | 400 days |
| finance ledger | exempt | 7 years |
| accounts under legal hold | exempt | 7 years |

## The consequence people meet first

The Meridian landing zone holds 90 live days. Anything that reads further back than that is reading the archive, whether or not the code says so, and the restore is the slow part of any rebuild. Plan a rebuild that reaches past 90 days as a restore job with a query on the end of it.

The pre-history detail is gone for the same reason. FY2023 exists in the warehouse at store-day summary grain only; the line-item detail behind it aged out under RET-2 long before anyone wanted it back.

## Working a deletion request

The order matters, because RET-3 and RET-5 both remove options.

1. Take the request from `ops/privacy/deletion_requests.csv`.
2. Check RET-5. An account under legal hold, or a record that is part of the ledger, is exempt and stays. Record that on the request.
3. Delete from the row-level tables the request reaches, per the surface list in `contracts/privacy.md`.
4. Leave the published aggregates alone, per RET-3, and write the restatement note instead.
5. Sweep the surfaces outside the warehouse — the partner-share extract, the feature exports, the audience files. They are on the same list and dbt cannot see them.
6. Write the receipt.

Step 4 is the one that gets skipped, and the reasoning that skips it is sound right up to the point where a published fiscal month moves.

## Open items

- RET-3 was written after an argument about a deletion request that would have rebuilt a published fiscal month. It says what was decided. Nobody has enjoyed reading it since.
- TODO: `audit.deleted_rows` has no partition key and is scanned in full by two models. It is small now.
- The archive has no lifecycle rule of its own. Files written in 2019 are still there. Legal has asked twice.
- Deletion receipts are written to `exports/privacy/` and kept indefinitely, which is the point of a receipt, and nobody has confirmed that the receipt itself is free of personal data.
