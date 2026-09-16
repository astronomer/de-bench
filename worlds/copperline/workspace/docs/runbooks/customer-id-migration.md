# Runbook: the customer id migration

Owner: customer. Review date 2026-04-07. Reads: `raw.customer_id_map`, `raw.customers`, `raw.orders`. Advisory, per `docs/change-management.md` §CM-3.

The commerce replatform re-keyed every trade account. This runbook is what anyone joining orders to customers across that boundary needs to know.

## CID-1 The cutover

The replatform cut over on 2024-11-04. Trade account ids changed from `CUST<4 digits>` to `C-<6 digits>` on that day. `raw.customers` holds 4,000 rows, all in the new scheme, with `created_on` spanning both eras. `raw.customer_id_map` holds 2,660 rows — `legacy_id`, `customer_id`, `migrated_at`, `note` — every one stamped 2024-11-04. Sixty of them carry no current id; CID-3 says what to do with those.

The store integration was not told, and kept writing old-format ids for two more weeks. So `raw.orders.customer_ref` carries old-format values before the cutover, `C-` values after it, and 45 old-format rows dated after it, through 2024-11-17. Those 45 are real orders from real accounts.

## CID-2 Applying the crosswalk

The crosswalk applies by id format, not by order date. A `customer_ref` that looks like `CUST####` goes through the map whatever date it carries; a `customer_ref` that looks like `C-######` is already current and is never mapped. Do not switch on the order date, and do not assume that everything after the cutover is in the new scheme.

Both readings produce a complete answer and one of them is wrong by 45 orders, so this is written down rather than left to judgement.

## CID-3 Accounts the map cannot translate

Sixty legacy ids have a map row with `customer_id` NULL and a note — "no current account found; bill under the legacy id". They are accounts that churned before the migration and were never carried across. They roll up under `customer_id = legacy_id`, untranslated, and they keep their old-format id in the dimension. Dropping them loses real history, and inventing a `C-` id for them is worse. Taking `m.customer_id` without the coalesce below turns all sixty into NULL, which is the quiet way to drop them.

## The join, written out

```sql
select o.order_id,
       coalesce(m.customer_id, o.customer_ref) as customer_id
from raw.orders o
left join raw.customer_id_map m
  on o.customer_ref = m.legacy_id
 and o.customer_ref like 'CUST%'
```

The `like` is the format test from CID-2 and the `coalesce` is CID-3. Take either one out and the answer is still complete and still wrong.

## What the counts should look like

| Population | Rows |
|---|---|
| accounts in `raw.customers` | 4,000 |
| map rows | 2,660 |
| map rows carrying a current id | 2,600 |
| map rows with `customer_id` NULL | 60 |
| post-cutover orders carrying an old-format ref | 45 |

The 4,000 and the 2,660 do not reconcile to each other and are not meant to: accounts created after the migration never had a legacy id at all.

## Open items

- The map is a one-off load and nothing maintains it. A legacy id that surfaces now has nowhere to go.
- `raw.orders.customer_ref` is the column name and it stays that way. The parquet orders export renames it on the way out, which surprises people who work from the export.
- TODO: nobody has checked whether the 60 orphans overlap the Northwave merge candidates. They are different books and probably do not.
- The old platform's own documentation is gone. The intranet page that held the id scheme has been dead since the migration, which is a small joke nobody enjoyed.
