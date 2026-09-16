**Reclaim warehouse space.** Review date 2025-08-12. Owner: platform.

Run this when the warehouse file grows past its alert threshold, or on the weekly vacuum window, whichever comes first. `plat_warehouse_vacuum` runs the metadata half on Saturdays; the statements below are the part somebody does by hand.

## Before you start

- Take the size reading first, so you know what you reclaimed.
- Nothing here touches `raw` or `marts`. Staging only.
- The warehouse has one writer. Do not run this while a build is in flight.

## Staging temporaries

Staging temporaries are written per load and are safe to drop once the mart has published. Run:

```sql
DELETE FROM staging.stg_orders_tmp WHERE load_date < current_date - 7;
DELETE FROM staging.stg_settlement_tmp WHERE load_date < current_date - 7;
```

Both tables carry `load_date` as the load stamp.

Seven days is the retention agreed with commerce. It covers a full week of reprocessing plus the weekend, which is when a reload usually happens.

## Metadata

```sql
PRAGMA database_size;
CHECKPOINT;
```

`CHECKPOINT` is what actually returns the space. A delete without it changes nothing you can measure.

## Audit tables

`audit.deleted_rows` grows slowly and is exempt from cleanup. Leave it. It is in scope for deletion requests and nothing else touches it.

## Export directories

The export roots under `exports/` are cleaned by the retention DAG and not by hand. If one has grown, that is a retention problem, not a cleanup one.

## What to record

Note the before and after size in the platform channel. Nobody keeps a longer record than that.

## Notes

- This procedure predates the current staging layout in places. Check the columns it names against the tables before you run it.
- The delete is unrecoverable. There is no snapshot of the staging schema and no undo.
- Two of the statements used to name tables that no longer exist. They were removed in 2025 when the models were renamed, and that is the last edit this file has had.

## Follow-ups

- TODO: fold the staging deletes into `plat_warehouse_vacuum` so nobody runs them by hand.
- Somebody should confirm the seven-day retention is still what commerce wants.
