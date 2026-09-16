# STO-419 — the regional trading review has no store-day numbers

Store ops run the regional trading review on FY2025 periods 9 and 10. There is
no table behind it. District managers have been mailing spreadsheets, two
districts have sent different numbers for the same store on the same night, and
nobody can say which of them is right.

Everything the review needs is already in the warehouse. What is missing is one
table, at store and day, that everybody reads instead of counting again.

## What to build

A one-off job, `store_trading_day_backfill`, in `projects/commerce/dags/`. It
fills `ops.store_trading_day` for the two periods — **2025-10-05** to
**2025-11-29**.

| column | what it holds |
|---|---|
| `store_id` | the store |
| `market_code` | the market the store reports into |
| `ds` | the store's trading day, and the partition key |
| `txn_count` | transactions the till recorded that day |
| `void_count` | of those, the ones that were voided |
| `return_count` | of those, the ones that were returns |
| `net_cents` | the net the till took, voided transactions left out |

The table is not in the warehouse yet.

## What it has to hold

- One row per store per trading day, for the days a store traded and no
  others. A store that sent us nothing is not a store that took nothing, so a
  day with no transactions behind it does not belong in the pack.
- A voided transaction still happened. It is counted and it took no money. A
  return is a transaction the till recorded too, and its amount is on the row
  the way the till wrote it — the pack does not net returns off.
- The day is the store's own trading day. A district manager reads a line of
  this table next to his store's end-of-day report for that night, and the two
  have to agree.
- It covers one window once, so triggering it with no configuration does the
  whole window.

Every store the till data covers is in scope, the acquired estate included.
