# MER-612 — the price on the day, from a dimension that only knows today

Merchandising ran the Q1 realisation review and every line came back against
today's list price. A jacket we marked down in November reads as though it was
never marked down. Two SKUs we retired in March are priced as though they are
still on the shelf. The review is the one finance takes to the category owners,
so it has to be the price that was in force on the day the thing sold.

The dimension is supposed to be effective-dated, and three statements already
read it that way:

- `close_price_book` takes the price book in force on the close date out of
  `marts.dim_product`;
- `enrich_product` takes the product record in force on the order's business
  date out of the same table;
- `check_snapshot_grain`, the last step of `product_snapshot_daily`, counts its
  open rows.

All three read `valid_from` and `valid_to`. Nothing in this tree writes a table
that has them.

What we have instead is the dbt pair. `snap_product_price` and
`snap_product_attrs` each take the newest row per SKU out of the change feed and
leave dbt to version it from one night to the next, so the history begins on the
night the snapshot first ran and holds nothing before it — and a change that
reaches us late is dated the night we noticed it rather than the day it took
effect. Palette's feed holds the real thing: every change to every SKU since we
turned it on, in `raw.pim_product_versions`, each one carrying the date it took
effect.

Build the history from the feed.

## The contract

Nobody wrote this down, which is half of why we are here. It is written down
now, and it is what the review needs.

`marts.dim_product`: one row per SKU per span of time in which its record did
not change. Columns, in this order.

    sku, product_name, category_id, brand, supplier_id,
    list_price_cents, status, valid_from, valid_to

1. Spans are dated by day and half-open. `valid_from` is the date the change
   took effect, `valid_to` is the date the next change took effect, and
   `valid_to` is null on the row in force now. A reader takes
   `d >= valid_from and (valid_to is null or d < valid_to)` — both statements
   above already do — and gets at most one row for a SKU on a date.

2. **The clock is `updated_at`.** `received_at` is when the feed reached us and
   it orders nothing. `docs/late-data-policy.md` says the feed arrives out of
   order with no bound at all, and it means it: a change that landed a week
   after the change that superseded it still belongs where its own stamp puts
   it. It splices into the history. It is not appended to the end of it.

3. **A day carries one row per SKU.** Where a SKU has two changes on one date
   the source decides, strongest first: `pim_ui`, then `supplier_feed`, then
   `bulk_load` — a person editing the record beats a supplier's file, and a bulk
   load is weakest because it is a re-import of what was already there. Two
   changes from the same source on one date: the later stamp wins. The one that
   loses leaves no row behind, and no span opens and closes on the same date.

4. **`operation = 'delete'` closes the span in force on its date and opens
   nothing of its own.** If nothing follows it the SKU has no open row at all,
   and an as-of read of any later date returns nothing for it. That is how the
   catalogue records a retirement, and it is what the two March SKUs above
   should have done.

5. **Palette resurrects a key.** A delete can be followed by an upsert months
   later. The SKU comes back with a fresh span from that change's date, and the
   days in between belong to no span.

6. Rebuilt whole every night, from the feed. It is under two thousand SKUs and
   it costs nothing. A second run of the same night has to leave the table as
   the first run left it.

## Where it goes

The statement goes in `projects/commerce/sql/product_history_build.sql`.
`projects/commerce/CONVENTIONS.md`: one statement per file, and `CONVENTIONS.md`
rule 4: qualify every name, because an unqualified one resolves against the
frozen `nwv` copy first. It takes no parameters — the rebuild is whole, so there
is no date to bind.

The step goes on `product_snapshot_daily` as `build_product_history`, a
`PythonOperator`, after `build_dim_product` and before `check_snapshot_grain`.
The DAG keeps its four steps and gains that one.

## Out of scope

**`dbt/`.** The dbt model of the same name is a current-row table the merch
dashboards read, and it is a second producer of `marts.dim_product`. Which of
the two keeps the name is a conversation commerce and platform have been having
for four quarters and it is not this ticket. Our step runs after the dbt one, so
what the close reads at 02:00 is the history.

**The feed.** `raw.pim_product_versions` is landed source and is read-only,
mess and all. The mess is the point: the contract is what turns it into a
history.

`include/lib/`, `fixtures/`, `legacy/`, `CONVENTIONS.md` and the other five
projects are not ours this week either.

## Before you finish

You will not get the whole DAG through on this box. The two snapshots and the
dbt run want the analytics venv and a warehouse a nightly has already filled.
The build step runs on its own, and `marts.*` is derived, so the copy here ships
without it — `AGENTS.md`, "The warehouse". Make the schema if it is not there.
