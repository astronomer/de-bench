# MER-618 — the enriched spine has never carried a product column

BI raised this against the merch pack. `staging.orders_enriched` has three
product columns on it — `line_count`, `unit_count`, `category_count` — and the
pack has been reading all three since March. Every one of them is null on every
row, on every day we have.

`join_product` is the step that fills them and it has never finished. It runs
`projects/commerce/sql/enrich_product.sql`, which reads `marts.dim_product` and
two columns on it, `valid_from` and `valid_to`. Nothing in this tree writes a
table that has them. The step raises, retries twice, gives up, and the other
three joins publish without it — `enrich_publish` left-joins what
`join_product` was meant to leave behind, so the spine lands every night with
the three columns null and no run has ever gone red over it.

## What this ticket is not

**Building the history.** MER-612 is spanning Palette's change feed out into an
effective-dated dimension, and it has not merged. We are not waiting for it and
we are not writing a second one alongside it. `product_snapshot_daily` is
theirs.

**Repointing `check_snapshot_grain`.** That step is the other reader of the same
two columns, and it is how MER-612's table gets checked. It stays exactly as it
is. We owe them a written answer about it instead, and that is the last section
here.

CLS-208 hit the same wall on the close's price book last month and did not wait
either: the book comes off `raw.pim_product_versions`, the feed the dimension is
built from, with the close date put on it. Do the same here.

## The record in force on a date

Nobody wrote this down for the enrichment. It is written down now.

1. **The clock is `updated_at`, read as a date.** `received_at` is when the feed
   reached us and it orders nothing — `docs/late-data-policy.md` says the feed
   arrives out of order with no bound at all. A change stamped at any hour of a
   date is in force from that date, so a change made at 13:47 counts for the
   whole of the day it was made.

2. **A date carries one change per SKU.** Where a SKU has two changes on one
   date the source decides, strongest first: `pim_ui`, then `supplier_feed`,
   then `bulk_load` — a person editing the record beats a supplier's file, and a
   bulk load is weakest because it is a re-import of what was already there. Two
   from the same source on one date: the later stamp wins. The rule is spelled
   out twice under `dbt/`; read either.

3. **`operation = 'delete'` retires the SKU.** It takes part in the ordering. A
   SKU whose newest change on or before the order date is a delete has no record
   in force that day and contributes nothing to that day's orders. A later
   upsert brings it back from that change's date. Drop the deletes before you
   rank and every retirement in the catalogue disappears.

4. **Most of the order book is not in the catalogue.** The order book carries a
   far wider SKU range than the PIM does, so the product join misses on most
   lines. Every order keeps its row and every line is counted whether or not its
   SKU resolves. `dim_product`'s own docstring and `fct_order_line`'s both say
   so, and `fct_order_line` puts a number on what dropping the unmatched lines
   costs.

## What the step has to leave

`staging.enrich_product`, keyed `order_id`. Four columns, and the names are the
contract with `enrich_publish`, which reads three of them by name.

- `order_id` — one row for every order in `staging.enrich_base` for the date,
  and no others. The base is the population and it is already settled: staff
  transactions are gone from it, soft-deleted orders are still in it, and
  neither of those is reopened here.
- `line_count` — the order's lines.
- `unit_count` — the sum of their `qty`.
- `category_count` — how many distinct categories the order's lines resolve to,
  under the record in force on the order's business date.

Rerunnable: a second run of the same date leaves the table as the first run left
it.

`projects/commerce/CONVENTIONS.md`: one statement per file, and bind values as
parameters rather than formatting a date into the text. `CONVENTIONS.md` rule 4:
qualify every name, because an unqualified one resolves against the frozen `nwv`
copy first.

**Two DAGs run this statement.** `orders_enrich_daily` is ours;
`nightly_close` runs the same file for the close date. Both call it the way they
call it today, and both still have to run it after you are finished. Neither DAG
changes shape: `orders_enrich_daily` keeps its eight steps and their names.

## What we owe MER-612 and BI

Four short answers in `RESPONSE.md` at the root of the working tree. They go
into the MER-612 review, so keep them to the numbers and the verdict.

1. How much of the order book the catalogue can speak for: how many distinct
   SKUs the order book carries, how many the change feed carries, and how many
   are in both.
2. Whether `check_grain`, the last step of this DAG, would have caught a
   `join_product` that dropped every order with no catalogued line — and what
   makes that so.
3. What `check_snapshot_grain` will return on the first night MER-612's history
   lands: both of the numbers `snapshot_open_rows.sql` comes back with.
4. How many SKUs the feed has retired and never brought back. That is the
   number the second figure in (3) turns on, and MER-612 will be asked it.

## Running it

`airflow dags test orders_enrich_daily <ds>` runs the whole DAG, and
`publish_enriched` will stop on `staging.orders_enriched`, which has no DDL in
this tree and is not ours this week. Drive `build_base` and `join_product` by
hand instead — both are callables taking a date and nothing else. Make the
`staging` schema if it is not there: `AGENTS.md`, "The warehouse", says the copy
here ships with the landed half only.
