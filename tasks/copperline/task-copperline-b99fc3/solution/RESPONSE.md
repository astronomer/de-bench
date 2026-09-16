# SUP-1174 — which loads double on a repeated file

## What made 3 November possible

Both house loaders take the same `mode` / `partition_col` pair, and **`mode` is
`"append"` by default**. An append load has no key check and no dedup, so the
same file read twice puts every row in twice and both runs report success.
That is the whole of it: nothing about the second load was an error.

The quieter one is worse, because the call reads as though it is already right.
**`mode="replace"` with no `partition_col` is accepted.** The load cannot scope
the delete without a column to scope it to, so it writes one line at INFO —
`mode=replace without partition_col, loading as append` — and appends. The guard
is there on purpose: a table-wide delete on the shared warehouse locked it for
nine minutes once. The result is that `replace` on its own is not a replace.

So a load is safe to repeat only when it passes **both** arguments together.

## Which loads append, and whether they are safe

Six call sites append. All six are safe, and for two different reasons.

**Four land in a stage the run empties first.** `marketplace_orders_na` and
`marketplace_orders_eu` each run `reset_stage`, which drops
`staging.mkt_orders_<region>` before the listing fills it; `returns_daily` and
`price_change_intake` do the same through their `reset_stage` step, which drops
`staging.returns_landing` and `staging.pim_changes`. Appending into a table the
run has just taken over is right. Each then merges onto the real table on the
row's key — `rma_id`, `change_id`, `marketplace_order_id` — so a repeated file
leaves one copy.

**Two land in a change-feed inbox that is never emptied.**
`cus_customer_intake` and `cus_support_tickets_intake` append every page of the
Halyard and support feeds into `ops.crm_account_inbox` and
`ops.support_ticket_inbox`. The inbox keeps every version it has seen on
purpose, and `merge_inbox` folds it onto the book with the newest version of
each account or ticket winning. A repeated file adds rows to the inbox and
changes nothing on the book.

**Every dated load passes both arguments.** `orders_intake`, both loads in
`payments_intake`, `sc_inventory_snapshot_daily`, `sc_rate_card_intake`,
`fin_cash_recon_daily`, `marketplace_settlement_intake`, `sc_carrier_api_poll`,
`sc_carrier_scan_intake` and the rendered `fx_rates_intake`, `cus_loyalty_daily`,
`cus_nps_weekly` and `gro_comp_prices_intake` all name a `partition_col` beside
the `replace`.

`gro_email_engagement_daily` is worth a second look and then clears: its
`partition_col` is the expression `event_time_utc::DATE` rather than a column
name, because Larkspur sends an event timestamp and no date column. The delete
is still scoped to one day and the yaml says so.

## The one job the two arguments cannot save

**`sc_carrier_invoice_intake`** — the carrier invoice feed, which is the feed
the 3 November file belonged to.

Its three steps pass `mode: replace` and `partition_col: invoice_date`, which
looks like the safe call. It is not, because the files are **not dated**. The
carriers key their subtree by carrier, not by day:
`landing/carriers/<carrier>/invoices.csv` is one file per carrier, rewritten
each month, carrying every period still in retention.
`projects/supply/lib/paths.carrier_invoices` says so.

The figure: Brightline's file holds **20 invoices on 20 distinct invoice dates**,
2024-12-06 through 2026-06-20, and the other two carriers' files are cut the
same way. A `replace` deletes one `partition_value`, which defaults to the run's
own `{{ ds }}`. So the run clears at most one of the twenty invoice dates and
then inserts all twenty. Nineteen land on top of rows that are already there.

The step's own comment — "each load replaces the invoice dates it brings rather
than appending to them: the same file delivered twice leaves one copy" — states
something the operator cannot do. `partition_value` is one value.

## What to do about it, at the call site

Give the feed the shape the estate already uses for a keyed, undated file, which
is what `returns_daily` and `price_change_intake` do:

1. empty a staging table the run owns — `staging.carrier_invoice_landing`;
2. land all three carrier files into it, where an append is correct;
3. merge onto `raw.carrier_invoices` on `carrier_invoice_id`, newest wins, so a
   reissued period corrects the row it belongs to and a re-sent file leaves one
   copy.

That is a change to `projects/supply/dags/sc_carrier_invoice_intake.dag.yaml`
and the SQL beside it. Nothing in `include/lib/` needs to move; the loader's
`mode` and `partition_col` mean what they say, and the feed is the thing that
does not fit them.

Worth raising separately: nothing in the estate has a check that a load did not
double. The 3 November file was found by a weekly accrual, six days late.
