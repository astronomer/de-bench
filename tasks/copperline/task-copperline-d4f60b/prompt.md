# DQ-96 — the store tie DQ-77 left red

DQ-77 sorted the nightly's twelve failures into two piles. Nine asserted a rule we no
longer hold and were retired. Three had found something and were left alone. DQ-91 took
the marketplace one. This is the store one, and it is the last of the three nobody can
explain.

`tie_channel_daily_store_pos` holds the till against the order book, store by store, night
by night. It does not fail on a handful of nights. It fails on very nearly every store-day
we have both sides of, and it has done that since the day it was written.

The header on `agg_daily_store_sales` gives three reasons for the gap. Nobody has ever put
a number on any of them. Finance have to defend the flash's store sales figure at the
quarter review and "the model comment lists three things" is not a defence.

## What I want

Measure it. Put a figure in cents on every part of the gap, and show the parts adding back
up to the whole. Then say what a tie worth keeping would compare.

Write it in `RESPONSE.md` at the root of the working tree.

- **The gap.** One signed total in cents over the whole range — the till less the order
  book, signed the way the tie signs it — and the number of store-days it fails on.
- **The three reasons the header gives.** One line each, with the cents that reason puts
  into the total. Where a reason puts nothing in, say so, and say how you know.
- **Whatever is left.** Name each remaining cause and give the cents it puts in. Every
  cause gets its own figure; do not roll two of them into one line.
- **The arithmetic.** A reader adds your parts and lands on your total.
- **The honest tie.** What it would compare instead, and what it would still go red for.

Cents throughout, per `CONVENTIONS.md`. The whole range, not a sample week and not one
region — a slice of the estate is not the answer, and a percentage is not a figure in
cents.

## What does not move

Nothing under `dbt/` changes this week. The flash reads `agg_daily_store_sales` and finance
will not take a store sales number that moves in the middle of a quarter. The tie stays red
tonight and so does the nightly. This ticket is the write-up that lets somebody change it
in the next one.
