# OPS-412 — the channel split from 2 March

**Which channels moved.** `trade` and `store`, by the same amount and in
opposite directions. What moved between them is the counter takings of the 44
stores that came over with Northwave Supply. `fin_revenue_daily`'s build joins
the day's orders to `raw.stores` and counts any sale rung at a store with
`acquired_from = 'northwave'` as trade revenue. An acquired store sells over
the counter the way ours do, and `docs/semantic-definitions.md` fixes
`booked_cents` at order grain under the order's own channel: the store that
rang a sale says where it was rung, not what it was.

**Why nothing caught it.** The relabelling is one row in, one row out, so the
day's total does not move a cent. `tie_to_orders` ties, the cents stay
integers, all four channels still land a row, and every run went green. The
split is the only place it shows.

**Which published days are wrong.** Not the fortnight. The cause has been in
the code since 2 March and it is still in it, so every published day since
2026-03-02 carries the same error, up to the last one published. The fortnight
is the part somebody happened to open.

**What re-publishing those days would have given.** The same split. The figures
come out of the build and not out of the file, so replaying those days before
the fix republishes what is already there. The fix lands first; the days from
2 March forward are then replayed through `plat_backfill_broker`, which is the
sanctioned route and writes down what it replayed and why.

The build now takes the channel from the order and nothing else.
