# CUS-509 — the feature file for a day is not the file we wrote on that day

The modelling team retrained Compass and it has been rolled back. It scored
better offline than any model they have had and worse live than the one it
replaced.

They built the training set by re-running `cus_features_daily` back over the
history rather than by using the parquet files they had kept, because the kept
ones have gaps. Afterwards they took one day they still had the original file
for and ran that day again. The two files disagree.

Same day, same accounts, same grain. `orders_30d` and `booked_cents_30d` are
identical to the unit on every row. `net_sales_cents_30d` is lower in the new
file for a few dozen accounts, and lower every time — never higher, on any day
they have tried.

Nothing failed, and nothing has ever failed here.
`tie_feature_table_has_no_future` is green, the grain test is green, and
`check_contract` passes.

`contracts/feature-store.md` §FS-1 is the clause this is about. The table is
`marts.feature_customer_daily` and the model that builds it is
`dbt/copperline_analytics/models/customer/feature_customer_daily.sql`.

## What to do

Work out why running a day again changes it, and make the money on a row the
money that was true on that row's own date — so that a day built today and the
same day built on the day itself come out the same figure.

Four things hold.

- **No window moves.** Every column covers the span it covers today. The only
  thing that changes is which events are inside a row.
- **The grain does not move.** One row per account per day, per FS-2.
- **`orders_30d` and `booked_cents_30d` do not move.** They are already right.
- **The id shapes are not yours, and neither is the acquired book.** CUS-233
  owns the pre-cutover account ids and the Northwave accounts are a separate
  ticket. The join from an order to an account stays as it is.

`docs/semantic-definitions.md` SD-2 says a consumer that wants something other
than a reserved name's definition defines it in its own contract. C-9's does.

## Then write the modelling team three lines

`RESPONSE.md` at the root of the working tree. It goes on the thread, not into a
document.

- what a row was carrying that had not happened yet on that row's date
- which other column on the table carries the same thing, and which columns do
  not
- what the files the modelling team kept are worth against the ones the retrain
  rebuilt, and whether rebuilding a day helps
