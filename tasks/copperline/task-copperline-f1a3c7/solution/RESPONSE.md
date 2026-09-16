# INV-317 — what happened to the shrink pack

## The pack now exists

`marts.shrink_weekly` had no table behind it anywhere in the tree, so
`build_shrink` died on its `DELETE` and the job had never written a row. The
DAG now creates the table before it builds the week.

## Why every FY2026 week priced at zero

Copperline changed how it values stock on **2026-02-01**, the first day of
FY2026. Before that date stock was held at **weighted-average cost** and
`raw.inventory_snapshots.unit_cost_cents` carried it per unit. From that date
the retail inventory method applies: `unit_cost_cents` stops arriving, and
`retail_value_cents` and `cost_complement_bps` start.
`docs/inventory-policy.md` INV-1 is the only place that is written down; the
data announces it and explains nothing.

The build multiplied the units counted by `coalesce(unit_cost_cents, 0)`. That
was right until 2026-01-31 and has been exactly zero every week since.

The build now reads which method the position states, the way
`fct_inventory_valuation` does: a row with a unit cost prices at that cost, and
a row with a retail value and a complement prices by the retail method. The
department's retail is summed, the complement is applied once, and the rounding
is the single half-up on that division — INV-2 puts the cost at department
grain and INV-3 puts the rounding there too.

FY2025 keeps its own basis. INV-4 says those periods are not restated, and the
data could not restate them anyway: a FY2025 position carries no retail value
and no complement.

So the two halves of the pack do not answer the same question. FY2025 is stock
at what it cost us. FY2026 is stock at retail, taken back to cost by a
department ratio. The retail method reads six to nine per cent higher on the
same stock, so a year-on-year comparison across 2026-02-01 carries that much
before any stock moves. Say so on the page.

## Why an ordinary day's count priced at nothing

The position is not taken every day. The network snapshot is weekly and the
A-class one daily, so a count taken on a Tuesday had no snapshot row on that
Tuesday. The build joined on the snapshot date itself, so those counts fell out
of the join, arrived under a row with no department, and were worth nothing.
Over the whole feed that is 9,423 of 59,712 counts priced and the rest lost.

A count is now priced against the position in force when it was taken — the
last snapshot on or before the day, per SKU and site, carried forward. That is
the rule `int_inventory_position_daily` already states for everything else that
reads a position. All 59,712 counts now find one.

## Which weeks were wrong

Both faults are in the code, not in one week's data, so replaying a week
without the fix reproduces the same numbers.

- **Every fiscal week from 2026-02-01 onwards** priced at zero. That is every
  FY2026 week to date, not only the weeks in the request.
- **Every week before that** priced about one count in seven and lost the rest,
  back to the start of the feed. The January pack loss prevention are holding
  is low for the same reason, so it should be rebuilt before they compare
  anything to it.

Rebuild the FY2025 weeks as well as the FY2026 ones, or the comparison is
between a full number and a partial one.
