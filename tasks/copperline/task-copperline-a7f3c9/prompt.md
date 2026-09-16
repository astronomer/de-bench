# INV-208 — the stock valuation writes nothing, and the run is green

Merchandising finance asked for `marts.fct_inventory_valuation` for April and
May, to put the retail-method figure beside the cost-method one for the FY2026
review. The table has no rows for any day in either month.

`sc_inventory_snapshot_daily` finishes green every night. The A-class file
loads, the position builds, the coverage check reports, the contract passes, and
the valuation step writes 0 rows without complaining. Nothing has failed,
nothing has retried and nothing has paged.

`marts.inventory_position` comes out of the same run, off the same snapshot
table, and it has every one of those days at full grain. So the counts are in
the warehouse.

Work out why the valuation writes nothing and fix it, so that a rebuild of a day
the snapshot holds a count for values that day.

Two rules the fix holds:

- The valuation itself is not the question. The grain, the columns, the boundary
  at the first day of FY2026 and the complement it applies are what
  `docs/inventory-policy.md` INV-1 and INV-3 make them, and none of them
  changes. What is missing is rows, not a method.
- No other day moves, and no other step in the run changes.

Finance wants the months they asked for, but the fix has to hold for any day,
not for April and May.
