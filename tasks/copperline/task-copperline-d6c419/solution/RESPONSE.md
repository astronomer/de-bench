# MER-618 — the four answers for the MER-612 review

**1. How much of the order book the catalogue can speak for.** The order book
uses **23,147** distinct SKUs. Palette's change feed carries **1,860**. **1,724**
of those are in both, so the catalogue can speak for about 7 per cent of the SKUs
we sell and the product join misses on the rest. Every line still counts and
every order still keeps its row; `category_count` is nought on an order whose
lines all miss, and that is the coverage, not a fault.

**2. Would `check_grain` have caught a `join_product` that dropped those
orders?** No. `enrich_publish` left-joins `staging.enrich_product` onto
`staging.enrich_base`, so an order with no row in the product table still lands
on the spine, carrying nulls in the three product columns.
`enrich_grain_check.sql` counts the day's orders in `raw.orders` against the
day's rows in `staging.orders_enriched` and looks for an order that came out
twice; both sides still agree and no order repeats, so the step goes green. The
same left join is why the columns being null for months never went red either.

**3. What `check_snapshot_grain` returns the first night MER-612's history
lands.** `snapshot_open_rows.sql` comes back with **0** and **1,718**: no SKU
carries more than one open row, and 1,718 SKUs have an open row. Nought is the
number that matters — the step raises on the first figure and returns the
second.

**4. How many SKUs the feed has retired and never brought back.** **142**. Their
newest change is a `delete`, which closes the span in force and opens nothing, so
they have no open row and an as-of read of any later date returns nothing for
them. 1,860 SKUs less those 142 is the 1,718 above.
