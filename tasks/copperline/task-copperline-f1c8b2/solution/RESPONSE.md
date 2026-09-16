# PS-274 — why Kestrel have had nothing, and what still stands in the way

## Why no run ever reached the drop

`sc_partner_share_kestrel` waited on a file name. The step rendered
`include/data/marts/sell_through_*_<week>.csv` out of
`projects/supply/lib/paths.py` (`SELL_THROUGH_GLOB`) and matched it by pattern.

**Nothing in this repository writes a file of that name.** No DAG, no dbt model,
no script and no blueprint step produces `sell_through_*` under
`include/data/marts/`. The glob beside it, `SHIPPING_COST_GLOB`, does have a
producer — `sc_shipping_cost_daily.export_partitions` writes
`shipping_costs_<carrier>_<ds>.csv` — which is what makes the sell-through one
look like it has one too. `marts.sell_through_daily` is a dbt model built into
the warehouse by `plat_dbt_analytics_daily`. It has never been exported as
files, so the wait could not clear, and it did not fail either until its own
six-hour timeout ran out. Twenty-two Fridays, every one of them failing at
`wait_for_sell_through`, and the failure reads as "the export is late" — which
is why it went to the team that builds the mart twice and came back "the mart
is built". They were right both times.

The wait is now on the table the share is cut from:
`marts.sell_through_daily`, for the Saturday the delivered week closes on. The
week opens on the Sunday five days before the run's own date, so the Saturday is
the last day of it to land and the day that says the week is whole.

## What still stops the delivery

Past the wait, `build_share` cannot run, and it is not the wait's fault.

The share query filters `p.supplier_name = 'Kestrel Outdoor'` against
`marts.dim_product`. **`marts.dim_product` has no `supplier_name` column.** It
carries `supplier_id`, taken from the PIM snapshot
(`dbt/copperline_analytics/models/commerce/dim_product.sql`), and the ids are
strings like `SUP-0118`. So the statement dies on a binder error the first time
it is ever reached.

The name cannot be recovered anywhere else, either:

- there is no `raw.suppliers` table in the warehouse, and no supplier master
  feed lands — `models/shared/dims/dim_supplier.sql` says so in its own
  docstring, and that dimension is built entirely from PIM product versions;
- `marts.dim_supplier` therefore carries `supplier_id` and an assortment
  summary, and no supplier name at all;
- the SSIS and Pentaho jobs under `legacy/` do read a `supplier_name` out of the
  ERP, but that drop lands on the interface share and nothing loads it into a
  table anything here can query.

So there is no fact in this estate that says which SKUs are Kestrel's, and
picking a `supplier_id` that looks likely would send one supplier's sell-through
to another supplier. I have left the filter naming Kestrel Outdoor and have not
guessed at it. PS-2's rule — a conflict is raised, not decided alone — is about
redaction, but it is the same rule.

**Who settles it.** Supply own the Kestrel relationship, so the supplier scope
is theirs to state: either the vendor's own SKU list, or the `supplier_id` merch
holds against Kestrel in the PIM. Once that exists as data the query joins on
`supplier_id` and the delivery works. Until then the run reaches `build_share`
and fails loudly there, which is the right place for it to fail.

## What changed

- `projects/supply/dags/sc_partner_share_kestrel.py` — the wait is a
  `PythonSensor` on `marts.sell_through_daily`, through the house
  `table_has_partition`, for the week the run delivers. Same six-hour timeout,
  same reschedule mode, same page on failure.
- `projects/supply/lib/paths.py` — `SELL_THROUGH_GLOB` removed. It named a file
  nothing writes and nothing reads any more.
- `docs/report-registry.md` and `docs/lineage.md` — C-11's coupling is a
  warehouse wait, not a filename pattern, and C-11 is no longer one of the
  consumers a grep cannot find.
