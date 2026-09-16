# Contract: merch dashboards

Schema: `contracts/order_economics.yml`. Consumer C-4 in `docs/report-registry.md`. Owner: commerce. Published by nothing — the BI tool reads the files under `dashboards/merch/`. Agreed 2024-03-11, last amended 2025-10-07.

Seven `.sql` files, each a literal query the BI tool runs on its own schedule. Merchandising reads them every day. There is no dbt reference and no Airflow task anywhere in the path, so this contract is the only record that these readers exist.

## MD-1 Order-level reconciliation

Two of the seven dashboards reconcile a total at order grain: they select `order_id` and a total that is only correct when one row means one order. `marts.order_economics` is an order-grain table and must stay one. A change that makes it one row per order line, or per anything else, breaks those two silently — the query still runs, the total is multiplied by the line count, and nobody is paged.

## MD-2 Columns the dashboards depend on

From `marts.order_economics`: `order_id`, `booked_cents`, `net_sales_cents`, `merch_margin_cents`, `channel`, `order_date`. From `marts.inventory_position`: `sku`, `location_id`, `demand_units`, `snapshot_date`. From `marts.sell_through_daily`: `sku`, `store_id`, `net_sales_cents`, `ds`.

Renaming or dropping any of these breaks a dashboard at query time, in the BI tool, in front of a merchandiser.

## MD-3 Changes need notice

A change to any of the three marts these dashboards read is agreed with commerce first, per `docs/change-management.md` §CM-2, and the seven files are updated in the same change. They are in the repository; there is no reason for them to lag.

## The seven files

| File | Reads |
|---|---|
| `margin_by_category.sql` | `marts.order_economics` |
| `order_mix.sql` | `marts.order_economics` |
| `sell_through.sql` | `marts.sell_through_daily` |
| `markdown_candidates.sql` | `marts.sell_through_daily` |
| `stock_cover.sql` | `marts.inventory_position` |
| `out_of_stock.sql` | `marts.inventory_position` |
| `category_mix.sql` | `marts.sell_through_daily` |

`margin_by_category.sql` and `order_mix.sql` are the two MD-1 is about.

## Notes

- The BI tool holds the schedule and the credentials, so a broken dashboard is discovered by a person, not by a run.
- One of the files still filters `where dbt_valid_to is null` against the product dimension, which is a habit from before the snapshot changed shape.

## Open items

- TODO: nobody has confirmed the seven are still seven. The tool can save a query without anyone committing it.
- The files have no tests, and adding some means running them somewhere. Nobody has picked where.
