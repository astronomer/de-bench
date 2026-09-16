{{ config(materialized='table') }}

{#-
    Sell-through by day, SKU and store. contracts/sell_through_daily.yml,
    consumer C-11 — Kestrel Outdoor's vendor share.

    The vendor share is found by a **name-pattern sensor**, not by a dependency:
    `sc_partner_share_kestrel` waits for a file called `sell_through_*_{week}.csv`.
    Rename the export and the sensor waits forever without failing, which is how
    Kestrel went three weeks without a file and nobody noticed until they rang.
    docs/lineage.md carries that row; `dbt ls` does not.

    Store grain, so the denominator is store stock rather than total stock.
    Where the SKU spellings do not cross, `stock_is_known` is false and the
    sell-through rate is NULL rather than 100 per cent.
-#}

with sales as (

    select
        l.order_date                            as ds,
        l.sku,
        l.store_id,
        sum(l.qty)                              as demand_units,
        sum(l.net_qty)                          as net_units,
        sum(l.net_sales_cents)                  as net_sales_cents,
        sum(l.returned_cents)                   as returned_cents,
        count(*)                                as line_count
    from {{ ref('int_net_sales_lines') }} l
    where l.store_id is not null
      and l.order_date > {{ ds_minus(120) }}
    group by 1, 2, 3

),

store_stock as (

    select
        p.ds,
        x.catalog_sku                           as sku,
        p.location_id                           as store_id,
        sum(p.on_hand_units)                    as on_hand_units
    from {{ ref('inventory_position') }} p
    join {{ ref('int_sku_crosswalk') }} x on x.merch_sku = p.sku
    where p.location_kind = 'store'
      and x.is_matched
    group by 1, 2, 3

)

select
    s.ds,
    s.sku,
    s.store_id,
    d.region_code                               as store_region,
    d.market_code                               as geography_key,
    d.store_format,

    cast(s.demand_units as integer)             as demand_units,
    cast(s.net_units as integer)                as net_units,
    s.net_sales_cents,
    s.returned_cents,
    s.line_count,

    k.on_hand_units,
    k.ds is not null                            as stock_is_known,

    case
        when k.on_hand_units is not null and (k.on_hand_units + s.demand_units) > 0
        then cast(round(
            cast(s.demand_units as decimal(38, 4)) * 10000 / (k.on_hand_units + s.demand_units), 0
        ) as integer)
    end                                         as sell_through_bps

from sales s
left join store_stock k on k.ds = s.ds and k.sku = s.sku and k.store_id = s.store_id
left join {{ ref('dim_store') }} d on d.store_id = s.store_id
