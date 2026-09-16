{{ config(materialized='table') }}

{#-
    Lines ordered against stock that was not there, by day.

    Reads marts.inventory_position, which this team owns — an aggregate reading
    its own team's base table, rule 3.

    An order line is treated as backordered when its SKU had no available stock
    anywhere on the order date. That is a coarse test and it is the only one
    available: there is no allocation feed, so nothing records which location a
    line was meant to ship from. `is_estimated` is true on every row and says so.
-#}

with stock_by_day as (

    select
        p.ds,
        x.catalog_sku                           as sku,
        sum(p.available_units)                  as available_units
    from {{ ref('inventory_position') }} p
    join {{ ref('int_sku_crosswalk') }} x on x.merch_sku = p.sku
    where x.is_matched
    group by 1, 2

),

lines as (

    select
        n.order_date                            as ds,
        n.sku,
        p.dept_code                             as cost_center_key,
        count(*)                                as line_count,
        sum(n.qty)                              as ordered_units,
        sum(n.net_sales_cents)                  as net_sales_cents
    from {{ ref('int_net_sales_lines') }} n
    left join {{ ref('dim_product') }} p on p.sku = n.sku
    where n.order_date > {{ ds_minus(120) }}
    group by 1, 2, 3

)

select
    l.ds                                        as date_key,
    l.ds,
    l.sku,
    l.cost_center_key,

    l.line_count,
    l.ordered_units,
    l.net_sales_cents,
    coalesce(s.available_units, 0)              as available_units,

    s.ds is null                                as stock_is_unknown,
    coalesce(s.available_units, 0) <= 0 and s.ds is not null as is_backordered,
    true                                        as is_estimated,

    case
        when s.ds is not null and s.available_units < l.ordered_units
        then cast(l.ordered_units - greatest(s.available_units, 0) as integer)
    end                                         as short_units

from lines l
left join stock_by_day s on s.ds = l.ds and s.sku = l.sku
