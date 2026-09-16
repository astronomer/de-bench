{{ config(materialized='table') }}

{#-
    List price against price paid, by day and product.

    The only pricing question no other team asks, so it stays in the mart rather
    than earning a place in `int`. Symmetry is not a reason to add a model.

    The lines come from int_net_sales_lines — the shared definition — and the
    list price from the conformed product dimension. Neither is commerce's mart:
    growth needs the same line-level net sales commerce needs, and rule 2 says
    the definition is shared rather than the table.

    `realization_bps` is what fraction of list the customer actually paid. Ten
    thousand is full price. Below that is discounting; above it is a data
    problem, and the two together are the point of the table.

    Lines with no catalogued product have no list price and are excluded from
    the rate but counted in `uncatalogued_lines`, because the order book carries
    a wider SKU range than the catalog does and pretending otherwise would make
    the rate look like it covered everything.
-#}

with lines as (

    select
        n.order_date,
        n.sku,
        n.channel,
        n.qty,
        n.gross_line_cents,
        n.total_discount_cents,
        n.discounted_line_cents,
        p.product_key,
        p.category_id,
        p.dept_code,
        p.list_price_cents,
        p.product_key is not null               as is_catalogued
    from {{ ref('int_net_sales_lines') }} n
    left join {{ ref('dim_product') }} p on p.sku = n.sku

)

select
    order_date                                  as date_key,
    order_date                                  as ds,
    sku,
    product_key,
    category_id,
    dept_code                                   as cost_center_key,
    channel                                     as channel_key,

    count(*)                                    as line_count,
    sum(qty)                                    as units,
    sum(gross_line_cents)                       as gross_cents,
    sum(total_discount_cents)                   as discount_cents,
    sum(discounted_line_cents)                  as paid_cents,

    max(list_price_cents)                       as list_price_cents,
    sum(case when is_catalogued then cast(round(qty * list_price_cents, 0) as bigint) end)
                                                as list_value_cents,

    count(*) filter (where not is_catalogued)   as uncatalogued_lines,

    cast(round(
        cast(sum(case when is_catalogued then discounted_line_cents end) as decimal(38, 4)) * 10000
        / nullif(sum(case when is_catalogued then cast(round(qty * list_price_cents, 0) as bigint) end), 0), 0
    ) as integer)                               as realization_bps

from lines
group by 1, 2, 3, 4, 5, 6, 7
