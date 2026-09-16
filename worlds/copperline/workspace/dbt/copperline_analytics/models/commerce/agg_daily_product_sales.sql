{{ config(materialized='table') }}

{#-
    Units and money by day and SKU.

    Reads this team's own base fact, which is the one exception to the rule that
    a mart does not read another mart — projects/platform/README.md, rule 3. It
    does not extend across a team boundary.
-#}

select
    l.order_date                                as date_key,
    l.order_date                                as ds,
    l.sku,
    l.product_key,
    l.category_id,
    l.cost_center_key,
    l.product_brand,

    count(distinct l.order_id)                  as order_count,
    count(*)                                    as line_count,
    sum(l.qty)                                  as ordered_units,
    sum(l.returned_qty)                         as returned_units,
    sum(l.net_qty)                              as net_units,

    sum(l.gross_line_cents)                     as gross_sales_cents,
    sum(l.total_discount_cents)                 as discount_cents,
    sum(l.returned_cents)                       as returned_cents,
    sum(l.net_sales_cents)                      as net_sales_cents,
    sum(l.extended_cost_cents)                  as cost_cents,
    sum(l.merch_margin_cents)                   as merch_margin_cents,

    count(*) filter (where not l.cost_is_stated) as lines_without_cost,
    bool_or(l.is_catalogued)                     as is_catalogued

from {{ ref('fct_order_line') }} l
group by 1, 2, 3, 4, 5, 6, 7
