select
    l.item_code,
    i.item_desc,
    i.product_group,
    sum(l.qty) as qty_sold,
    sum(l.extended_price) as revenue
from {{ ref('stg_nwv_order_lines') }} l
left join {{ ref('stg_nwv_items') }} i on i.item_code = l.item_code
group by 1, 2, 3
