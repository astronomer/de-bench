select
    product_group,
    sum(revenue) as revenue,
    sum(revenue) * 0.28 as assumed_margin
from {{ ref('nwv_item_sales') }}
group by 1
