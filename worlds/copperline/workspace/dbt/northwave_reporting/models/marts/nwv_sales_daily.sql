select
    o.order_date,
    o.branch_code,
    count(distinct o.order_id) as order_count,
    sum(o.subtotal) as subtotal,
    sum(o.tax_amount) as tax_amount,
    sum(o.total_amount) as total_amount
from {{ ref('stg_nwv_orders') }} o
group by 1, 2
