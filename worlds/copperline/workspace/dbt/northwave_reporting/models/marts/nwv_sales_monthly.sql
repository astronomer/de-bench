select
    date_trunc('month', order_date) as month_start,
    branch_code,
    sum(order_count) as order_count,
    sum(total_amount) as total_amount
from {{ ref('nwv_sales_daily') }}
group by 1, 2
