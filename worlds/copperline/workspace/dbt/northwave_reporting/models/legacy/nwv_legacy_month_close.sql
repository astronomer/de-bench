select
    month_start,
    sum(total_amount) as sales,
    sum(order_count) as orders
from {{ ref('nwv_sales_monthly') }}
group by 1
