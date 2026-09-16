select
    order_date,
    sum(total_amount) as sales,
    sum(order_count) as orders
from {{ ref('nwv_sales_daily') }}
group by 1
