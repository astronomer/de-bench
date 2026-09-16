select
    order_date,
    branch_code,
    'ALL' as product_group,
    total_amount
from {{ ref('nwv_sales_daily') }}
