select
    month_start,
    branch_code,
    total_amount,
    lag(total_amount, 12) over (partition by branch_code order by month_start) as ly_amount
from {{ ref('nwv_sales_monthly') }}
