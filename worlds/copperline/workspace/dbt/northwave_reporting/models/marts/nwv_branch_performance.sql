select
    b.branch_code,
    b.branch_name,
    b.region,
    sum(d.total_amount) as total_amount,
    sum(d.order_count) as order_count
from {{ ref('stg_nwv_branches') }} b
left join {{ ref('nwv_sales_daily') }} d on d.branch_code = b.branch_code
group by 1, 2, 3
