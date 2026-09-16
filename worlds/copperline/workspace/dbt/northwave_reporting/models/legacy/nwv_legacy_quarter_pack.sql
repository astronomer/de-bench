select
    date_trunc('quarter', month_start) as quarter_start,
    sum(sales) as sales,
    sum(orders) as orders
from {{ ref('nwv_legacy_month_close') }}
group by 1
