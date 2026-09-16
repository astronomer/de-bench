select
    a.account_id,
    a.account_name,
    a.status,
    min(o.order_date) as first_order_date,
    max(o.order_date) as last_order_date,
    count(o.order_id) as order_count,
    sum(o.total_amount) as lifetime_value
from {{ ref('stg_nwv_accounts') }} a
left join {{ ref('stg_nwv_orders') }} o on o.account_id = a.account_id
group by 1, 2, 3
