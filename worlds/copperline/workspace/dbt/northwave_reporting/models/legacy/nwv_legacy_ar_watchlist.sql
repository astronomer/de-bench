select
    account_id,
    sum(open_amount) as open_amount
from {{ ref('nwv_ar_open') }}
group by 1
having sum(open_amount) > 25000
