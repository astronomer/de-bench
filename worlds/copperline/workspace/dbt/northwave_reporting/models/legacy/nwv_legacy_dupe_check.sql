select
    tax_id,
    count(*) as account_count
from {{ ref('stg_nwv_accounts') }}
where tax_id is not null
group by 1
having count(*) > 1
