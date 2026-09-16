select
    account_id,
    lifetime_value,
    case
        when lifetime_value > 500000 then lifetime_value * 0.03
        when lifetime_value > 100000 then lifetime_value * 0.02
        else 0
    end as rebate
from {{ ref('nwv_account_summary') }}
