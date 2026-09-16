select
    a.account_id,
    a.account_name,
    a.billing_city,
    a.billing_state,
    a.tax_id,
    s.lifetime_value
from {{ ref('stg_nwv_accounts') }} a
left join {{ ref('nwv_account_summary') }} s on s.account_id = a.account_id
