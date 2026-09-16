select
    r.rep_id,
    r.rep_name,
    r.region,
    sum(a.lifetime_value) as book_value,
    sum(a.lifetime_value) * 0.015 as commission
from {{ ref('stg_nwv_reps') }} r
left join {{ ref('stg_nwv_accounts') }} acc on acc.owner_rep = r.rep_id
left join {{ ref('nwv_account_summary') }} a on a.account_id = acc.account_id
group by 1, 2, 3
