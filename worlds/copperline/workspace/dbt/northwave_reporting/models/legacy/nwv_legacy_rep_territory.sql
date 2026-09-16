select
    r.rep_id,
    r.rep_name,
    r.region,
    count(a.account_id) as accounts
from {{ ref('stg_nwv_reps') }} r
left join {{ ref('stg_nwv_accounts') }} a on a.owner_rep = r.rep_id
group by 1, 2, 3
