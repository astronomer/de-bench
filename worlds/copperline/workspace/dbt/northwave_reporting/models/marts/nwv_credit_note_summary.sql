select
    date_trunc('month', c.issued_on) as month_start,
    c.reason,
    count(*) as credit_count,
    sum(c.credit_amount) as credit_amount
from {{ ref('stg_nwv_credit_notes') }} c
group by 1, 2
