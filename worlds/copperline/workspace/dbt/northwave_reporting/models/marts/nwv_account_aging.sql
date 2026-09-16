select
    i.account_id,
    case
        when current_date - i.due_date <= 0 then 'current'
        when current_date - i.due_date <= 30 then '1-30'
        when current_date - i.due_date <= 60 then '31-60'
        else '60+'
    end as bucket,
    sum(i.total_amount - i.paid_amount) as open_amount
from {{ ref('stg_nwv_invoices') }} i
where i.total_amount > i.paid_amount
group by 1, 2
