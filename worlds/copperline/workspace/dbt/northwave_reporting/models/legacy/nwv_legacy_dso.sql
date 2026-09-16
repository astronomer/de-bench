select
    date_trunc('month', invoice_date) as month_start,
    avg(due_date - invoice_date) as avg_terms_days,
    sum(total_amount - paid_amount) as open_amount
from {{ ref('stg_nwv_invoices') }}
group by 1
