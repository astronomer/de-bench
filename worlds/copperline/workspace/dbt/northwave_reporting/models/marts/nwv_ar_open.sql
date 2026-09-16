select
    invoice_no,
    account_id,
    invoice_date,
    due_date,
    total_amount - paid_amount as open_amount
from {{ ref('stg_nwv_invoices') }}
where total_amount > paid_amount
