select
    invoice_no,
    account_id,
    order_id,
    invoice_date,
    due_date,
    net_amount,
    tax_amount,
    total_amount,
    paid_amount,
    invoice_status
from {{ source('nwv', 'invoices') }}
