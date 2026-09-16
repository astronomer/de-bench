select
    payment_id,
    invoice_no,
    payment_date,
    payment_amount,
    payment_method
from {{ source('nwv', 'payments') }}
