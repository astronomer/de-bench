select
    credit_no,
    invoice_no,
    issued_on,
    credit_amount,
    reason
from {{ source('nwv', 'credit_notes') }}
