select
    order_id,
    account_id,
    order_date,
    branch_code,
    order_status,
    subtotal,
    tax_amount,
    total_amount,
    currency
from {{ source('nwv', 'orders') }}
where order_status != 'void'
