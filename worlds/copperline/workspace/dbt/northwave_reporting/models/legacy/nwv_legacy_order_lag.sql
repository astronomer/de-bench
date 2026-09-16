select
    o.order_id,
    o.order_date,
    i.invoice_date,
    i.invoice_date - o.order_date as days_to_invoice
from {{ ref('stg_nwv_orders') }} o
join {{ ref('stg_nwv_invoices') }} i on i.order_id = o.order_id
