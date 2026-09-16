select
    i.invoice_no,
    i.order_id,
    i.total_amount as invoice_total,
    o.total_amount as order_total,
    i.total_amount - o.total_amount as variance
from {{ ref('stg_nwv_invoices') }} i
left join {{ ref('stg_nwv_orders') }} o on o.order_id = i.order_id
