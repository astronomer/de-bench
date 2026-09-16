select
    o.order_id,
    o.account_id,
    o.order_date,
    o.branch_code,
    l.line_seq,
    l.item_code,
    l.qty,
    l.unit_price,
    l.extended_price
from {{ ref('stg_nwv_orders') }} o
join {{ ref('stg_nwv_order_lines') }} l on l.order_id = o.order_id
