select
    line_id,
    order_id,
    line_seq,
    item_code,
    qty,
    unit_price,
    extended_price
from {{ source('nwv', 'order_lines') }}
