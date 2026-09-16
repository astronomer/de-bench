select
    item_code,
    item_desc,
    product_group,
    uom,
    list_price,
    active_flag
from {{ source('nwv', 'items') }}
