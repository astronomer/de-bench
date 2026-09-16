select
    item_code,
    item_desc,
    product_group,
    list_price
from {{ ref('stg_nwv_items') }}
