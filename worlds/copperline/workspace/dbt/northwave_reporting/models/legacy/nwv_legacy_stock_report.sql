select
    item_code,
    item_desc,
    product_group,
    active_flag
from {{ ref('stg_nwv_items') }}
