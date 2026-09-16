select
    l.item_code,
    l.unit_price,
    p.unit_price as book_price,
    l.unit_price - p.unit_price as variance
from {{ ref('stg_nwv_order_lines') }} l
left join {{ ref('stg_nwv_price_book') }} p on p.item_code = l.item_code
