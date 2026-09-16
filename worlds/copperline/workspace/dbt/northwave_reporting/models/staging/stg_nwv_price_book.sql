select
    item_code,
    price_tier,
    effective_from,
    effective_to,
    unit_price
from {{ source('nwv', 'price_book') }}
