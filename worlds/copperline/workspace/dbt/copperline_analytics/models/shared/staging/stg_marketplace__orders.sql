{{
    config(
        materialized='view'
    )
}}

{#- Marketplace orders.

    `order_id` on this feed is the marketplace's own integer key. It is not the
    OMS order id and it does not join raw.orders — it is renamed
    `marketplace_order_key` here so that nobody joins it by accident.

    `gmv_cents` is the full order value, sellers included. That is the reserved
    meaning of the name — docs/semantic-definitions.md.
-#}

select
    marketplace_order_id,
    cast(order_id as bigint)                as marketplace_order_key,
    seller_id,
    currency_code,
    order_state,
    cast(gmv_cents as bigint)               as gmv_cents,
    cast(commission_cents as bigint)        as commission_cents,
    cast(fulfilment_fee_cents as bigint)    as fulfilment_fee_cents,
    cast(commission_rate_bps as integer)    as commission_rate_bps,
    placed_at,
    ship_confirmed_at,
    cast(placed_at as date)                 as placed_date,
    loaded_at
from {{ source('marketplace', 'marketplace_orders') }}
