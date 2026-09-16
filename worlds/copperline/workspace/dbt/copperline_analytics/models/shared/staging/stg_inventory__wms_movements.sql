{{
    config(
        materialized='view'
    )
}}

{#- One row per warehouse movement.

    `qty` is signed upstream and stays signed. The shape of a movement says which
    location columns it carries: a pick has an origin and no destination, a
    receipt has a destination and often no origin, an adjustment has neither.
    None of those is a missing value.

    `reference_id` cites the document that caused the movement. The strings it
    holds do not join the order book — they were minted against a key range the
    OMS never used — so a model that joins on it gets nothing rather than
    something wrong.
-#}

select
    movement_id,
    sku,
    from_location,
    to_location,
    movement_type,
    reference_type,
    reference_id,
    operator_id,
    cast(qty as decimal(12, 3))         as qty,
    abs(cast(qty as decimal(12, 3)))    as qty_abs,
    occurred_at,
    cast(occurred_at as date)           as occurred_date,
    loaded_at
from {{ source('inventory', 'wms_movements') }}
