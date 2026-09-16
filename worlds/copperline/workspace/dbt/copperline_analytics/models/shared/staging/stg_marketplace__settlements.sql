{{
    config(
        materialized='view'
    )
}}

{#- Marketplace settlement lines.

    Three lines per order — principal, commission, fulfilment fee — and a
    fourth kind for refunds.

    **The feed signs its own lines and nothing here signs them again.** A
    principal line is negative: it is money leaving for the seller. Commission
    and fulfilment fee are positive: they are money Copperline keeps. A refund
    is negative. `magnitude_cents` is the amount without the sign, for the
    models that state the direction themselves.
-#}

select
    settlement_id,
    payout_id,
    marketplace_order_id,
    cast(order_id as bigint)            as marketplace_order_key,
    line_type,
    currency_code,
    cast(amount_cents as bigint)        as amount_cents,
    abs(cast(amount_cents as bigint))   as magnitude_cents,
    cast(amount_cents as bigint) < 0    as is_money_out,
    posted_at,
    payout_date,
    loaded_at
from {{ source('marketplace', 'marketplace_settlements') }}
