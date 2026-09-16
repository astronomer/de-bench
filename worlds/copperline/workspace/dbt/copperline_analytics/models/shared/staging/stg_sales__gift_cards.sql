{{
    config(
        materialized='view'
    )
}}

{#- The gift-card master.

    `jurisdiction_code` is <market>-<nn> and drives the breakage rule. It is
    kept as it arrived; the split into market and jurisdiction number is done
    here so no mart has to slice a string.
-#}

select
    card_id,
    issued_order_id,
    cast(initial_cents as bigint)               as initial_cents,
    currency_code,
    market_code,
    jurisdiction_code,
    split_part(jurisdiction_code, '-', 1)       as jurisdiction_market,
    split_part(jurisdiction_code, '-', 2)       as jurisdiction_no,
    status,
    issued_at,
    expires_at,
    cast(issued_at as date)                     as issued_date,
    cast(expires_at as date)                    as expires_date
from {{ source('sales', 'gift_cards') }}
