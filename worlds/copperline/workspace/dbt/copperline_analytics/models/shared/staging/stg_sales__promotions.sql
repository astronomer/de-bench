{{
    config(
        materialized='view'
    )
}}

{#- The promotion master.

    `market_codes` lands as a comma-separated list. It is split into a list here
    so that nothing downstream has to parse a string, and the raw string is kept
    beside it for the readers that still match on it.
-#}

select
    promo_id,
    promo_code,
    promo_type,
    applies_to,
    cast(value_bps as integer)              as value_bps,
    cast(value_cents as bigint)             as value_cents,
    cast(min_order_cents as bigint)         as min_order_cents,
    stackable,
    cast(stack_priority as integer)         as stack_priority,
    funded_by,
    market_codes,
    string_split(market_codes, ',')         as market_code_list,
    starts_at,
    ends_at,
    cast(starts_at as date)                 as starts_on,
    cast(ends_at as date)                   as ends_on
from {{ source('sales', 'promotions') }}
