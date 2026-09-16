{{
    config(
        materialized='view'
    )
}}

{#- The terms code table.

    `discount_pct_bps` is basis points, so 2% is 200. Nothing here is a
    percentage in a float column.
-#}

select
    payment_terms_code,
    description,
    cast(net_days as integer)           as net_days,
    cast(discount_pct_bps as integer)   as discount_pct_bps,
    cast(discount_days as integer)      as discount_days,
    discount_pct_bps > 0                as has_early_discount
from {{ source('reference', 'payment_terms') }}
