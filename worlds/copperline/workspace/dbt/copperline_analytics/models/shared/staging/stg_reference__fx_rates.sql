{{
    config(
        materialized='view'
    )
}}

{#- One rate per currency per day, in parts per million against USD.

    Integer parts per million, never a float. A USD row is minted here at
    1,000,000 so that every converting model can join a rate rather than special-
    case the base currency.
-#}

select
    rate_date,
    currency_code,
    cast(rate_to_usd_ppm as bigint)     as fx_rate_ppm,
    source,
    published_at
from {{ source('reference', 'fx_rates') }}

union all

select distinct
    rate_date,
    'USD'                               as currency_code,
    cast(1000000 as bigint)             as fx_rate_ppm,
    'base'                              as source,
    published_at
from {{ source('reference', 'fx_rates') }}
