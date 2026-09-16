{{
    config(
        materialized='view'
    )
}}

{#- The store estate, SCD2.

    Every version row is kept. `is_current` marks the live one and dim_store is
    where the choice between them is made. A model that wants "the store as it is
    now" reads dim_store, not this.
-#}

select
    store_id,
    store_name,
    store_format,
    region_code,
    market_code,
    tz_name,
    status,
    acquired_from,
    coalesce(acquired_from, 'copperline')   as source_book,
    opened_on,
    closed_on,
    valid_from,
    coalesce(valid_to, date '9999-12-31')   as valid_to,
    coalesce(is_current, false)             as is_current
from {{ source('store', 'stores') }}
