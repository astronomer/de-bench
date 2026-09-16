{{
    config(
        materialized='view'
    )
}}

{#- The re-key crosswalk, legacy id to current id.

    Sixty rows have no current account. They are kept with a NULL
    `customer_id` and `is_orphan` set, because an orphan is a billing decision —
    the account stays on its legacy id — and not a row to throw away.
-#}

select
    legacy_id,
    customer_id,
    customer_id is null                 as is_orphan,
    note,
    migrated_at,
    cast(migrated_at as date)           as migrated_date
from {{ source('customer', 'customer_id_map') }}
