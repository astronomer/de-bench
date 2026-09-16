{{
    config(
        materialized='view'
    )
}}

{#- Which region each Halcyon merchant account belongs to.

    Six rows. It is the only way to recover currency on a Halcyon settlement,
    which is why a table this small has a staging model of its own.
-#}

select
    merchant_acct,
    region_code,
    valid_from,
    coalesce(valid_to, date '9999-12-31')   as valid_to,
    valid_to is null                        as is_current
from {{ source('payments', 'merchant_regions') }}
