{{
    config(
        materialized='view'
    )
}}

{#- One row per seller payout. -#}

select
    payout_id,
    seller_id,
    payout_date,
    payout_status,
    cast(principal_cents as bigint)     as principal_cents,
    cast(commission_cents as bigint)    as commission_cents,
    cast(fee_cents as bigint)           as fee_cents,
    cast(net_paid_cents as bigint)      as net_paid_cents
from {{ source('marketplace', 'marketplace_payouts') }}
