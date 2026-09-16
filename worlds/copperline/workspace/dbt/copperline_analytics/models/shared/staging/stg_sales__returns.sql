{{
    config(
        materialized='view'
    )
}}

{#- Return authorisations.

    `order_line_id` is the line ordinal, so the join back to the line is on the
    pair. `received_at` can sit past today for a return in flight; nothing here
    clips it, because a return that has not arrived is still a return and the
    marts decide whether it counts yet.
-#}

select
    rma_id,
    order_id,
    order_line_id,
    {{ dbt_utils.surrogate_key(['order_id', 'order_line_id']) }} as order_line_ordinal_key,
    sku,
    cast(qty as decimal(12, 3))         as qty,
    return_reason,
    return_channel,
    disposition,
    refund_method,
    cast(refund_cents as bigint)        as refund_cents,
    restock_flag,
    initiated_at,
    received_at,
    cast(initiated_at as date)          as initiated_date,
    cast(received_at as date)           as received_date
from {{ source('sales', 'returns') }}
