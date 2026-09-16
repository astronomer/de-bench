{{
    config(
        materialized='view'
    )
}}

{#- One row per payment attempt.

    `order_ref` is the join back to the order and it is recycled — the OMS reuses
    a seven-digit reference about every fifty days. `intent_id` is the only
    unambiguous key here. Anything joining on `order_ref` alone is joining on a
    value that names three different orders over the life of the estate; see
    docs/runbooks/processor-migration.md.
-#}

select
    intent_id,
    order_ref,
    cast(order_id as bigint)            as processor_order_key,
    cast(attempt_no as integer)         as attempt_no,
    outcome,
    created_at,
    cast(created_at as date)            as created_date
from {{ source('payments', 'payment_intents') }}
