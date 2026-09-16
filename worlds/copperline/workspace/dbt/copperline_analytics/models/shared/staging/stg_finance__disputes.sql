{{
    config(
        materialized='view'
    )
}}

{#- Invoice disputes.

    `settled_cents` is what was given up, not what was claimed. An open dispute
    has no settled amount and no `resolved_on`.
-#}

select
    dispute_id,
    invoice_id,
    dispute_status,
    reason_code,
    owner,
    cast(disputed_cents as bigint)      as disputed_cents,
    cast(settled_cents as bigint)       as settled_cents,
    raised_on                           as raised_date,
    resolved_on                         as resolved_date,
    dispute_status = 'open'             as is_open,
    case
        when resolved_on is not null then date_diff('day', raised_on, resolved_on)
    end                                 as days_to_resolve
from {{ source('finance', 'disputes') }}
