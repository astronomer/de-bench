{{
    config(
        materialized='view'
    )
}}

{#- Credits against an issued invoice.

    Signed negative here, because a credit memo reduces revenue and every reader
    of it wants to add it to something.
-#}

select
    credit_memo_id,
    invoice_id,
    invoice_line_id,
    reason_code,
    currency_code,
    applies_to_period                   as fiscal_month,
    approved_by,
    cast(amount_cents as bigint)        as amount_cents,
    -cast(amount_cents as bigint)       as signed_amount_cents,
    issued_on                           as issued_date,
    loaded_at
from {{ source('finance', 'credit_memos') }}
