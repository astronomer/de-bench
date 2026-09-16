{{
    config(
        materialized='view'
    )
}}

{#- One invoice per carrier per month. The freight accrual ties to this. -#}

select
    carrier_invoice_id,
    carrier_code,
    invoice_date,
    period_start,
    period_end,
    status,
    cast(total_cents as bigint)         as total_cents
from {{ source('logistics', 'carrier_invoices') }}
