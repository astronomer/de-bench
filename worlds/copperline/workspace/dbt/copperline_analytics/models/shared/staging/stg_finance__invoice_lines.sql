{{
    config(
        materialized='view'
    )
}}

{#- Invoice lines.

    `invoice_line_id` upstream is a line ordinal — L-01, L-02 — the same shape
    the order lines use, and it repeats in every invoice. The key of this table
    is (invoice_id, line_no), spelled once as `invoice_line_key`.

    Three kinds: goods, plan and freight. There is no adjustment kind — an
    adjustment to an issued invoice is a credit memo and lands in
    stg_finance__credit_memos.

    `line_total_cents` is tax-exclusive. Tax is beside it, the same shape as the
    order lines.
-#}

select
    {{ dbt_utils.surrogate_key(['invoice_id', 'line_no']) }} as invoice_line_key,
    invoice_line_id,
    invoice_id,
    cast(line_no as integer)                as line_no,
    line_kind,
    sku,
    plan_line_id,
    revenue_account_code,
    cast(qty as decimal(12, 3))             as qty,
    cast(unit_price_cents as bigint)        as unit_price_cents,
    cast(line_discount_cents as bigint)     as line_discount_cents,
    cast(tax_cents as bigint)               as tax_cents,
    cast(line_total_cents as bigint)        as line_total_cents,
    service_start,
    service_end,
    case
        when service_start is not null and service_end is not null
        then date_diff('day', service_start, service_end) + 1
    end                                     as service_days
from {{ source('finance', 'invoice_lines') }}
