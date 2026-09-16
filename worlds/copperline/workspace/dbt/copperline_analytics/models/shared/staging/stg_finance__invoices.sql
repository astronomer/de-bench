{{
    config(
        materialized='view'
    )
}}

{#- The trade invoice header.

    `customer_ref` may hold either id format, so it does not join the customer
    master on its own. int_customer_resolved is where the crosswalk is applied.

    `fx_rate_ppm` is the rate the ERP posted at, in parts per million. It is on
    the invoice, not looked up, because the ERP's rate is what the ledger holds
    and a rate looked up afterwards will not tie.
-#}

select
    invoice_id,
    invoice_number,
    customer_ref,
    nwv_account_id,
    order_id,
    entity_code,
    market_code,
    currency_code,
    payment_terms_code,
    billing_era,
    invoice_status,
    posted_period                       as fiscal_month,
    cast(fx_rate_ppm as bigint)         as fx_rate_ppm,
    cast(net_cents as bigint)           as net_cents,
    cast(tax_cents as bigint)           as tax_cents,
    cast(total_cents as bigint)         as total_cents,
    cast(settled_cents as bigint)       as settled_cents,
    cast(total_cents as bigint) - cast(settled_cents as bigint) as open_cents,
    invoice_date,
    service_start,
    service_end,
    due_date,
    loaded_at
from {{ source('finance', 'invoices') }}
