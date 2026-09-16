{{ config(materialized='view') }}

{#-
    Invoice lines with the dates recognition needs.

    Three dates matter and they are not the same date:

    * `invoice_date`   when it was billed.
    * `service_start` / `service_end`  the period the line covers.
    * `fiscal_month`   the period the header was posted into.

    A goods line is serviced on the day it is invoiced. A plan line is serviced
    over its term, which can be twelve months and can straddle three fiscal
    years. `service_days` is the length of the term and it is the denominator
    recognition divides by; a line with no term has one day.
-#}

with lines as (

    select * from {{ ref('stg_finance__invoice_lines') }}

),

headers as (

    select
        invoice_id,
        customer_ref,
        nwv_account_id,
        entity_code,
        market_code,
        currency_code,
        fx_rate_ppm,
        billing_era,
        invoice_status,
        fiscal_month,
        invoice_date,
        due_date,
        payment_terms_code
    from {{ ref('stg_finance__invoices') }}

),

plans as (

    select plan_line_id, plan_type, billing_frequency, term_start, term_end, is_cancelled, cancelled_on
    from {{ ref('stg_finance__plan_lines') }}

)

select
    l.invoice_line_key,
    l.invoice_line_id,
    l.invoice_id,
    l.line_no,
    l.line_kind,
    l.sku,
    l.plan_line_id,
    l.revenue_account_code                      as account_key,
    l.revenue_account_code,

    h.customer_ref,
    h.nwv_account_id,
    h.entity_code,
    h.market_code                               as geography_key,
    h.currency_code                             as currency_key,
    h.currency_code,
    h.fx_rate_ppm,
    h.billing_era,
    h.invoice_status,
    h.fiscal_month,
    h.invoice_date                              as date_key,
    h.invoice_date,
    h.due_date,
    h.payment_terms_code,

    l.qty,
    l.unit_price_cents,
    l.line_discount_cents,
    l.tax_cents,
    l.line_total_cents,

    coalesce(l.service_start, p.term_start, h.invoice_date) as service_start,
    coalesce(l.service_end, p.term_end, h.invoice_date)     as service_end,
    greatest(coalesce(l.service_days, p.term_end - p.term_start + 1, 1), 1) as service_days,

    p.plan_type,
    p.billing_frequency,
    p.is_cancelled                              as plan_is_cancelled,
    p.cancelled_on                              as plan_cancelled_on,

    l.line_kind = 'plan'                        as is_over_time,
    h.billing_era = 'legacy'                    as is_legacy_era

from lines l
join headers h on h.invoice_id = l.invoice_id
left join plans p on p.plan_line_id = l.plan_line_id
