{{ config(materialized='table') }}

{#-
    One row per invoice line, with its recognition schedule rolled up.
-#}

with lines as (

    select * from {{ ref('int_invoice_line_dated') }}

),

recognition as (

    select
        invoice_line_key,
        count(*)                                        as recognition_months,
        min(fiscal_month)                               as first_recognized_month,
        max(fiscal_month)                               as last_recognized_month,
        sum(recognized_cents)                           as scheduled_cents
    from {{ ref('int_recognition_schedule') }}
    group by 1

),

credits as (

    select
        invoice_id,
        sum(amount_cents)                       as credited_cents,
        count(*)                                as credit_memo_count
    from {{ ref('stg_finance__credit_memos') }}
    group by 1

)

select
    l.invoice_line_key,
    l.invoice_id,
    l.invoice_line_id,
    l.line_no,
    l.line_kind,
    l.sku,
    p.product_key,
    l.plan_line_id,
    l.plan_type,
    l.billing_frequency,

    l.date_key,
    l.invoice_date,
    l.due_date,
    l.fiscal_month,
    l.service_start,
    l.service_end,
    l.service_days,

    l.customer_ref,
    l.nwv_account_id,
    l.entity_code,
    l.geography_key,
    l.currency_key,
    l.currency_code,
    l.account_key,
    l.billing_era,
    l.invoice_status,
    l.payment_terms_code,

    l.qty,
    l.unit_price_cents,
    l.line_discount_cents,
    l.tax_cents,
    l.line_total_cents,
    l.fx_rate_ppm,
    l.invoice_date                              as fx_rate_date,
    {{ to_base_cents('l.line_total_cents', 'l.fx_rate_ppm') }} as line_total_base_cents,

    coalesce(r.recognition_months, 0)           as recognition_months,
    r.first_recognized_month,
    r.last_recognized_month,
    coalesce(r.scheduled_cents, 0)              as scheduled_cents,

    coalesce(c.credit_memo_count, 0)            as credit_memo_count,
    l.is_over_time,
    l.is_legacy_era,
    l.plan_is_cancelled

from lines l
left join recognition r on r.invoice_line_key = l.invoice_line_key
left join credits c on c.invoice_id = l.invoice_id
left join {{ ref('dim_product') }} p on p.sku = l.sku
