{{ config(materialized='table') }}

{#-
    Receivables by aging band, as of `var('ds')`.

    Bands are counted from the due date, not the invoice date. An invoice on
    ninety-day terms is not sixty days overdue on its sixtieth day, and every
    version of this table that aged from the invoice date reported the trade
    book as three months behind when it was current.
-#}

with open_invoices as (

    select *
    from {{ ref('fct_ar_invoices') }}
    where is_open

)

select
    {{ ds() }}                                  as ds,
    entity_code                                 as entity,
    geography_key,
    currency_key                                as currency_code,
    billing_era,

    case
        when days_past_due <= 0     then 'current'
        when days_past_due <= 30    then '1-30'
        when days_past_due <= 60    then '31-60'
        when days_past_due <= 90    then '61-90'
        else '90+'
    end                                         as aging_band,

    case
        when days_past_due <= 0     then 0
        when days_past_due <= 30    then 1
        when days_past_due <= 60    then 2
        when days_past_due <= 90    then 3
        else 4
    end                                         as aging_band_order,

    count(*)                                    as invoice_count,
    count(distinct customer_ref)                as account_count,
    sum(open_cents)                             as open_cents,
    sum(header_total_cents)                     as invoiced_cents,
    sum(settled_cents)                          as settled_cents,
    sum(disputed_cents)                         as disputed_cents,
    count(*) filter (where open_dispute_count > 0) as disputed_invoice_count,
    count(*) filter (where is_northwave_book)   as northwave_invoice_count,
    max(days_past_due)                          as worst_days_past_due

from open_invoices
group by 1, 2, 3, 4, 5, 6, 7
