{{ config(materialized='table') }}

{#-
    One row per trade invoice.

    `header_line_variance_cents` is the load-bearing column. The invoice header
    carries its own net and tax and they do not agree with the lines: the ERP
    splits the header at a different point than the lines do. The postings in
    int_gl_postings_unified are built from the **lines**, because a receivable
    that does not equal the revenue it created is a plug rather than a posting.
    This column is the size of the disagreement, per invoice, and it is the
    number to work if anybody wants to close it.
-#}

with invoices as (

    select * from {{ ref('stg_finance__invoices') }}

),

line_rollup as (

    select
        invoice_id,
        count(*)                                            as line_count,
        sum(line_total_cents)                               as line_net_cents,
        sum(tax_cents)                                      as line_tax_cents,
        count(*) filter (where line_kind = 'goods')         as goods_lines,
        count(*) filter (where line_kind = 'plan')          as plan_lines,
        count(*) filter (where line_kind = 'freight')       as freight_lines
    from {{ ref('stg_finance__invoice_lines') }}
    group by 1

),

credits as (

    select invoice_id, sum(amount_cents) as credited_cents, count(*) as credit_memo_count
    from {{ ref('stg_finance__credit_memos') }}
    group by 1

),

disputes as (

    select
        invoice_id,
        count(*)                                as dispute_count,
        count(*) filter (where is_open)         as open_dispute_count,
        sum(disputed_cents)                     as disputed_cents,
        sum(settled_cents)                      as dispute_settled_cents
    from {{ ref('stg_finance__disputes') }}
    group by 1

)

select
    i.invoice_id                                as invoice_key,
    i.invoice_id,
    i.invoice_number,
    i.invoice_date                              as date_key,
    i.invoice_date,
    i.due_date,
    i.fiscal_month,

    i.customer_ref,
    i.nwv_account_id,
    i.order_id,
    i.entity_code,
    i.market_code                               as geography_key,
    i.currency_code                             as currency_key,
    i.currency_code,
    i.payment_terms_code,
    i.billing_era,
    i.invoice_status,

    i.net_cents                                 as header_net_cents,
    i.tax_cents                                 as header_tax_cents,
    i.total_cents                               as header_total_cents,
    i.settled_cents,
    i.open_cents,

    coalesce(l.line_count, 0)                   as line_count,
    coalesce(l.line_net_cents, 0)               as line_net_cents,
    coalesce(l.line_tax_cents, 0)               as line_tax_cents,
    coalesce(l.goods_lines, 0)                  as goods_lines,
    coalesce(l.plan_lines, 0)                   as plan_lines,
    coalesce(l.freight_lines, 0)                as freight_lines,

    i.total_cents - (coalesce(l.line_net_cents, 0) + coalesce(l.line_tax_cents, 0))
                                                as header_line_variance_cents,

    i.fx_rate_ppm,
    i.invoice_date                              as fx_rate_date,
    {{ to_base_cents('i.total_cents', 'i.fx_rate_ppm') }} as total_base_cents,

    coalesce(c.credited_cents, 0)               as credited_cents,
    coalesce(c.credit_memo_count, 0)            as credit_memo_count,
    coalesce(d.dispute_count, 0)                as dispute_count,
    coalesce(d.open_dispute_count, 0)           as open_dispute_count,
    coalesce(d.disputed_cents, 0)               as disputed_cents,
    coalesce(d.dispute_settled_cents, 0)        as dispute_settled_cents,

    date_diff('day', i.due_date, {{ ds() }})    as days_past_due,
    i.open_cents > 0                            as is_open,
    i.nwv_account_id is not null                as is_northwave_book

from invoices i
left join line_rollup l on l.invoice_id = i.invoice_id
left join credits c on c.invoice_id = i.invoice_id
left join disputes d on d.invoice_id = i.invoice_id
