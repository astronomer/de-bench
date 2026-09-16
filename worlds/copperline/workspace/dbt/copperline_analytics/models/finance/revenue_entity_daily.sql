{{ config(materialized='table') }}

{#-
    Revenue by day and entity, from the postings.

    **Finance does not rebuild revenue from source.** This model reads
    int_gl_postings_unified, which is the same model commerce's reconciliation
    reads, so the P&L and the sales dashboard cannot disagree about what a sale
    was. That is the load-bearing rule of the whole architecture: not one right
    number and one wrong one, but two right ones, both defensible.

    Revenue accounts carry a credit balance, so the postings are negative and
    `sign_for_reporting` on dim_account flips them once. Nobody downstream
    flips them again.
-#}

select
    p.posting_date                              as date_key,
    p.posting_date                              as ds,
    p.fiscal_month,
    p.entity_code                               as entity,
    p.currency_code                             as currency_key,

    count(*)                                    as posting_count,
    count(distinct p.document_ref)              as document_count,
    count(distinct p.subledger)                 as subledger_count,

    sum(p.reported_signed_cents) filter (where p.account_type = 'revenue')
                                                as revenue_cents,
    sum(p.reported_signed_cents) filter (where p.account_code = '4000')
                                                as goods_revenue_cents,
    sum(p.reported_signed_cents) filter (where p.account_code = '4100')
                                                as plan_revenue_cents,
    sum(p.reported_signed_cents) filter (where p.account_code in ('4300', '4310'))
                                                as marketplace_revenue_cents,
    sum(p.reported_signed_cents) filter (where p.account_code = '4400')
                                                as freight_recovered_cents,
    sum(p.reported_signed_cents) filter (where p.account_code = '4900')
                                                as breakage_cents,
    sum(p.reported_signed_cents) filter (where p.account_type = 'expense')
                                                as expense_cents,
    sum(p.reported_signed_cents) filter (where p.account_code = '2200')
                                                as tax_cents,

    count(*) filter (where p.rate_date_differs) as postings_on_a_stale_rate,
    bool_and(p.period_is_closed)                as period_is_closed

from {{ ref('fct_gl_postings') }} p
group by 1, 2, 3, 4, 5
