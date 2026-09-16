{{ config(materialized='table') }}

{#-
    Every posting, from every subledger, in reporting currency.

    Journal-line grain. Debits positive, credits negative, so a balanced
    document sums to zero and the reconciliation is a `sum`, not an argument.

    **This is not the ERP's journal.** It is what the warehouse's five subledgers
    would post if they posted, and the ERP's own monthly close file is a
    separate and independent number. Keeping the two apart is the point: a tie
    between them means something, and a tie between a table and itself does not.
-#}

select
    f.posting_key,
    f.subledger,
    f.document_ref,
    f.posting_date                              as date_key,
    f.posting_date,
    f.fiscal_month,
    f.fiscal_year,
    f.fiscal_period,
    f.entity_code,
    f.account_code                              as account_key,
    f.account_code,
    a.account_name,
    a.account_type,
    a.statement,
    a.rollup_group,
    a.sign_for_reporting,
    f.side,
    f.currency_code                             as currency_key,
    f.currency_code,

    f.signed_amount_cents,
    f.amount_cents,
    f.fx_rate_ppm,
    f.fx_rate_date,
    f.signed_amount_base_cents,
    f.amount_base_cents,
    f.signed_amount_base_cents * a.sign_for_reporting as reported_signed_cents,

    f.rate_source,
    f.rate_date_differs,
    f.currency_was_defaulted,
    f.entity_was_derived,
    c.is_closed                                 as period_is_closed

from {{ ref('int_fx_applied') }} f
left join {{ ref('dim_account') }} a on a.account_code = f.account_code
left join {{ ref('int_closed_month') }} c on c.fiscal_month = f.fiscal_month
