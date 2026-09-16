{{ config(materialized='view') }}

{#-
    Postings in reporting currency, with the rate that got them there.

    The ERP posts an invoice at its own rate and that rate is on the invoice,
    because the ledger holds it and a rate looked up afterwards will not tie.
    Everything else — the processor books, the marketplace, the gift cards —
    carries no rate and is converted at the rate for the posting date.

    Both paths are here and `rate_source` says which one produced each row. That
    column exists because the two were mixed for most of FY2024 and a month that
    had both in it could not be reconciled to the ledger without going back to
    the invoices one at a time.

    Rates are parts per million, integers. The arithmetic multiplies first and
    divides once, so a converted amount is reproducible from the row: amount,
    rate, date.
-#}

select
    p.posting_key,
    p.subledger,
    p.document_ref,
    p.posting_date,
    p.fiscal_month,
    p.fiscal_year,
    p.fiscal_period,
    p.entity_code,
    p.account_code,
    p.side,
    p.currency_code,

    p.signed_amount_cents,
    p.amount_cents,

    p.fx_rate_ppm,
    p.fx_rate_date,
    p.signed_amount_base_cents,
    abs(p.signed_amount_base_cents)             as amount_base_cents,

    case
        when p.currency_code = 'USD'            then 'base'
        when p.subledger like 'ar_%'            then 'posted on the document'
        when p.fx_rate_date = p.posting_date    then 'daily rate'
        else 'nearest published rate'
    end                                         as rate_source,

    p.currency_was_defaulted,
    p.entity_was_derived,
    p.fx_rate_date <> p.posting_date            as rate_date_differs

from {{ ref('int_gl_postings_unified') }} p
