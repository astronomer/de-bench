{{
    config(
        materialized='table'
    )
}}

{#-
    Every subledger, normalised to debit and credit.

    Five books post into the ledger and each of them has its own shape. Before
    this model, the P&L read the invoice book, the cash reconciliation read the
    processor feeds, the deferred-revenue schedule read the gift-card ledger,
    and the marketplace take was worked out in a spreadsheet. Four answers,
    four owners, and no way to prove they added up.

    Here they are one table with one shape: a posting, an account, a side, and a
    signed amount in cents. **Debits are positive and credits are negative.** A
    balanced document sums to zero. That is the whole reconciliation and it is
    why `fct_gl_postings` can test it with a sum.

    The five books:

    | subledger | document | what it posts |
    |---|---|---|
    | `ar_invoice` | invoice | debit receivable, credit revenue by account, credit tax |
    | `ar_credit`  | credit memo | debit revenue, credit receivable |
    | `gift_card`  | ledger entry | issue credits deferred, redeem debits it, breakage moves it to revenue |
    | `marketplace`| settlement line | commission and fee are revenue; the principal is owed to the seller |
    | `card`       | processor event | debit cash, credit clearing |

    **Finance does not rebuild any of these from source.** This is the model the
    P&L reads, and it is the same model commerce's settlement reconciliation
    reads. That is the load-bearing rule of the architecture: not one right
    number and one wrong one, but two right ones that disagree, which is the
    worst thing a warehouse can produce.

    The entity: invoices and gift cards carry one. The processor books do not,
    so it comes from the currency, which is the same mapping finance uses in the
    close pack. A currency that trades in two entities splits by the entity that
    holds the merchant account, and Halcyon is the only book where that applies.
-#}

with fiscal as (

    select cal_date, fiscal_month, fiscal_year, fiscal_period
    from {{ ref('stg_reference__fiscal_calendar') }}

),

fx as (

    select rate_date, currency_code, fx_rate_ppm
    from {{ ref('stg_reference__fx_rates') }}

),

-- 1. The trade invoice book. Revenue by the account the line names, tax to the
--    tax account, the whole invoice to receivables.
invoice_lines as (

    select
        l.invoice_line_key                      as source_line_ref,
        i.invoice_id                            as document_ref,
        'ar_invoice'                            as subledger,
        i.invoice_date                          as posting_date,
        i.entity_code,
        i.currency_code,
        i.fx_rate_ppm,
        l.revenue_account_code                  as account_code,
        'credit'                                as side,
        -l.line_total_cents                     as signed_amount_cents,
        l.line_kind                             as posting_note
    from {{ ref('stg_finance__invoice_lines') }} l
    join {{ ref('stg_finance__invoices') }} i on i.invoice_id = l.invoice_id

),

invoice_totals as (

    -- The invoice header carries its own net and tax and they do not agree
    -- with the lines: the ERP splits the header at a different point than the
    -- lines do, and the two have disagreed since the trade book was migrated.
    -- The posting is built from the **lines**, because the lines are what the
    -- revenue accounts are on and a receivable that does not equal the revenue
    -- it created is not a posting, it is a plug. The header's own split is kept
    -- on marts.fct_ar_invoices as `header_line_variance_cents`, which is the
    -- number to work if anybody wants to close the gap.
    select
        l.invoice_id,
        sum(l.line_total_cents) as line_total_cents,
        sum(l.tax_cents)        as line_tax_cents
    from {{ ref('stg_finance__invoice_lines') }} l
    group by 1

),

invoice_tax as (

    select
        i.invoice_id || '-TAX'                  as source_line_ref,
        i.invoice_id                            as document_ref,
        'ar_invoice'                            as subledger,
        i.invoice_date                          as posting_date,
        i.entity_code,
        i.currency_code,
        i.fx_rate_ppm,
        '2200'                                  as account_code,
        'credit'                                as side,
        -t.line_tax_cents                       as signed_amount_cents,
        'output tax'                            as posting_note
    from {{ ref('stg_finance__invoices') }} i
    join invoice_totals t on t.invoice_id = i.invoice_id
    where t.line_tax_cents <> 0

),

invoice_receivable as (

    select
        i.invoice_id || '-AR'                   as source_line_ref,
        i.invoice_id                            as document_ref,
        'ar_invoice'                            as subledger,
        i.invoice_date                          as posting_date,
        i.entity_code,
        i.currency_code,
        i.fx_rate_ppm,
        '1200'                                  as account_code,
        'debit'                                 as side,
        t.line_total_cents + t.line_tax_cents   as signed_amount_cents,
        'trade receivable'                      as posting_note
    from {{ ref('stg_finance__invoices') }} i
    join invoice_totals t on t.invoice_id = i.invoice_id

),

-- 2. Credit memos. The mirror of an invoice line, against the same accounts.
credit_memo_revenue as (

    select
        m.credit_memo_id || '-REV'              as source_line_ref,
        m.credit_memo_id                        as document_ref,
        'ar_credit'                             as subledger,
        m.issued_date                           as posting_date,
        i.entity_code,
        m.currency_code,
        i.fx_rate_ppm,
        '4000'                                  as account_code,
        'debit'                                 as side,
        m.amount_cents                          as signed_amount_cents,
        m.reason_code                           as posting_note
    from {{ ref('stg_finance__credit_memos') }} m
    join {{ ref('stg_finance__invoices') }} i on i.invoice_id = m.invoice_id

),

credit_memo_receivable as (

    select
        m.credit_memo_id || '-AR'               as source_line_ref,
        m.credit_memo_id                        as document_ref,
        'ar_credit'                             as subledger,
        m.issued_date                           as posting_date,
        i.entity_code,
        m.currency_code,
        i.fx_rate_ppm,
        '1200'                                  as account_code,
        'credit'                                as side,
        -m.amount_cents                         as signed_amount_cents,
        'trade receivable'                      as posting_note
    from {{ ref('stg_finance__credit_memos') }} m
    join {{ ref('stg_finance__invoices') }} i on i.invoice_id = m.invoice_id

),

-- 3. Gift cards. A card is a liability until it is spent or it expires.
gift_card_postings as (

    select
        g.entry_id || '-LIAB'                   as source_line_ref,
        g.entry_id                              as document_ref,
        'gift_card'                             as subledger,
        g.occurred_date                         as posting_date,
        g.entity_code,
        c.currency_code,
        cast(null as bigint)                    as fx_rate_ppm,
        '2400'                                  as account_code,
        case when g.is_money_out then 'debit' else 'credit' end as side,
        case when g.is_money_out then g.magnitude_cents else -g.magnitude_cents end
                                                as signed_amount_cents,
        g.entry_type                            as posting_note
    from {{ ref('stg_sales__gift_card_ledger') }} g
    join {{ ref('stg_sales__gift_cards') }} c on c.card_id = g.card_id

    union all

    select
        g.entry_id || '-CONTRA'                 as source_line_ref,
        g.entry_id                              as document_ref,
        'gift_card'                             as subledger,
        g.occurred_date                         as posting_date,
        g.entity_code,
        c.currency_code,
        cast(null as bigint)                    as fx_rate_ppm,
        case g.entry_type
            when 'issue'    then '1000'         -- cash taken for the card
            when 'reload'   then '1000'
            when 'redeem'   then '4000'         -- the card pays for goods
            when 'breakage' then '4900'         -- an expired card becomes income
        end                                     as account_code,
        case when g.is_money_out then 'credit' else 'debit' end as side,
        case when g.is_money_out then -g.magnitude_cents else g.magnitude_cents end
                                                as signed_amount_cents,
        g.entry_type                            as posting_note
    from {{ ref('stg_sales__gift_card_ledger') }} g
    join {{ ref('stg_sales__gift_cards') }} c on c.card_id = g.card_id

),

-- 4. Marketplace settlement.
--
--    Copperline runs the marketplace, so the principal on a settlement is the
--    seller's money passing through: it credits the clearing account and lands
--    in a payable, and it is not Copperline revenue. Copperline's revenue is the
--    commission and the fulfilment fee. Booking the principal as revenue is how
--    the FY2024 marketplace line came out about eight times too big, and it is
--    why the seller payable exists as an account.
marketplace_postings as (

    select
        s.settlement_id || '-CLR'               as source_line_ref,
        s.marketplace_order_id                  as document_ref,
        'marketplace'                           as subledger,
        cast(s.posted_at as date)               as posting_date,
        cast(null as varchar)                   as entity_code,
        s.currency_code,
        cast(null as bigint)                    as fx_rate_ppm,
        '1210'                                  as account_code,
        case when s.is_money_out then 'credit' else 'debit' end as side,
        case when s.is_money_out then -s.magnitude_cents else s.magnitude_cents end
                                                as signed_amount_cents,
        s.line_type                             as posting_note
    from {{ ref('stg_marketplace__settlements') }} s

    union all

    select
        s.settlement_id || '-CTR'               as source_line_ref,
        s.marketplace_order_id                  as document_ref,
        'marketplace'                           as subledger,
        cast(s.posted_at as date)               as posting_date,
        cast(null as varchar)                   as entity_code,
        s.currency_code,
        cast(null as bigint)                    as fx_rate_ppm,
        case s.line_type
            when 'principal'      then '2500'   -- owed to the seller
            when 'commission'     then '4300'   -- commission earned
            when 'fulfilment_fee' then '4310'   -- fulfilment fee earned
            when 'refund'         then '4300'   -- commission given back
        end                                     as account_code,
        case when s.is_money_out then 'debit' else 'credit' end as side,
        case when s.is_money_out then s.magnitude_cents else -s.magnitude_cents end
                                                as signed_amount_cents,
        s.line_type                             as posting_note
    from {{ ref('stg_marketplace__settlements') }} s

),

-- 5. The card books. Both processors, and the migration quarter is in here
--    twice on purpose: dropping one side would hide the overlap rather than
--    reconcile it. marts.cash_recon_daily is where the two are netted, and
--    docs/runbooks/processor-migration.md says which window is affected.
card_postings as (

    select
        m.event_id || '-CASH'                   as source_line_ref,
        m.payment_id                            as document_ref,
        'card'                                  as subledger,
        coalesce(m.settlement_date, cast(m.event_time_utc as date)) as posting_date,
        cast(null as varchar)                   as entity_code,
        m.currency_code,
        cast(null as bigint)                    as fx_rate_ppm,
        '1000'                                  as account_code,
        case when m.is_money_out then 'credit' else 'debit' end as side,
        case when m.is_money_out then -m.magnitude_cents else m.magnitude_cents end
                                                as signed_amount_cents,
        'meridian ' || m.event_type             as posting_note
    from {{ ref('stg_payments__meridian_settlements') }} m
    where m.event_type in ('captured', 'refunded', 'chargeback')
      and not m.is_restated

    union all

    select
        m.event_id || '-CLR'                    as source_line_ref,
        m.payment_id                            as document_ref,
        'card'                                  as subledger,
        coalesce(m.settlement_date, cast(m.event_time_utc as date)) as posting_date,
        cast(null as varchar)                   as entity_code,
        m.currency_code,
        cast(null as bigint)                    as fx_rate_ppm,
        '1210'                                  as account_code,
        case when m.is_money_out then 'debit' else 'credit' end as side,
        case when m.is_money_out then m.magnitude_cents else -m.magnitude_cents end
                                                as signed_amount_cents,
        'meridian ' || m.event_type             as posting_note
    from {{ ref('stg_payments__meridian_settlements') }} m
    where m.event_type in ('captured', 'refunded', 'chargeback')
      and not m.is_restated

    union all

    -- Halcyon lands about a third of its rows with no currency at all: the feed
    -- was built before Copperline sold outside the United States. The merchant
    -- account is the only thing on the row that knows, so the region behind it
    -- supplies the currency here.
    select
        h.txn_id || '-CASH'                     as source_line_ref,
        h.txn_id                                as document_ref,
        'card'                                  as subledger,
        coalesce(h.settled_date, h.file_date)   as posting_date,
        cast(null as varchar)                   as entity_code,
        coalesce(h.currency_code, r.region_currency_code) as currency_code,
        cast(null as bigint)                    as fx_rate_ppm,
        '1000'                                  as account_code,
        case when h.status = 'SETTLED' then 'debit' else 'credit' end as side,
        case when h.status = 'SETTLED' then h.amount_cents else -h.amount_cents end
                                                as signed_amount_cents,
        'halcyon ' || lower(h.status)           as posting_note
    from {{ ref('stg_payments__halcyon_settlements') }} h
    left join (
        select
            m.merchant_acct,
            case m.region_code
                when 'US' then 'USD'
                when 'CA' then 'CAD'
                when 'GB' then 'GBP'
                when 'IE' then 'EUR'
                when 'DE' then 'EUR'
            end as region_currency_code
        from {{ ref('stg_payments__merchant_regions') }} m
        where m.is_current
    ) r on r.merchant_acct = h.merchant_acct
    where h.status in ('SETTLED', 'REVERSED')

    union all

    select
        h.txn_id || '-CLR'                      as source_line_ref,
        h.txn_id                                as document_ref,
        'card'                                  as subledger,
        coalesce(h.settled_date, h.file_date)   as posting_date,
        cast(null as varchar)                   as entity_code,
        coalesce(h.currency_code, r.region_currency_code) as currency_code,
        cast(null as bigint)                    as fx_rate_ppm,
        '1210'                                  as account_code,
        case when h.status = 'SETTLED' then 'credit' else 'debit' end as side,
        case when h.status = 'SETTLED' then -h.amount_cents else h.amount_cents end
                                                as signed_amount_cents,
        'halcyon ' || lower(h.status)           as posting_note
    from {{ ref('stg_payments__halcyon_settlements') }} h
    left join (
        select
            m.merchant_acct,
            case m.region_code
                when 'US' then 'USD'
                when 'CA' then 'CAD'
                when 'GB' then 'GBP'
                when 'IE' then 'EUR'
                when 'DE' then 'EUR'
            end as region_currency_code
        from {{ ref('stg_payments__merchant_regions') }} m
        where m.is_current
    ) r on r.merchant_acct = h.merchant_acct
    where h.status in ('SETTLED', 'REVERSED')

),

all_postings as (

    select * from invoice_lines
    union all select * from invoice_tax
    union all select * from invoice_receivable
    union all select * from credit_memo_revenue
    union all select * from credit_memo_receivable
    union all select * from gift_card_postings
    union all select * from marketplace_postings
    union all select * from card_postings

),

entity_from_currency as (

    -- The processor books carry no entity. The close pack maps them by the
    -- currency the payment settled in, and this is the same mapping.
    select * from (
        values
            ('USD', 'CL-US'),
            ('CAD', 'CL-US'),
            ('GBP', 'CL-GB'),
            ('EUR', 'CL-DE'),
            ('MXN', 'CL-MX'),
            ('BRL', 'CL-US'),
            ('PLN', 'CL-DE'),
            ('IDR', 'CL-US')
    ) as t (currency_code, entity_code)

)

select
    {{ dbt_utils.surrogate_key(['p.subledger', 'p.source_line_ref']) }} as posting_key,
    p.subledger,
    p.document_ref,
    p.source_line_ref,
    p.posting_date,
    f.fiscal_month,
    f.fiscal_year,
    f.fiscal_period,

    coalesce(p.entity_code, e.entity_code, 'CL-US')  as entity_code,
    p.entity_code is null                            as entity_was_derived,

    p.account_code,
    p.side,
    p.posting_note,

    coalesce(p.currency_code, 'USD')                 as currency_code,
    p.currency_code is null                          as currency_was_defaulted,

    p.signed_amount_cents,
    abs(p.signed_amount_cents)                       as amount_cents,

    -- Reporting currency. Every converting row carries the rate it used and the
    -- date that rate came from, so the arithmetic can be redone from the row.
    coalesce(p.fx_rate_ppm, r.fx_rate_ppm, 1000000)  as fx_rate_ppm,
    case
        when p.fx_rate_ppm is not null then p.posting_date
        else coalesce(r.rate_date, p.posting_date)
    end                                              as fx_rate_date,
    {{ to_base_cents('p.signed_amount_cents', 'coalesce(p.fx_rate_ppm, r.fx_rate_ppm, 1000000)') }}
                                                     as signed_amount_base_cents

from all_postings p
left join fiscal f on f.cal_date = p.posting_date
left join entity_from_currency e on e.currency_code = p.currency_code
left join fx r
       on r.currency_code = coalesce(p.currency_code, 'USD')
      and r.rate_date = p.posting_date
