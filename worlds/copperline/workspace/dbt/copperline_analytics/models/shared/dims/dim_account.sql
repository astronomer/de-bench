{{
    config(
        materialized='table'
    )
}}

{#-
    The chart of accounts, as far as the warehouse posts to it.

    Every account int_gl_postings_unified writes to has a row here, with its
    statement, its normal side and its rollup. The list is written out rather
    than derived, because an account with no postings this period still belongs
    in the account rollup and a `select distinct` over the postings would drop
    it — which is exactly how the FY2025 board pack lost the breakage line.

    `normal_side` is what a positive balance means. Postings are signed debit
    positive, so a revenue account with a healthy month shows a large negative
    balance, and every consumer of the rollup flips it. That flip is stated here
    as `sign_for_reporting` so nobody has to remember which way round it goes.
-#}

with chart as (

    select * from (
        values
            ('1000', 'Cash and cash equivalents',   'asset',     'balance_sheet', 'debit',  'current assets',        1),
            ('1200', 'Trade receivables',           'asset',     'balance_sheet', 'debit',  'current assets',        1),
            ('1210', 'Payment clearing',            'asset',     'balance_sheet', 'debit',  'current assets',        1),
            ('2200', 'Output tax payable',          'liability', 'balance_sheet', 'credit', 'current liabilities',  -1),
            ('2400', 'Gift-card liability',         'liability', 'balance_sheet', 'credit', 'current liabilities',  -1),
            ('2500', 'Marketplace seller payable',   'liability', 'balance_sheet', 'credit', 'current liabilities',  -1),
            ('4000', 'Goods revenue',               'revenue',   'income',        'credit', 'revenue',              -1),
            ('4100', 'Plan revenue',                'revenue',   'income',        'credit', 'revenue',              -1),
            ('4300', 'Marketplace commission',      'revenue',   'income',        'credit', 'revenue',              -1),
            ('4310', 'Marketplace fulfilment fee',  'revenue',   'income',        'credit', 'revenue',              -1),
            ('4400', 'Freight recovered',           'revenue',   'income',        'credit', 'revenue',              -1),
            ('4900', 'Gift-card breakage',          'revenue',   'income',        'credit', 'other income',         -1)
    ) as t (
        account_code,
        account_name,
        account_type,
        statement,
        normal_side,
        rollup_group,
        sign_for_reporting
    )

),

posted as (

    select
        account_code,
        count(*)                    as posting_count,
        sum(signed_amount_cents)    as posted_signed_cents,
        min(posting_date)           as first_posting_date,
        max(posting_date)           as last_posting_date,
        count(distinct subledger)   as subledger_count
    from {{ ref('int_gl_postings_unified') }}
    group by 1

)

select
    c.account_code                              as account_key,
    c.account_code,
    c.account_name,
    c.account_type,
    c.statement,
    c.normal_side,
    c.rollup_group,
    c.sign_for_reporting,
    c.account_type in ('revenue', 'expense')    as is_income_statement,

    coalesce(p.posting_count, 0)                as posting_count,
    coalesce(p.posted_signed_cents, 0)          as posted_signed_cents,
    coalesce(p.posted_signed_cents, 0) * c.sign_for_reporting as reported_signed_cents,
    coalesce(p.subledger_count, 0)              as subledger_count,
    p.first_posting_date,
    p.last_posting_date,
    coalesce(p.posting_count, 0) > 0            as has_postings

from chart c
left join posted p on p.account_code = c.account_code
