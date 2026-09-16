{#-
    Tie: a revenue account carries a credit balance.

    Postings are signed debit-positive, so revenue nets negative. A revenue
    account with a debit balance for a whole fiscal month means the postings
    have been signed the wrong way round somewhere, which has happened once and
    took a week to find.

    Credit memos are debits against revenue and a small entity can credit more
    in a month than it invoiced, which is legitimate and happens about three
    dozen times in the range. So the test covers the books that create revenue
    and leaves the credit book out; a credit memo debiting revenue is the whole
    point of a credit memo.
-#}

select
    fiscal_month,
    entity_code,
    account_code,
    sum(signed_amount_cents) as balance_cents
from {{ ref('fct_gl_postings') }}
where account_code in ('4000', '4100', '4300', '4310')
  and subledger <> 'ar_credit'
group by 1, 2, 3
having sum(signed_amount_cents) > 0
