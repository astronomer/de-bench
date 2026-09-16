{#-
    Tie: every document balances.

    Debits are positive and credits are negative, so a document that posts
    correctly sums to zero. This is the reconciliation the whole ledger model
    exists to make possible, and it is one `sum`.

    Traceable to contracts/finance-close.md: the close pack cannot be published
    from a book that does not balance.
-#}

select
    subledger,
    document_ref,
    count(*)                    as posting_count,
    sum(signed_amount_cents)    as out_of_balance_cents
from {{ ref('fct_gl_postings') }}
group by 1, 2
having sum(signed_amount_cents) <> 0
