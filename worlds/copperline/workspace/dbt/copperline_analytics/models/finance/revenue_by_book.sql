{{ config(materialized='table') }}

{#-
    Revenue split by the book it came from: Copperline or Northwave.

    The Northwave book has never been merged. Its accounts are still keyed
    `NWA-#####` and its invoices carry `nwv_account_id` rather than a customer
    reference, so "which book" is a real question with a real answer and not a
    reporting nicety. ops.merge_candidates holds every merge decision anyone has
    made; an undecided candidate is not a merge and is not folded in here.
-#}

with rollup as (

    select
        fiscal_month,
        entity,
        customer_id,
        account_key,
        reported_cents,
        recognized_cents,
        is_closed
    from {{ ref('account_rollup') }}

),

books as (

    select
        i.customer_ref,
        i.nwv_account_id,
        i.fiscal_month,
        i.entity_code,
        case when i.nwv_account_id is not null then 'northwave' else 'copperline' end as source_book,
        sum(i.header_total_cents)               as invoiced_cents,
        count(*)                                as invoice_count
    from {{ ref('fct_ar_invoices') }} i
    group by 1, 2, 3, 4, 5

)

select
    b.fiscal_month,
    b.entity_code                               as entity,
    b.source_book,

    count(distinct coalesce(b.customer_ref, b.nwv_account_id)) as account_count,
    sum(b.invoiced_cents)                       as invoiced_cents,
    sum(b.invoice_count)                        as invoice_count,

    sum(r.reported_cents)                       as reported_cents,
    sum(r.recognized_cents)                     as recognized_cents,
    bool_and(coalesce(r.is_closed, false))      as period_is_closed

from books b
left join rollup r
       on r.fiscal_month = b.fiscal_month
      and r.entity = b.entity_code
      and r.customer_id = b.customer_ref
group by 1, 2, 3
