{{ config(materialized='table') }}

{#-
    Cash in against cash expected, by day.

    Reads the postings rather than the settlement mart, because the postings are
    the shared model and the settlement mart is commerce's. The two processors
    are both in the postings and the migration quarter is in there twice — that
    is the shadow quarter and not a duplicate — so `shadow_overlap_cents` is the
    part of the day's cash that appears in both books.

    docs/runbooks/processor-migration.md names raw.pay_processor_windows as the
    authority on which window belongs to which processor. This model does not
    read it, and so it can report the overlap but not resolve it.
-#}

with cash_postings as (

    select
        posting_date                            as ds,
        entity_code,
        currency_code,
        sum(signed_amount_cents) filter (where account_code = '1000')   as cash_cents,
        sum(signed_amount_cents) filter (where account_code = '1210')   as clearing_cents,
        sum(signed_amount_cents) filter (where account_code = '1200')   as receivable_cents,
        count(*) filter (where subledger = 'card')                      as card_postings,
        count(*) filter (where subledger = 'marketplace')               as marketplace_postings,
        count(*)                                                        as posting_count
    from {{ ref('fct_gl_postings') }}
    group by 1, 2, 3

),

by_processor as (

    select
        coalesce(settlement_date, cast(event_time_utc as date)) as ds,
        'meridian'                              as processor,
        sum(amount_cents)                       as settled_cents,
        count(*)                                as txn_count
    from {{ ref('stg_payments__meridian_settlements') }}
    where event_type in ('captured', 'refunded', 'chargeback') and not is_restated
    group by 1

    union all

    select
        coalesce(settled_date, file_date)       as ds,
        'halcyon'                               as processor,
        sum(case when status = 'SETTLED' then amount_cents else -amount_cents end) as settled_cents,
        count(*)                                as txn_count
    from {{ ref('stg_payments__halcyon_settlements') }}
    where status in ('SETTLED', 'REVERSED')
    group by 1

),

processors as (

    select
        ds,
        sum(settled_cents) filter (where processor = 'meridian')    as meridian_cents,
        sum(settled_cents) filter (where processor = 'halcyon')     as halcyon_cents,
        sum(txn_count)                                              as processor_txn_count,
        count(*)                                                    as processor_count
    from by_processor
    group by 1

)

select
    c.ds                                        as date_key,
    c.ds,
    c.entity_code                               as entity,
    c.currency_code                             as currency_key,

    coalesce(c.cash_cents, 0)                   as cash_cents,
    coalesce(c.clearing_cents, 0)               as clearing_cents,
    coalesce(c.receivable_cents, 0)             as receivable_cents,
    c.card_postings,
    c.marketplace_postings,
    c.posting_count,

    coalesce(p.meridian_cents, 0)               as meridian_cents,
    coalesce(p.halcyon_cents, 0)                as halcyon_cents,
    coalesce(p.processor_txn_count, 0)          as processor_txn_count,

    -- The part of the day that both processors claim. Not a duplicate.
    case
        when coalesce(p.meridian_cents, 0) <> 0 and coalesce(p.halcyon_cents, 0) <> 0
        then least(p.meridian_cents, p.halcyon_cents)
        else 0
    end                                         as shadow_overlap_cents,

    coalesce(p.processor_count, 0) > 1          as is_shadow_quarter_day,
    coalesce(c.cash_cents, 0) + coalesce(c.clearing_cents, 0) as cash_and_clearing_cents

from cash_postings c
left join processors p on p.ds = c.ds
