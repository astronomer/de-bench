{{ config(materialized='view') }}

{#-
    An order, its payment attempts and what the processors said about them.

    Two processors, one payment. Meridian is the current one and its events key
    on the payment intent, which is clean. Halcyon is the legacy one and its
    only key back to the order is `order_ref`, which the OMS recycles about
    every fifty days.

    **The migration quarter is the hard part.** For three months in 2025 both
    processors fed settlements for the same payments — that was the plan, it is
    a shadow quarter and it is not a duplicate. docs/runbooks/processor-migration.md
    explains it and names raw.pay_processor_windows as the authority on which
    window belongs to which processor. This model marks a payment seen in both
    books as `is_shadow_quarter` rather than picking one, because picking one
    here would hide the overlap from the reconciliation that exists to find it.

    The Halcyon match is narrowed by the settlement date: an attempt is matched
    only when the Halcyon transaction falls within thirty days of the order.
    That still leaves the recycling ambiguity, so `halcyon_match_is_ambiguous`
    says when more than one order claims the same reference in the window.
-#}

with orders as (

    select
        order_id,
        order_ref,
        order_date,
        channel,
        market_code,
        currency_code,
        grand_total_cents,
        payment_intent_id,
        payment_outcome,
        payment_match_is_ambiguous
    from {{ ref('int_orders_enriched') }}

),

meridian as (

    select
        intent_id,
        count(*)                                                as event_count,
        sum(case when event_type = 'captured' then magnitude_cents else 0 end)   as captured_cents,
        sum(case when event_type = 'refunded' then magnitude_cents else 0 end)   as refunded_cents,
        sum(case when event_type = 'chargeback' then magnitude_cents else 0 end) as chargeback_cents,
        min(settlement_date)                                    as first_settlement_date,
        max(settlement_date)                                    as last_settlement_date,
        max(currency_code)                                      as currency_code
    from {{ ref('stg_payments__meridian_settlements') }}
    where not is_restated
    group by 1

),

halcyon as (

    select
        order_ref,
        settled_date,
        count(*)                                                as txn_count,
        sum(case when status = 'SETTLED' then amount_cents else 0 end)  as settled_cents,
        sum(case when status = 'REVERSED' then amount_cents else 0 end) as reversed_cents,
        max(merchant_acct)                                      as merchant_acct,
        max(currency_code)                                      as currency_code
    from {{ ref('stg_payments__halcyon_settlements') }}
    group by 1, 2

),

halcyon_matched as (

    select
        o.order_id,
        h.txn_count,
        h.settled_cents,
        h.reversed_cents,
        h.merchant_acct,
        h.currency_code,
        h.settled_date,
        count(*) over (partition by o.order_id) as halcyon_candidate_count
    from orders o
    join halcyon h
      on h.order_ref = o.order_ref
     and h.settled_date between o.order_date and o.order_date + 30

),

halcyon_best as (

    select *
    from (
        select *, row_number() over (partition by order_id order by settled_date) as pick
        from halcyon_matched
    ) r
    where pick = 1

)

select
    o.order_id,
    o.order_ref,
    o.order_date,
    o.channel,
    o.market_code,
    o.currency_code,
    o.grand_total_cents,

    o.payment_intent_id,
    o.payment_outcome,
    o.payment_match_is_ambiguous,

    coalesce(m.captured_cents, 0)               as meridian_captured_cents,
    coalesce(m.refunded_cents, 0)               as meridian_refunded_cents,
    coalesce(m.chargeback_cents, 0)             as meridian_chargeback_cents,
    coalesce(m.event_count, 0)                  as meridian_event_count,
    m.first_settlement_date                     as meridian_first_settlement_date,

    coalesce(h.settled_cents, 0)                as halcyon_settled_cents,
    coalesce(h.reversed_cents, 0)               as halcyon_reversed_cents,
    coalesce(h.txn_count, 0)                    as halcyon_txn_count,
    h.merchant_acct                             as halcyon_merchant_acct,
    h.settled_date                              as halcyon_settled_date,
    coalesce(h.halcyon_candidate_count, 0) > 1  as halcyon_match_is_ambiguous,

    m.intent_id is not null                     as seen_by_meridian,
    h.order_id is not null                      as seen_by_halcyon,

    -- Both books carry it. Not a duplicate: the shadow quarter.
    m.intent_id is not null and h.order_id is not null as is_shadow_quarter,

    coalesce(m.captured_cents, 0) - coalesce(m.refunded_cents, 0)
        + coalesce(h.settled_cents, 0) - coalesce(h.reversed_cents, 0) as settled_net_cents,

    case
        when m.intent_id is not null and h.order_id is not null then 'both'
        when m.intent_id is not null then 'meridian'
        when h.order_id is not null then 'halcyon'
        else 'none'
    end                                         as processor

from orders o
left join meridian m on m.intent_id = o.payment_intent_id
left join halcyon_best h on h.order_id = o.order_id
