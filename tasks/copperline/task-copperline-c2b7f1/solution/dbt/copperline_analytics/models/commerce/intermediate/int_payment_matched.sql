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
    window belongs to which processor.

    So this model reads that table and takes each settlement row from the book
    that was authoritative **on the row's own date**: Halcyon to the end of its
    window, Meridian from the start of its own. The rest is shadow traffic. It
    is counted, in `shadow_cents`, and it is not in `settled_net_cents` — the
    reconciliation that exists to find the overlap can still see it, and nothing
    downstream adds the same payment up twice.

    The boundary is inside the quarter and not at the end of it, which is the
    thing that made this look like a whole quarter of duplicates: both feeds run
    from July to September, and the authority changes hands in the middle. A
    payment captured in August and settled in September is claimed by both books
    and both are right, each on its own date, so an order can draw an
    authoritative half from each. Every column here says which book it came
    from; `processor` says which book the money came from.

    Both books are still published whole in `meridian_*_cents` and
    `halcyon_*_cents`. Netting them is what hides the overlap, so nothing here
    nets them.

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

-- The record. Two rows. An open window ends at the end of dates.
windows as (

    select
        processor,
        authoritative_from,
        coalesce(authoritative_to, date '9999-12-31')   as authoritative_to
    from {{ ref('stg_payments__processor_windows') }}

),

meridian_window as (

    select authoritative_from, authoritative_to
    from windows
    where processor = 'meridian'

),

halcyon_window as (

    select authoritative_from, authoritative_to
    from windows
    where processor = 'halcyon'

),

meridian as (

    select
        m.intent_id,
        count(*)                                                as event_count,
        sum(case when m.event_type = 'captured' then m.magnitude_cents else 0 end)   as captured_cents,
        sum(case when m.event_type = 'refunded' then m.magnitude_cents else 0 end)   as refunded_cents,
        sum(case when m.event_type = 'chargeback' then m.magnitude_cents else 0 end) as chargeback_cents,

        -- The same three sums, over the events settled inside Meridian's own
        -- window. Everything else is the shadow copy of a payment Halcyon was
        -- authoritative for.
        sum(case
                when m.event_type = 'captured'
                 and m.settlement_date between w.authoritative_from and w.authoritative_to
                then m.magnitude_cents else 0 end)              as authoritative_captured_cents,
        sum(case
                when m.event_type = 'refunded'
                 and m.settlement_date between w.authoritative_from and w.authoritative_to
                then m.magnitude_cents else 0 end)              as authoritative_refunded_cents,
        sum(case
                when m.event_type = 'captured'
                 and m.settlement_date not between w.authoritative_from and w.authoritative_to
                then m.magnitude_cents else 0 end)              as shadow_captured_cents,

        min(m.settlement_date)                                  as first_settlement_date,
        max(m.settlement_date)                                  as last_settlement_date,
        max(m.currency_code)                                    as currency_code
    from {{ ref('stg_payments__meridian_settlements') }} m
    cross join meridian_window w
    where not m.is_restated
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
        h.settled_date between w.authoritative_from and w.authoritative_to as is_authoritative,
        count(*) over (partition by o.order_id) as halcyon_candidate_count
    from orders o
    join halcyon h
      on h.order_ref = o.order_ref
     and h.settled_date between o.order_date and o.order_date + 30
    cross join halcyon_window w

),

halcyon_best as (

    select *
    from (
        select *, row_number() over (partition by order_id order by settled_date) as pick
        from halcyon_matched
    ) r
    where pick = 1

),

assembled as (

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

        -- What each book says, whole. Neither is netted against the other.
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

        -- What each book was authoritative for, on the dates its own rows carry.
        coalesce(m.authoritative_captured_cents, 0)
            - coalesce(m.authoritative_refunded_cents, 0)        as meridian_net_cents,
        case
            when coalesce(h.is_authoritative, false)
            then coalesce(h.settled_cents, 0) - coalesce(h.reversed_cents, 0)
            else 0
        end                                                     as halcyon_net_cents,

        -- What each book claimed outside its own window: the same payment, seen
        -- a second time. Reported, never counted.
        coalesce(m.shadow_captured_cents, 0)                     as meridian_shadow_cents,
        case
            when h.order_id is not null and not coalesce(h.is_authoritative, false)
            then coalesce(h.settled_cents, 0)
            else 0
        end                                                     as halcyon_shadow_cents

    from orders o
    left join meridian m on m.intent_id = o.payment_intent_id
    left join halcyon_best h on h.order_id = o.order_id

)

select
    a.*,

    a.meridian_net_cents + a.halcyon_net_cents      as settled_net_cents,
    a.meridian_shadow_cents + a.halcyon_shadow_cents as shadow_cents,

    -- The shadow quarter, as the runbook means it: a payment one book claims
    -- outside the window that book was authoritative for.
    a.meridian_shadow_cents + a.halcyon_shadow_cents <> 0 as is_shadow_quarter,

    case
        when a.meridian_net_cents <> 0 and a.halcyon_net_cents <> 0 then 'both'
        when a.meridian_net_cents <> 0 then 'meridian'
        when a.halcyon_net_cents <> 0 then 'halcyon'
        else 'none'
    end                                             as processor

from assembled a
