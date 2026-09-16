{{ config(materialized='view') }}

{#-
    Marketplace settlement lines, matched back to the marketplace order.

    Three lines an order — principal, commission, fulfilment fee — plus refunds,
    all arriving on their own schedule. This model pivots them to one row per
    marketplace order and states the take.

    **The principal line is the order's gross value, not the seller's net.** The
    feed is written from the operator's side. The principal is the money
    Copperline owes the seller and signs negative; the commission and the
    fulfilment fee sign positive and are the take, held back before the payout
    leaves. So the principal reconstructs `gmv_cents` on its own, and what
    reaches the seller is the principal less the take.
    `stg_marketplace__payouts.net_paid_cents` is that same arithmetic done by
    the platform, and `contracts/settlement-summary.md` SS-1 states it for the
    week.

    **One row per marketplace order.** A refund posts with the payout run that
    carried it, three weeks after the run that carried the commission, so an
    order's lines can sit in two payouts. The pivot is keyed on the order alone
    and `payout_id` is the payout that carried the principal. Keying it on the
    payout as well puts every refunded order in this model twice, and every
    reader of it counts that order's GMV twice.

    `commission_cents` is a reserved name: Copperline's take under
    docs/finance-policy.md REV-14. It is the commission the settlement actually
    posted, not the rate on the order header multiplied out. Those two disagree
    on every order where the seller's rate changed mid-month, and the settlement
    is the one that moved money.
-#}

with settlement_lines as (

    select
        marketplace_order_id,
        currency_code,
        min(payout_id) filter (where line_type = 'principal')                    as payout_id,
        sum(case when line_type = 'principal'      then magnitude_cents else 0 end) as principal_cents,
        sum(case when line_type = 'commission'     then magnitude_cents else 0 end) as commission_cents,
        sum(case when line_type = 'fulfilment_fee' then magnitude_cents else 0 end) as fulfilment_fee_cents,
        sum(case when line_type = 'refund'         then magnitude_cents else 0 end) as refund_cents,
        count(*)                                                                 as line_count,
        min(payout_date)                                                         as first_payout_date,
        max(payout_date)                                                         as last_payout_date,
        min(cast(posted_at as date))                                             as first_posted_date
    from {{ ref('stg_marketplace__settlements') }}
    group by 1, 2

),

orders as (

    select *
    from {{ ref('stg_marketplace__orders') }}

),

payouts as (

    select payout_id, seller_id, payout_date, payout_status, net_paid_cents
    from {{ ref('stg_marketplace__payouts') }}

)

select
    o.marketplace_order_id,
    o.marketplace_order_key,
    o.seller_id,
    o.order_state,
    o.placed_date,
    o.ship_confirmed_at,
    coalesce(o.currency_code, s.currency_code)      as currency_code,

    o.gmv_cents,
    o.commission_rate_bps,

    coalesce(s.principal_cents, 0)                  as principal_cents,
    coalesce(s.commission_cents, 0)                 as commission_cents,
    coalesce(s.fulfilment_fee_cents, 0)             as fulfilment_fee_cents,
    coalesce(s.refund_cents, 0)                     as refund_cents,
    coalesce(s.line_count, 0)                       as settlement_line_count,

    -- What actually left Copperline's account for this order. The principal is
    -- the order's gross value, so the commission and the fulfilment fee come
    -- off it rather than adding to it, and a refund hands the commission back
    -- and so adds. This is `net_paid_cents` at order grain.
    coalesce(s.principal_cents, 0)
        - coalesce(s.commission_cents, 0)
        - coalesce(s.fulfilment_fee_cents, 0)
        + coalesce(s.refund_cents, 0)               as seller_net_cents,

    -- The header's rate against what settlement actually took. A gap means the
    -- rate moved after the order was placed.
    {{ apply_bps('o.gmv_cents', 'o.commission_rate_bps') }}     as commission_at_header_rate_cents,
    coalesce(s.commission_cents, 0)
        - {{ apply_bps('o.gmv_cents', 'o.commission_rate_bps') }} as commission_rate_variance_cents,

    s.payout_id,
    p.payout_date,
    p.payout_status,
    s.first_posted_date                             as settled_from_date,
    s.last_payout_date                              as settled_to_date,

    s.marketplace_order_id is not null              as is_settled,
    coalesce(s.line_count, 0) >= 3                  as is_fully_settled,
    coalesce(s.refund_cents, 0) > 0                 as has_refund

from orders o
left join settlement_lines s on s.marketplace_order_id = o.marketplace_order_id
left join payouts p on p.payout_id = s.payout_id
