{{ config(materialized='view') }}

{#-
    Marketplace settlement lines, matched back to the marketplace order.

    Three lines an order — principal, commission, fulfilment fee — plus refunds,
    all arriving on their own schedule. This model pivots them to one row per
    marketplace order and states the take.

    `commission_cents` is a reserved name: Copperline's take under
    docs/finance-policy.md REV-14. It is the commission the settlement actually
    posted, not the rate on the order header multiplied out. Those two disagree
    on every order where the seller's rate changed mid-month, and the settlement
    is the one that moved money.
-#}

with settlement_lines as (

    select
        marketplace_order_id,
        payout_id,
        currency_code,
        sum(case when line_type = 'principal'      then magnitude_cents else 0 end) as principal_cents,
        sum(case when line_type = 'commission'     then magnitude_cents else 0 end) as commission_cents,
        sum(case when line_type = 'fulfilment_fee' then magnitude_cents else 0 end) as fulfilment_fee_cents,
        sum(case when line_type = 'refund'         then magnitude_cents else 0 end) as refund_cents,
        count(*)                                                                 as line_count,
        min(payout_date)                                                         as first_payout_date,
        max(payout_date)                                                         as last_payout_date,
        min(cast(posted_at as date))                                             as first_posted_date
    from {{ ref('stg_marketplace__settlements') }}
    group by 1, 2, 3

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

    -- What the seller was actually paid: the principal line, less anything
    -- refunded. The commission and the fee never reach the seller, so they are
    -- not subtracted again — the principal is already net of them.
    coalesce(s.principal_cents, 0) - coalesce(s.refund_cents, 0) as seller_net_cents,

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
