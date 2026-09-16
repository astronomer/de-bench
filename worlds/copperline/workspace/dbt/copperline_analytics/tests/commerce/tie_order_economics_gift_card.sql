{#-
    Tie: the order header's gift-card amount against the gift-card ledger.

    An order that was part-paid with a gift card carries the amount on the
    header. The gift-card ledger carries a redemption against the same order.
    They are two systems recording one event and they should agree to the cent.

    Traceable to contracts/merch-dashboards.md, which states that
    `booked_cents` is the whole order including tender of any kind, so the
    tender split has to add up.
-#}

with header as (

    select
        order_id,
        order_date,
        gift_card_applied_cents
    from {{ ref('fct_order') }}
    where gift_card_applied_cents > 0

),

ledger as (

    select
        order_id,
        sum(magnitude_cents) as redeemed_cents
    from {{ ref('stg_sales__gift_card_ledger') }}
    where entry_type = 'redeem'
      and order_id is not null
    group by 1

)

select
    h.order_id,
    h.order_date,
    h.gift_card_applied_cents,
    coalesce(l.redeemed_cents, 0)                           as redeemed_cents,
    h.gift_card_applied_cents - coalesce(l.redeemed_cents, 0) as variance_cents
from header h
left join ledger l on l.order_id = h.order_id
where h.gift_card_applied_cents <> coalesce(l.redeemed_cents, 0)
