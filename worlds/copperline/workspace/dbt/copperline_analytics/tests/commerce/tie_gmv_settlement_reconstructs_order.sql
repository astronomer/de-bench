{#-
    Tie: the settlement lines add back up to the marketplace order.

    A marketplace order arrives on one feed with a `gmv_cents` on it. The
    settlement lines arrive on another feed, three or four of them, and between
    them they account for the same money: the principal paid to the seller, the
    commission Copperline kept, and the fulfilment fee. Add the three back up
    and you should have the order.

    Traceable to contracts/settlement-summary.md, which commits consumer C-3 to
    one GMV per seller per week and to the take rate read off it. If the lines
    do not reconstruct the order, the take rate is being computed against a
    denominator that is not the numerator's.

    A tolerance of one hundred cents an order covers the marketplace's own
    rounding on the commission.
-#}

select
    s.marketplace_order_id,
    s.seller_id,
    s.placed_date,
    s.gmv_cents,
    s.principal_cents,
    s.commission_cents,
    s.fulfilment_fee_cents,
    s.principal_cents + s.commission_cents + s.fulfilment_fee_cents as settled_cents,
    s.gmv_cents - (s.principal_cents + s.commission_cents + s.fulfilment_fee_cents) as variance_cents
from {{ ref('int_settlement_matched') }} s
where s.is_settled
  and abs(s.gmv_cents - (s.principal_cents + s.commission_cents + s.fulfilment_fee_cents)) > 100
