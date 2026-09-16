{#-
    Tie: the settlement lines add back up to the marketplace order.

    A marketplace order arrives on one feed with a `gmv_cents` on it. The
    settlement lines arrive on another, and the principal line is that same
    money seen from the operator's side: the whole order value, owed to the
    seller, before anything is held back. The commission and the fulfilment fee
    are Copperline's take. They come off the principal, so a reconstruction
    that adds them to it counts Copperline's own revenue into the seller's
    order and disagrees with every settled order by exactly the take.

    So the reconstruction is the principal against the order. If a principal
    line goes missing, lands on the wrong order or is restated, the two part
    company and this test says so.

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
    s.refund_cents,
    s.seller_net_cents,
    s.principal_cents as settled_cents,
    s.gmv_cents - s.principal_cents as variance_cents
from {{ ref('int_settlement_matched') }} s
where s.is_settled
  and abs(s.gmv_cents - s.principal_cents) > 100
