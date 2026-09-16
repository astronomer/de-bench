{#-
    Tie: no payment is counted twice.

    Meridian took over from Halcyon and both books fed settlements for the same
    payments through the migration quarter. That is the shadow quarter and it is
    not a duplicate, so "both books hold this order" was never the thing to
    assert — it was true of the whole quarter by design, and this test said so
    every night.

    `raw.pay_processor_windows` is the record of which book was authoritative on
    a date, and `int_payment_matched` now takes each settlement row from the book
    whose window holds the row's own date. Once it does, one thing has to hold:
    an order cannot settle for more than it was placed for. Money above the
    order's own total is the same payment counted from both books.

    An order that settles for LESS is a refund, a reversal, a chargeback, or an
    attempt the recycled reference never matched. That is a real disagreement
    and it belongs on `marts.recon_exceptions` under docs/reconciliation-policy.md
    R-7, not here.

    A hundred cents an order of allowance, for the rounding a Halcyon amount
    picks up on its way through `to_cents` from the text the feed lands.
-#}

select
    p.order_id,
    p.order_date,
    p.order_ref,
    p.processor,
    p.grand_total_cents,
    p.meridian_captured_cents,
    p.halcyon_settled_cents,
    p.shadow_cents,
    p.settled_net_cents,
    p.settled_net_cents - p.grand_total_cents as variance_cents
from {{ ref('int_payment_matched') }} p
where p.settled_net_cents - p.grand_total_cents > 100
