{#-
    Tie: no payment settles through both processors.

    Meridian took over from Halcyon and the migration was meant to be clean
    after the shadow quarter closed on 2025-09-30. A payment that both books
    claim outside that window is money counted twice, and inside it is the
    overlap the migration planned for.

    This test covers the whole range on purpose, because "the shadow quarter is
    over" is the assertion. Traceable to contracts/settlement-summary.md, which
    commits the summary to one settled amount per order.
-#}

select
    p.order_id,
    p.order_date,
    p.order_ref,
    p.meridian_captured_cents,
    p.halcyon_settled_cents,
    p.meridian_captured_cents + p.halcyon_settled_cents - p.grand_total_cents as variance_cents
from {{ ref('int_payment_matched') }} p
where p.is_shadow_quarter
