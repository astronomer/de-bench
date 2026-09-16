{#-
    Tie: the order-level discount against the promotions applied to the order.

    Written in FY2024, when every promotion was an order-level one and the two
    numbers matched by construction. Line-level promotions arrived in FY2025 and
    they land inside `line_total_cents` on the line, not on the header — so a
    line promotion is in the sum on one side of this comparison and not on the
    other, and the test has failed every night since.

    No contract states this rule. contracts/order_economics.yml pins the grain,
    the columns and the channel list, and says nothing about how a discount is
    split between the header and the line.
-#}

with header as (

    select order_id, order_date, order_discount_cents
    from {{ ref('order_economics') }}

),

applied as (

    select order_id, sum(discount_cents) as applied_cents
    from {{ ref('stg_sales__promo_applications') }}
    group by 1

)

select
    h.order_id,
    h.order_date,
    h.order_discount_cents,
    a.applied_cents,
    h.order_discount_cents - a.applied_cents as variance_cents
from header h
join applied a on a.order_id = h.order_id
where h.order_discount_cents <> a.applied_cents
