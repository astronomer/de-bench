{#-
    Tie: the order-level discount against the header promotions applied to the
    order.

    Written in FY2024 against every promotion on the order, when every promotion
    was an order-level one and the two numbers matched by construction.
    Line-level promotions arrived in FY2025 and they land inside
    `line_total_cents` on the line, not on the header, so they were in the sum on
    one side of this comparison and not on the other and the test failed every
    night.

    Re-derived under DQ-77 against the header promotions only, which is the rule
    `order_discount_cents` actually states. `stg_sales__promo_applications`
    carries `is_header_discount` — a row with no `order_line_id` discounted the
    header — and the two sides tie on that scope.
-#}

with header as (

    select order_id, order_date, order_discount_cents
    from {{ ref('order_economics') }}

),

applied as (

    select order_id, sum(discount_cents) as applied_cents
    from {{ ref('stg_sales__promo_applications') }}
    where is_header_discount
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
