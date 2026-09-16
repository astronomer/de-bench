{{
    config(
        materialized='view'
    )
}}

{#- One row per promotion applied to an order.

    A row with `order_line_id` set discounted that line. A row without one
    discounted the header. The two do not add up to the same number and they are
    not meant to: the header discount is the order-level promotion and the line
    discounts are already inside line_total_cents. int_order_lines_discounted is
    where the header discount is allocated down.
-#}

select
    application_id,
    order_id,
    order_line_id,
    promo_id,
    cast(discount_cents as bigint)      as discount_cents,
    cast(applied_seq as integer)        as applied_seq,
    computed_by,
    order_line_id is null               as is_header_discount
from {{ source('sales', 'promo_applications') }}
