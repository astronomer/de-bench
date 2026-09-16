{#-
    Tie: the header discount allocated to the lines equals the header discount.

    Largest remainder, in whole cents. A one-cent drift here is the difference
    between a margin number that ties and one that nearly ties, and nearly is
    what people spend afternoons on.
-#}

with allocated as (

    select order_id, sum(header_discount_cents) as allocated_cents
    from {{ ref('int_order_lines_discounted') }}
    group by 1

)

select
    o.order_id,
    o.order_discount_cents,
    a.allocated_cents,
    o.order_discount_cents - a.allocated_cents as variance_cents
from {{ ref('int_orders_enriched') }} o
join allocated a on a.order_id = o.order_id
where o.order_discount_cents <> a.allocated_cents
