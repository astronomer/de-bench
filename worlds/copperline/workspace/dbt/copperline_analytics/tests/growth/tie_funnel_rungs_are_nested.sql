{#-
    Tie: each funnel rung is a subset of the one below it.

    A session that purchased also reached the cart. If `reached_cart` is ever
    smaller than `reached_purchase`, the rungs have been counted independently
    rather than as a high-water mark, and every rate read off the table is
    wrong. The old version of this table did exactly that.
-#}

select
    ds,
    attributed_source,
    device,
    sessions,
    reached_product,
    reached_cart,
    reached_checkout,
    reached_purchase
from {{ ref('funnel_daily') }}
where reached_product > sessions
   or reached_cart > reached_product
   or reached_checkout > reached_cart
   or reached_purchase > reached_checkout
