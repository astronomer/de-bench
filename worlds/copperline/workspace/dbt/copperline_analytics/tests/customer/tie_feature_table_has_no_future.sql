{#-
    Tie: no feature row is computed from the future.

    Every column on marts.feature_customer_daily is "as of ds". A row whose last
    order date is after its own `ds` has read an event that had not happened
    yet, which scores beautifully in training and does nothing in production.
    This table has leaked once already.
-#}

select
    ds,
    customer_id,
    last_order_date,
    orders_7d,
    orders_30d,
    net_sales_cents
from {{ ref('feature_customer_daily') }}
where last_order_date > ds
