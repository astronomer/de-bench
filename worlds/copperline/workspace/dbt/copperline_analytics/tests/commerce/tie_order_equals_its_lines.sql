{#-
    Tie: an order's net sales equal the sum of its lines.

    `net_sales_cents` is an order-line number. Summing it to order grain is
    fine, and marts.order_economics does exactly that, so the two have to agree
    exactly — not approximately, because the discount allocation floors per line
    and the largest-remainder step is what makes the sum exact.

    Traceable to contracts/order_economics.yml, which pins the grain and the
    column.
-#}

with lines as (

    select order_id, sum(net_sales_cents) as line_net_cents
    from {{ ref('fct_order_line') }}
    group by 1

)

select
    e.order_id,
    e.order_date,
    e.net_sales_cents,
    l.line_net_cents,
    e.net_sales_cents - l.line_net_cents as variance_cents
from {{ ref('order_economics') }} e
join lines l on l.order_id = e.order_id
where e.net_sales_cents <> l.line_net_cents
