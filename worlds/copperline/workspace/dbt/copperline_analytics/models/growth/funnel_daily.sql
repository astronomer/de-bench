{{ config(materialized='table') }}

{#-
    The funnel by day, source and device: how many sessions reached each rung.

    The rungs are cumulative — a session that purchased also reached the cart —
    so each column is a subset of the one before it and the rates read straight
    off. That is the shape people expect and the shape the old model did not
    have, which is why its cart rate could exceed its product-view rate.
-#}

select
    s.session_date                              as date_key,
    s.session_date                              as ds,
    s.attributed_source,
    s.device,

    count(*)                                            as sessions,
    count(*) filter (where s.funnel_stage >= 2)         as reached_product,
    count(*) filter (where s.funnel_stage >= 3)         as reached_cart,
    count(*) filter (where s.funnel_stage >= 4)         as reached_checkout,
    count(*) filter (where s.funnel_stage >= 5)         as reached_purchase,

    sum(s.booked_cents)                                 as booked_cents,
    sum(s.net_sales_cents)                              as net_sales_cents,

    cast(round(
        cast(count(*) filter (where s.funnel_stage >= 5) as decimal(38, 4)) * 10000
        / nullif(count(*), 0), 0
    ) as integer)                                       as conversion_rate_bps,

    cast(round(
        cast(count(*) filter (where s.funnel_stage >= 3) as decimal(38, 4)) * 10000
        / nullif(count(*) filter (where s.funnel_stage >= 2), 0), 0
    ) as integer)                                       as product_to_cart_bps,

    cast(round(
        cast(count(*) filter (where s.funnel_stage >= 5) as decimal(38, 4)) * 10000
        / nullif(count(*) filter (where s.funnel_stage >= 4), 0), 0
    ) as integer)                                       as checkout_to_purchase_bps

from {{ ref('int_session_attributed') }} s
group by 1, 2, 3, 4
