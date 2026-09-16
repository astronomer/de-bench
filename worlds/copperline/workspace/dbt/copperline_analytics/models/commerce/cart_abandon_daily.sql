{{ config(materialized='table') }}

{#-
    Carts and checkouts started and not finished, by day.

    Reads int_sessions_funnel, which is the shared definition of a session and
    of what a session reached. Commerce reads it for this and growth reads it
    for the funnel, and they get the same denominator because there is only one.

    Web only. A marketplace order has no session, so it has no cart to abandon
    and does not appear in the denominator.
-#}

select
    s.session_date                              as date_key,
    s.session_date                              as ds,
    s.device,
    s.acquisition_source,
    coalesce(s.country_code, 'unknown')         as country_code,

    count(*)                                    as session_count,
    count(*) filter (where s.product_views > 0)     as sessions_with_product_view,
    count(*) filter (where s.add_to_carts > 0)      as sessions_with_cart,
    count(*) filter (where s.checkout_starts > 0)   as sessions_with_checkout,
    count(*) filter (where s.converted)             as sessions_converted,

    count(*) filter (where s.abandoned_cart)        as carts_abandoned,
    count(*) filter (where s.abandoned_checkout)    as checkouts_abandoned,

    sum(s.add_to_carts)                         as add_to_cart_events,
    sum(s.checkout_starts)                      as checkout_start_events,

    cast(round(
        cast(count(*) filter (where s.abandoned_cart) as decimal(38, 4)) * 10000
        / nullif(count(*) filter (where s.add_to_carts > 0), 0), 0
    ) as integer)                               as cart_abandon_rate_bps,

    cast(round(
        cast(count(*) filter (where s.abandoned_checkout) as decimal(38, 4)) * 10000
        / nullif(count(*) filter (where s.checkout_starts > 0), 0), 0
    ) as integer)                               as checkout_abandon_rate_bps

from {{ ref('int_sessions_funnel') }} s
group by 1, 2, 3, 4, 5
