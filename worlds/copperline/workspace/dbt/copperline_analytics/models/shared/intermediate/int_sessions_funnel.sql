{{
    config(
        materialized='table'
    )
}}

{#-
    One row per web session, with the furthest step it reached.

    **Session-to-order stitching defines conversion rate for the whole company**,
    so it is one model here and not two in two marts. Growth reads it for the
    funnel and customer reads it for the 360; when it lived in both, the two
    published conversion rates that differed by about a fifth and each team could
    defend its own.

    The ladder is fixed and ordered: view the site, view a product, add to cart,
    start checkout, purchase. `funnel_stage` is the highest rung the session
    reached, as an integer so it sorts, with `funnel_stage_name` beside it for
    reading. A session that adds to cart without ever viewing a product still
    counts as having reached the cart — the ladder is a high-water mark, not a
    path.

    Attribution is the first touch **in the session**: the first non-null
    `utm_source` the session emitted. Cross-session attribution is a different
    question and int_touch_ordered in models/growth answers it.

    There is no clickstream from the marketplace. A marketplace order has no
    session and never will; any funnel that divides by all orders is wrong.
-#}

with events as (

    select *
    from {{ ref('stg_ecommerce__web_events') }}

),

first_touch as (

    select
        session_id,
        utm_source,
        utm_medium,
        utm_campaign,
        row_number() over (partition by session_id order by event_time_utc, event_id) as touch_seq
    from events
    where utm_source is not null

),

rolled as (

    select
        session_id,
        min(event_time_utc)                                             as session_started_at,
        max(event_time_utc)                                             as session_ended_at,
        cast(min(event_time_utc) as date)                               as session_date,
        count(*)                                                        as event_count,
        count(distinct page_path)                                       as distinct_pages,
        count(distinct sku) filter (where sku is not null)              as distinct_skus,
        max(anonymous_id)                                               as anonymous_id,
        max(customer_ref)                                               as customer_ref,
        max(device)                                                     as device,
        max(country_code)                                               as country_code,
        max(order_id)                                                   as order_id,

        count(*) filter (where event_name = 'page_view')                as page_views,
        count(*) filter (where event_name = 'product_view')             as product_views,
        count(*) filter (where event_name = 'add_to_cart')              as add_to_carts,
        count(*) filter (where event_name = 'checkout_start')           as checkout_starts,
        count(*) filter (where event_name = 'purchase')                 as purchases

    from events
    group by 1

)

select
    r.session_id,
    r.anonymous_id,
    r.customer_ref,
    r.session_date,
    r.session_started_at,
    r.session_ended_at,
    date_diff('second', r.session_started_at, r.session_ended_at) as session_seconds,

    r.device,
    r.country_code,

    t.utm_source,
    t.utm_medium,
    t.utm_campaign,
    coalesce(t.utm_source, 'direct')            as acquisition_source,

    r.event_count,
    r.distinct_pages,
    r.distinct_skus,
    r.page_views,
    r.product_views,
    r.add_to_carts,
    r.checkout_starts,
    r.purchases,

    r.order_id,
    r.order_id is not null                      as converted,

    case
        when r.purchases > 0        then 5
        when r.checkout_starts > 0  then 4
        when r.add_to_carts > 0     then 3
        when r.product_views > 0    then 2
        else 1
    end                                         as funnel_stage,

    case
        when r.purchases > 0        then 'purchase'
        when r.checkout_starts > 0  then 'checkout'
        when r.add_to_carts > 0     then 'cart'
        when r.product_views > 0    then 'product'
        else 'visit'
    end                                         as funnel_stage_name,

    -- A cart that never reached checkout. cart_abandon_daily counts these.
    r.add_to_carts > 0 and r.checkout_starts = 0    as abandoned_cart,
    r.checkout_starts > 0 and r.purchases = 0       as abandoned_checkout

from rolled r
left join first_touch t on t.session_id = r.session_id and t.touch_seq = 1
