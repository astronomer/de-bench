{{ config(materialized='view') }}

{#-
    A session, with the campaign that brought it and the order it produced.

    int_sessions_funnel is the shared definition of a session. This model adds
    growth's own question on top: which paid campaign, if any, gets the credit.

    The rule here is **last non-direct touch in the session**, and it is growth's
    rule rather than a company one, which is why it lives in this project and
    not in the shared layer. Finance attributes on the order and gets a
    different number; that is fine and it is why the two are never compared
    without saying which rule was used.

    A session with no UTM at all is `direct`. Sixty per cent of sessions are,
    and a channel report that quietly drops them overstates every paid channel.
-#}

with sessions as (

    select * from {{ ref('int_sessions_funnel') }}

),

last_touch as (

    select
        session_id,
        utm_source,
        utm_medium,
        utm_campaign,
        row_number() over (partition by session_id order by event_time_utc desc, event_id desc) as touch_seq
    from {{ ref('stg_ecommerce__web_events') }}
    where utm_source is not null

),

orders as (

    select order_id, order_date, channel, market_code, booked_cents, net_sales_cents
    from {{ ref('int_orders_enriched') }}

)

select
    s.session_id,
    s.session_date,
    s.anonymous_id,
    s.customer_ref,
    s.device,
    s.country_code,

    coalesce(l.utm_source, 'direct')            as attributed_source,
    l.utm_medium                                as attributed_medium,
    l.utm_campaign                              as attributed_campaign,
    l.utm_source is null                        as is_direct,
    s.acquisition_source                        as first_touch_source,
    coalesce(l.utm_source, 'direct') <> s.acquisition_source as touch_changed_within_session,

    s.funnel_stage,
    s.funnel_stage_name,
    s.event_count,
    s.session_seconds,
    s.converted,

    s.order_id,
    o.order_date,
    o.channel                                   as order_channel,
    o.market_code,
    coalesce(o.booked_cents, 0)                 as booked_cents,
    coalesce(o.net_sales_cents, 0)              as net_sales_cents

from sessions s
left join last_touch l on l.session_id = s.session_id and l.touch_seq = 1
left join orders o on o.order_id = s.order_id
