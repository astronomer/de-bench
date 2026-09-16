{{ config(materialized='table') }}

{#-
    One row per web session. Growth's published session table.
-#}

select
    a.session_id                                as session_key,
    a.session_id,
    a.session_date                              as date_key,
    a.session_date                              as ds,
    a.anonymous_id,
    a.customer_ref,
    a.device,
    coalesce(a.country_code, 'unknown')         as country_code,

    a.first_touch_source,
    a.attributed_source,
    a.attributed_medium,
    a.attributed_campaign,
    a.is_direct,
    a.touch_changed_within_session,

    a.funnel_stage,
    a.funnel_stage_name,
    a.event_count,
    a.session_seconds,

    a.converted,
    a.order_id,
    a.order_channel,
    a.market_code                               as geography_key,
    a.booked_cents,
    a.net_sales_cents

from {{ ref('int_session_attributed') }} a
