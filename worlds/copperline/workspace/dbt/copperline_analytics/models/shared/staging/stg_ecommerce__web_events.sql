{{
    config(
        materialized='view'
    )
}}

{#- The clickstream.

    The collector delivers at least once, so a replay lands events that are
    already here. `event_id` is the deduplication key and the earliest arrival
    wins — a replayed copy carries a later `load_time` and the same everything
    else. The January 2026 replay is the reason this view exists in this shape;
    ops/incidents/2026-01-23-event-replay.md has the count.

    Nothing here is filtered by date. The mart decides its window.
-#}

with ranked as (

    select
        *,
        row_number() over (partition by event_id order by load_time) as arrival_seq
    from {{ source('ecommerce', 'web_events') }}

)

select
    event_id,
    session_id,
    anonymous_id,
    customer_ref,
    event_name,
    page_path,
    sku,
    order_id,
    utm_source,
    utm_medium,
    utm_campaign,
    device,
    country_code,
    producer,
    cast("partition" as integer)          as source_partition,
    cast("offset" as bigint)              as source_offset,
    event_time_utc,
    cast(event_time_utc as date)        as event_date,
    load_time
from ranked
where arrival_seq = 1
