{{ config(materialized='table') }}

{#-
    Email sends and what happened to them, by day and campaign.

    The address never leaves the hash. contracts/privacy.md.

    Campaign ids here and campaign ids on the ad feed are different namespaces —
    `LAR-` against `SOL-`, `TES-`, `BEA-` — and they do not join. A model that
    unions them on campaign_id is inventing a relationship, so this table stays
    separate from channel_roi_daily rather than being folded into it.
-#}

select
    e.event_date                                as date_key,
    e.event_date                                as ds,
    e.campaign_id,

    count(distinct e.send_id)                   as sends,
    count(distinct e.customer_ref)              as recipients,
    count(*) filter (where e.event_type = 'sent')       as sent_events,
    count(*) filter (where e.event_type = 'delivered')  as delivered_events,
    count(*) filter (where e.event_type = 'open')       as open_events,
    count(*) filter (where e.event_type = 'click')      as click_events,
    count(*) filter (where e.event_type = 'bounce')     as bounce_events,
    count(*) filter (where e.event_type = 'unsub')      as unsub_events,

    cast(round(
        cast(count(*) filter (where e.event_type = 'open') as decimal(38, 4)) * 10000
        / nullif(count(*) filter (where e.event_type = 'delivered'), 0), 0
    ) as integer)                               as open_rate_bps,

    cast(round(
        cast(count(*) filter (where e.event_type = 'click') as decimal(38, 4)) * 10000
        / nullif(count(*) filter (where e.event_type = 'open'), 0), 0
    ) as integer)                               as click_through_rate_bps,

    cast(round(
        cast(count(*) filter (where e.event_type = 'bounce') as decimal(38, 4)) * 10000
        / nullif(count(distinct e.send_id), 0), 0
    ) as integer)                               as bounce_rate_bps

from {{ ref('stg_marketing__email_events') }} e
group by 1, 2, 3
