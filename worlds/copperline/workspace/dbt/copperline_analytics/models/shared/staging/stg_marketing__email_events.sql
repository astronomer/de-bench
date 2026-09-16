{{
    config(
        materialized='view'
    )
}}

{#- Send, delivery and engagement events from the email service.

    `email_hash` is the only address that ever lands here and it stays a hash.
    Nothing in this project reverses it and nothing publishes it beside a name.
    contracts/privacy.md holds the rule.

    The campaign ids on this feed and the ones on the ad feed are different
    namespaces. They do not join, and a model that unions them on campaign_id is
    inventing a relationship.
-#}

select
    {{ dbt_utils.surrogate_key(['send_id', 'event_type']) }} as email_event_key,
    send_id,
    message_id,
    campaign_id,
    customer_ref,
    email_hash,
    event_type,
    event_time_utc,
    cast(event_time_utc as date)        as event_date
from {{ source('marketing', 'email_events') }}
