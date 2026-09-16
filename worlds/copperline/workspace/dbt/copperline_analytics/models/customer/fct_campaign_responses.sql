{{ config(materialized='table') }}

{#-
    Email campaign responses, one row per recipient per campaign.

    The join back to the customer is `customer_ref`, which on this feed is
    always a current-format id. `responded` is an open or a click; a delivery is
    not a response and a bounce is the opposite of one.

    There is no link from an email to an order. The email service does not stamp
    one and the order does not carry a campaign, so attribution from email to
    revenue is not available and is not invented here.
-#}

with events as (

    select
        campaign_id,
        customer_ref,
        count(*)                                                as event_count,
        count(*) filter (where event_type = 'sent')             as sent_events,
        count(*) filter (where event_type = 'delivered')        as delivered_events,
        count(*) filter (where event_type = 'open')             as open_events,
        count(*) filter (where event_type = 'click')            as click_events,
        count(*) filter (where event_type = 'bounce')           as bounce_events,
        count(*) filter (where event_type = 'unsub')            as unsub_events,
        min(event_date)                                         as first_event_date,
        max(event_date)                                         as last_event_date,
        count(distinct send_id)                                 as send_count
    from {{ ref('stg_marketing__email_events') }}
    group by 1, 2

)

select
    {{ dbt_utils.surrogate_key(['e.campaign_id', 'e.customer_ref']) }} as campaign_response_key,
    e.campaign_id,
    e.customer_ref                              as customer_id,
    c.account_name,
    c.region_code,
    c.tier,
    c.source_book,

    e.first_event_date                          as date_key,
    e.first_event_date,
    e.last_event_date,

    e.send_count,
    e.event_count,
    e.sent_events,
    e.delivered_events,
    e.open_events,
    e.click_events,
    e.bounce_events,
    e.unsub_events,

    e.open_events > 0 or e.click_events > 0     as responded,
    e.click_events > 0                          as clicked,
    e.bounce_events > 0                         as bounced,
    e.unsub_events > 0                          as unsubscribed,
    c.customer_key is not null                  as customer_resolves

from events e
left join {{ ref('dim_customer') }} c on c.customer_key = e.customer_ref
