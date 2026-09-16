{{
    config(
        materialized='view'
    )
}}

{#- Spend and attributed orders, by platform, campaign, market and day.

    The platforms restate, and a restatement arrives as a **second row** for the
    same day, campaign and channel rather than as an edit. The latest arrival
    wins here; `restated_at` says when it was rewritten. A reader comparing
    yesterday's number to the same number today has to expect it to have moved.

    Spend is in the platform's billing currency, not USD. int_fx_applied is
    where the conversion happens, and it carries fx_rate_ppm and fx_rate_date
    beside the result.
-#}

with ranked as (

    select
        *,
        row_number() over (
            partition by report_date, platform, campaign_id, market_code, channel
            order by coalesce(restated_at, loaded_at) desc
        ) as restatement_seq
    from {{ source('marketing', 'ads_spend_daily') }}

)

select
    {{ dbt_utils.surrogate_key(['report_date', 'platform', 'campaign_id', 'market_code', 'channel']) }} as ad_spend_key,
    report_date,
    platform,
    campaign_id,
    campaign_name,
    market_code,
    channel,
    currency_code,
    cast(impressions as bigint)                 as impressions,
    cast(clicks as bigint)                      as clicks,
    cast(spend_cents as bigint)                 as spend_cents,
    cast(attributed_orders as bigint)           as attributed_orders,
    cast(attributed_revenue_cents as bigint)    as attributed_revenue_cents,
    restated_at,
    restated_at is not null                     as is_restated,
    loaded_at
from ranked
where restatement_seq = 1
