{{ config(materialized='table') }}

{#-
    Spend against return, by day, platform and campaign.

    **This model reads marts.gmv_daily, which commerce owns.** That crosses an
    ownership boundary and the stated rule says it should not —
    projects/platform/README.md rule 2, cross-team logic moves to `int`. It has
    been like this since the marketplace launch, it is on docs/lineage.md as a
    `ref` row, and closing it means moving the marketplace return definition
    into the shared layer. Nobody has.

    Spend arrives in the platform's billing currency. It is converted here with
    the rate for the report date, and `fx_rate_ppm` and `fx_rate_date` ride
    along so the conversion can be checked from the row.

    `attributed_revenue_cents` is the **platform's** claim, not Copperline's
    measurement. The platforms each count a conversion their own way and the
    three of them together claim about a fifth more orders than exist. Growth's
    own number is `attributed_booked_cents`, from int_session_attributed.
-#}

with spend as (

    select
        a.report_date                           as ds,
        a.platform,
        a.campaign_id,
        a.campaign_name,
        a.market_code,
        a.channel,
        a.currency_code,
        sum(a.impressions)                      as impressions,
        sum(a.clicks)                           as clicks,
        sum(a.spend_cents)                      as spend_cents,
        sum(a.attributed_orders)                as platform_attributed_orders,
        sum(a.attributed_revenue_cents)         as platform_attributed_revenue_cents,
        max(a.restated_at)                      as last_restated_at
    from {{ ref('stg_marketing__ads_spend_daily') }} a
    group by 1, 2, 3, 4, 5, 6, 7

),

rates as (

    select rate_date, currency_code, fx_rate_ppm
    from {{ ref('stg_reference__fx_rates') }}

),

measured as (

    select
        s.session_date                          as ds,
        s.attributed_campaign                   as campaign_id,
        count(*)                                as attributed_sessions,
        count(*) filter (where s.converted)     as attributed_orders,
        sum(s.booked_cents)                     as attributed_booked_cents,
        sum(s.net_sales_cents)                  as attributed_net_sales_cents
    from {{ ref('int_session_attributed') }} s
    where s.attributed_campaign is not null
    group by 1, 2

),

marketplace as (

    select ds, sum(gmv_cents) as marketplace_gmv_cents, sum(commission_cents) as marketplace_commission_cents
    from {{ ref('gmv_daily') }}
    group by 1

)

select
    s.ds                                        as date_key,
    s.ds,
    s.platform,
    s.campaign_id,
    s.campaign_name,
    s.market_code                               as geography_key,
    s.channel,
    s.currency_code                             as currency_key,
    s.currency_code,

    s.impressions,
    s.clicks,
    s.spend_cents,
    coalesce(r.fx_rate_ppm, 1000000)            as fx_rate_ppm,
    coalesce(r.rate_date, s.ds)                 as fx_rate_date,
    {{ to_base_cents('s.spend_cents', 'coalesce(r.fx_rate_ppm, 1000000)') }} as spend_base_cents,

    s.platform_attributed_orders,
    s.platform_attributed_revenue_cents,

    coalesce(m.attributed_sessions, 0)          as measured_sessions,
    coalesce(m.attributed_orders, 0)            as measured_orders,
    coalesce(m.attributed_booked_cents, 0)      as measured_booked_cents,
    coalesce(m.attributed_net_sales_cents, 0)   as measured_net_sales_cents,

    mp.marketplace_gmv_cents,
    mp.marketplace_commission_cents,

    cast(round(
        cast(s.spend_cents as decimal(38, 4)) * 10000 / nullif(m.attributed_booked_cents, 0), 0
    ) as integer)                               as cost_of_sale_bps,

    cast(round(
        cast(s.spend_cents as decimal(38, 4)) / nullif(m.attributed_orders, 0), 0
    ) as bigint)                                as cost_per_order_cents,

    s.last_restated_at

from spend s
left join rates r on r.currency_code = s.currency_code and r.rate_date = s.ds
left join measured m on m.ds = s.ds and m.campaign_id = s.campaign_id
left join marketplace mp on mp.ds = s.ds
