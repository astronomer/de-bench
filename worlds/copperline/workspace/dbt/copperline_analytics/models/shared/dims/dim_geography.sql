{{
    config(
        materialized='table'
    )
}}

{#-
    Market, region and country, in one place.

    Three sources spell geography three ways: an order carries a market, a store
    carries a market and a region, and a lane carries a destination region and a
    country. This dimension is the grain they all reduce to — the market — with
    the regions inside it listed, so a model can go from any of the three to the
    others without a case statement.

    The market is the unit because it is what the calendar, the currency and the
    tax rules key on. A region is a sales-management line inside a market and
    moves more often than the market does.
-#}

with markets as (

    select
        market_code,
        count(*) filter (where is_trading_day)  as trading_days,
        count(*) filter (where is_holiday)      as holidays,
        min(calendar_date)                      as calendar_from,
        max(calendar_date)                      as calendar_to
    from {{ ref('stg_reference__market_calendar') }}
    group by 1

),

store_regions as (

    select
        market_code,
        count(distinct region_code)                             as region_count,
        count(*)                                                as store_count,
        string_agg(distinct region_code, ',' order by region_code) as regions,
        count(distinct tz_name)                                 as timezone_count
    from {{ ref('stg_store__stores') }}
    where is_current
    group by 1

),

market_currency as (

    select * from (
        values
            ('US', 'USD', 'US', 'CL-US'),
            ('CA', 'CAD', 'CA', 'CL-US'),
            ('GB', 'GBP', 'GB', 'CL-GB'),
            ('IE', 'EUR', 'IE', 'CL-IE'),
            ('DE', 'EUR', 'DE', 'CL-DE'),
            ('MX', 'MXN', 'MX', 'CL-MX'),
            ('BR', 'BRL', 'BR', 'CL-US'),
            ('PL', 'PLN', 'PL', 'CL-DE'),
            ('ID', 'IDR', 'ID', 'CL-US')
    ) as t (market_code, currency_code, country_code, entity_code)

)

select
    m.market_code                               as geography_key,
    m.market_code,
    c.country_code,
    c.currency_code,
    c.entity_code,

    coalesce(s.region_count, 0)                 as region_count,
    coalesce(s.store_count, 0)                  as store_count,
    coalesce(s.timezone_count, 0)               as timezone_count,
    s.regions,
    coalesce(s.store_count, 0) > 0              as has_stores,

    m.trading_days,
    m.holidays,
    m.calendar_from,
    m.calendar_to

from markets m
left join store_regions s on s.market_code = m.market_code
left join market_currency c on c.market_code = m.market_code
