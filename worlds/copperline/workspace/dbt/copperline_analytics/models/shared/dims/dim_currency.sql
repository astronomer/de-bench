{{
    config(
        materialized='table'
    )
}}

{#-
    Currencies, with the rate as of `var('ds')`.

    Rates are parts per million against USD, as integers. A rate here is never a
    float and never a percentage: `fx_rate_ppm` of 1,085,000 means one unit is
    1.085 US dollars. Every fact that converts carries the rate it used and the
    date that rate came from, so this dimension is for describing a currency,
    not for converting with — a fact that joins here to find a rate has picked
    today's rate for a posting made two years ago.
-#}

with currencies as (

    select
        currency_code,
        count(*)                        as rate_days,
        min(rate_date)                  as first_rate_date,
        max(rate_date)                  as last_rate_date,
        min(fx_rate_ppm)                as min_rate_ppm,
        max(fx_rate_ppm)                as max_rate_ppm
    from {{ ref('stg_reference__fx_rates') }}
    group by 1

),

as_of as (

    select
        currency_code,
        fx_rate_ppm,
        rate_date,
        row_number() over (partition by currency_code order by rate_date desc) as recency
    from {{ ref('stg_reference__fx_rates') }}
    where rate_date <= {{ ds() }}

),

markets as (

    select currency_code, count(*) as market_count, string_agg(market_code, ',' order by market_code) as markets
    from {{ ref('dim_geography') }}
    group by 1

)

select
    c.currency_code                             as currency_key,
    c.currency_code,
    c.currency_code = 'USD'                     as is_base_currency,

    a.fx_rate_ppm                               as fx_rate_ppm_as_of,
    a.rate_date                                 as fx_rate_date_as_of,

    c.rate_days,
    c.first_rate_date,
    c.last_rate_date,
    c.min_rate_ppm,
    c.max_rate_ppm,

    coalesce(m.market_count, 0)                 as market_count,
    m.markets,
    coalesce(m.market_count, 0) > 0             as is_trading_currency

from currencies c
left join as_of a on a.currency_code = c.currency_code and a.recency = 1
left join markets m on m.currency_code = c.currency_code
