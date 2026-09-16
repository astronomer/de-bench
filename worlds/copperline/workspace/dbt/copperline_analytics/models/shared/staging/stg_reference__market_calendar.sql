{{
    config(
        materialized='view'
    )
}}

{#- Trading days, holidays and whether a feed is expected, per market.

    `is_trading_day` and `feed_expected` are two different questions. A market
    can be shut and still send a file, and a market can be open on a day the
    feed is not expected. The freshness checks read `feed_expected`; the sales
    models read `is_trading_day`.
-#}

select
    market_code,
    calendar_date,
    coalesce(is_trading_day, false)     as is_trading_day,
    coalesce(feed_expected, false)      as feed_expected,
    holiday_name,
    holiday_name is not null            as is_holiday
from {{ source('reference', 'market_calendar') }}
