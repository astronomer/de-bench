{{
    config(
        materialized='table'
    )
}}

{#-
    The retail calendar, one row per day.

    Copperline runs a 4-5-4 fiscal calendar and FY2023 has a fifty-third week.
    That is the whole reason this dimension exists rather than a date_trunc:
    subtract 364 days to find "the same day last year" and you are wrong for
    every day after the fifty-third week, by one week, silently. `comp_date_ly`
    is on the source row, computed against the calendar, and it is the only
    correct answer. docs/retail-calendar.md holds the rule.

    The trading-day flags come from the home market. A market-specific trading
    day is a question for stg_reference__market_calendar, which is per market;
    this dimension is one row per date and cannot answer it.
-#}

with fiscal as (

    select * from {{ ref('stg_reference__fiscal_calendar') }}

),

home_market as (

    select calendar_date, is_trading_day, is_holiday, holiday_name
    from {{ ref('stg_reference__market_calendar') }}
    where market_code = 'US'

)

select
    f.cal_date                                  as date_key,
    f.cal_date,

    f.fiscal_year,
    f.fiscal_quarter,
    f.fiscal_period,
    f.fiscal_month,
    f.fiscal_week,
    f.day_of_fiscal_week,
    f.week_start                                as fiscal_week_start,
    f.week_end                                  as fiscal_week_end,
    f.is_53rd_week,

    f.comp_date_ly,
    f.comp_week_ly,

    extract(year from f.cal_date)               as calendar_year,
    extract(month from f.cal_date)              as calendar_month,
    extract(day from f.cal_date)                as calendar_day,
    date_trunc('month', f.cal_date)::date       as calendar_month_start,
    strftime(f.cal_date, '%Y-%m')               as calendar_month_key,
    dayname(f.cal_date)                         as day_name,
    isodow(f.cal_date)                          as iso_weekday,
    isodow(f.cal_date) in (6, 7)                as is_weekend,

    coalesce(h.is_trading_day, false)           as is_us_trading_day,
    coalesce(h.is_holiday, false)               as is_us_holiday,
    h.holiday_name                              as us_holiday_name,

    f.cal_date <= {{ ds() }}                    as is_past,
    f.cal_date = {{ ds() }}                     as is_today

from fiscal f
left join home_market h on h.calendar_date = f.cal_date
