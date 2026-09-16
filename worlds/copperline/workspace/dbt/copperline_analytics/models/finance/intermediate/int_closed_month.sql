{{ config(materialized='view') }}

{#-
    Which fiscal months are closed, and when they closed.

    There is no close-calendar feed. The rule is in docs/finance-policy.md: a
    period closes on the **fifth business day** of the following period, counted
    against the US market calendar. It is computed here so that no mart has to
    count business days, and so that changing the rule is one edit.

    A month whose close date has not arrived is open. An open month's numbers
    move; a closed month's do not, and a model that restates a closed month is a
    restatement and goes through docs/change-management.md.
-#}

with months as (

    select
        fiscal_month,
        fiscal_year,
        min(fiscal_period)  as fiscal_period,
        min(cal_date)       as month_start,
        max(cal_date)       as month_end
    from {{ ref('dim_date') }}
    group by 1, 2

),

trading_days as (

    select
        calendar_date,
        row_number() over (order by calendar_date) as trading_day_seq
    from {{ ref('stg_reference__market_calendar') }}
    where market_code = 'US'
      and is_trading_day

),

close_dates as (

    select
        m.fiscal_month,
        m.fiscal_year,
        m.fiscal_period,
        m.month_start,
        m.month_end,
        min(t.calendar_date) filter (
            where t.trading_day_seq = (
                select min(t2.trading_day_seq) + 4
                from trading_days t2
                where t2.calendar_date > m.month_end
            )
        )                                       as close_date
    from months m
    cross join trading_days t
    where t.calendar_date > m.month_end
      and t.calendar_date <= m.month_end + 20
    group by 1, 2, 3, 4, 5

)

select
    fiscal_month,
    fiscal_year,
    fiscal_period,
    month_start,
    month_end,
    close_date,
    close_date <= {{ ds() }}                    as is_closed,
    case when close_date <= {{ ds() }} then 'closed' else 'open' end as close_status,
    date_diff('day', {{ ds() }}, close_date)    as days_until_close
from close_dates
