{{ config(materialized='table') }}

{#-
    The board's weekly revenue line.

    Reads this team's own comp table and its own rollup. Fiscal weeks, never ISO
    weeks: the board reads a 4-5-4 calendar and a deck cut on ISO weeks drifts
    from the close.
-#}

with weeks as (

    select
        fiscal_week_start                       as week_start,
        fiscal_year,
        fiscal_week,
        fiscal_month,
        min(cal_date)                           as first_day,
        max(cal_date)                           as last_day
    from {{ ref('dim_date') }}
    group by 1, 2, 3, 4

),

comp as (

    select
        w.week_start,
        c.region,
        sum(c.comp_sales_cents)                 as comp_sales_cents,
        sum(c.comp_sales_cents_ly)              as comp_sales_cents_ly,
        max(c.comp_store_count)                 as comp_store_count
    from {{ ref('comp_sales_daily') }} c
    join weeks w on c.ds between w.first_day and w.last_day
    group by 1, 2

),

rollup as (

    select
        fiscal_month,
        entity,
        sum(reported_cents)                     as reported_cents,
        sum(recognized_cents)                   as recognized_cents,
        max(active_accounts)                    as active_accounts
    from {{ ref('account_rollup') }}
    group by 1, 2

)

select
    w.week_start,
    w.fiscal_year,
    w.fiscal_week,
    w.fiscal_month,
    c.region,

    coalesce(c.comp_sales_cents, 0)             as comp_sales_cents,
    coalesce(c.comp_sales_cents_ly, 0)          as comp_sales_cents_ly,
    coalesce(c.comp_store_count, 0)             as comp_store_count,
    coalesce(c.comp_sales_cents, 0) - coalesce(c.comp_sales_cents_ly, 0) as comp_change_cents,

    r.reported_cents                            as month_reported_cents,
    r.recognized_cents                          as month_recognized_cents,
    r.active_accounts                           as month_active_accounts,

    cl.is_closed                                as month_is_closed

from weeks w
left join comp c on c.week_start = w.week_start
left join rollup r on r.fiscal_month = w.fiscal_month and r.entity = 'CL-US'
left join {{ ref('int_closed_month') }} cl on cl.fiscal_month = w.fiscal_month
where w.week_start <= {{ ds() }}
