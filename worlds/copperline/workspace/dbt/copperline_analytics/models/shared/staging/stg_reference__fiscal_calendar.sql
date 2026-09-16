{{
    config(
        materialized='view'
    )
}}

{#- The 4-5-4 retail calendar.

    `comp_date_ly` is the comparable day a year back and it is on the row
    already. Nothing computes it by subtracting 364 days: FY2023 has a
    fifty-third week and the subtraction is wrong for every day after it.
    docs/retail-calendar.md holds the rule.
-#}

select
    cal_date,
    fiscal_year,
    cast(fiscal_quarter as integer)     as fiscal_quarter,
    cast(fiscal_period as integer)      as fiscal_period,
    cast(fiscal_week as integer)        as fiscal_week,
    cast(day_of_fiscal_week as integer) as day_of_fiscal_week,
    week_start,
    week_end,
    comp_date_ly,
    cast(comp_week_ly as integer)       as comp_week_ly,
    coalesce(is_53rd_week, false)       as is_53rd_week,
    fiscal_year || '-P' || lpad(cast(fiscal_period as varchar), 2, '0') as fiscal_month
from {{ source('reference', 'fiscal_calendar') }}
