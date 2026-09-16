{{ config(materialized='table') }}

{#-
    The income statement by cost centre and fiscal month.

    A cost centre here is a merchandising department, which is what the cost
    complement and the category tree both key on. There is no separate
    cost-centre master and no payroll feed, so this covers sales, cost of sales
    and margin, and stops there. Operating expense is not in the warehouse, and
    neither is closing stock: the valuation is supply's mart and rule 2 says
    finance does not read it. Putting a stock line on the P&L is a conversation
    with supply about moving the valuation into `int`, not a join.
-#}

with margin as (

    select
        fiscal_month,
        fiscal_year,
        fiscal_period,
        cost_center_key,
        sum(net_sales_cents)                    as net_sales_cents,
        sum(cost_cents)                         as cost_of_sales_cents,
        sum(merch_margin_cents)                 as merch_margin_cents,
        sum(booked_cents)                       as booked_cents,
        sum(line_count)                         as line_count,
        sum(lines_without_cost)                 as lines_without_cost
    from {{ ref('category_margin') }}
    group by 1, 2, 3, 4

)

select
    m.fiscal_month,
    m.fiscal_year,
    m.fiscal_period,
    m.cost_center_key,
    c.current_cost_complement_bps,

    m.booked_cents,
    m.net_sales_cents,
    m.cost_of_sales_cents,
    m.merch_margin_cents,
    m.line_count,
    m.lines_without_cost,

    cast(round(
        cast(m.merch_margin_cents as decimal(38, 4)) * 10000 / nullif(m.net_sales_cents, 0), 0
    ) as integer)                               as margin_rate_bps,

    cl.is_closed                                as period_is_closed

from margin m
left join {{ ref('dim_cost_center') }} c on c.dept_code = m.cost_center_key
left join {{ ref('int_closed_month') }} cl on cl.fiscal_month = m.fiscal_month
