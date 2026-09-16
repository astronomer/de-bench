{{ config(materialized='table') }}

{#-
    Margin by category and month.

    **Finance does not rebuild revenue or cost of goods from source.** Net sales
    come from int_net_sales_lines and landed cost from int_order_lines_costed,
    which are the same two models commerce's margin numbers come from. That is
    the load-bearing rule of the architecture: the alternative is not one right
    number and one wrong one, it is two right ones that disagree and both of
    which their owner can defend.

    **The one thing this model reads across a boundary is
    marts.order_economics**, for the channel and the booked total. The stated
    rule says it should not — projects/platform/README.md rule 2 — and it has
    read it since `fin_margin_daily` was written. docs/lineage.md carries the
    row. The fix is to take the channel onto the shared line model, where it
    already nearly is, and nobody has scheduled it.

    `merch_margin_cents` is a reserved name: net sales less landed cost. Lines
    with no stated cost are excluded from the margin rate and counted in
    `lines_without_cost`, because a line with an unknown cost has an unknown
    margin and not a full one.
-#}

with lines as (

    select
        n.order_line_key,
        n.order_id,
        n.order_date,
        n.sku,
        n.net_sales_cents,
        c.extended_cost_cents,
        c.cost_is_stated,
        case
            when c.cost_is_stated then n.net_sales_cents - c.extended_cost_cents
        end                                     as merch_margin_cents
    from {{ ref('int_net_sales_lines') }} n
    join {{ ref('int_order_lines_costed') }} c on c.order_line_key = n.order_line_key

),

products as (

    select sku, category_id, dept_code from {{ ref('dim_product') }}

),

economics as (

    select order_id, channel, booked_cents
    from {{ ref('order_economics') }}

),

calendar as (

    select cal_date, fiscal_month, fiscal_year, fiscal_period from {{ ref('dim_date') }}

)

select
    cal.fiscal_month,
    cal.fiscal_year,
    cal.fiscal_period,
    coalesce(p.dept_code, 'UNASSIGNED')         as cost_center_key,
    p.category_id,
    e.channel                                   as channel_key,

    count(*)                                            as line_count,
    count(distinct l.order_id)                          as order_count,
    count(*) filter (where not l.cost_is_stated)        as lines_without_cost,

    sum(l.net_sales_cents)                              as net_sales_cents,
    sum(l.extended_cost_cents)                          as cost_cents,
    sum(l.merch_margin_cents)                           as merch_margin_cents,
    sum(l.net_sales_cents) filter (where l.cost_is_stated) as costed_net_sales_cents,
    sum(e.booked_cents)                                 as booked_cents,

    cast(round(
        cast(sum(l.merch_margin_cents) as decimal(38, 4)) * 10000
        / nullif(sum(l.net_sales_cents) filter (where l.cost_is_stated), 0), 0
    ) as integer)                                       as margin_rate_bps

from lines l
join calendar cal on cal.cal_date = l.order_date
left join products p on p.sku = l.sku
left join economics e on e.order_id = l.order_id
group by 1, 2, 3, 4, 5, 6
