{{ config(materialized='table') }}

{#-
    Comparable store sales by day and region. contracts/comp_sales_daily.yml,
    consumer C-5, the morning flash.

    Two rules and both of them are easy to get wrong:

    * **The comparable day is on the calendar, not three hundred and sixty-four
      days back.** FY2023 has a fifty-third week, so subtracting 364 days lands
      on the wrong day for every date after it. `comp_date_ly` on dim_date is
      the answer.
    * **An acquired store enters comp thirteen full fiscal months after the
      acquisition close, not after its own opening.** Forty-four Northwave
      stores opened between 2011 and 2023; applying the opening-date rule puts
      every one of them into comp about a year early, which moves the headline
      by about four points and is wrong on every per-region vector.
      dim_store.first_comp_date applies both rules, once.

    docs/comp-store-policy.md is the source for both.

    The sales come from int_orders_enriched, not from commerce's store mart.
    Both teams need store sales by day and rule 2 says the definition moves to
    `int` rather than one mart reading the other's.
-#}

with store_days as (

    select
        o.order_date                            as ds,
        o.store_id,
        d.region_code                           as region,
        d.first_comp_date,
        d.is_acquired,
        sum(o.net_sales_cents)                  as net_sales_cents,
        count(*)                                as order_count
    from {{ ref('int_orders_enriched') }} o
    join {{ ref('dim_store') }} d on d.store_id = o.store_id
    where o.channel = 'store'
      and o.store_id is not null
    group by 1, 2, 3, 4, 5

),

comparable as (

    select
        sd.*,
        sd.ds >= sd.first_comp_date             as is_comparable
    from store_days sd

),

by_region_day as (

    select
        ds,
        region,
        sum(net_sales_cents) filter (where is_comparable)       as comp_sales_cents,
        count(distinct store_id) filter (where is_comparable)   as comp_store_count,
        sum(net_sales_cents)                                    as total_sales_cents,
        count(distinct store_id)                                as store_count,
        count(distinct store_id) filter (where is_acquired and is_comparable)
                                                                as acquired_comp_store_count
    from comparable
    group by 1, 2

),

calendar as (

    select cal_date, comp_date_ly, fiscal_year, fiscal_week, fiscal_month
    from {{ ref('dim_date') }}

)

select
    b.ds,
    b.region,
    c.fiscal_year,
    c.fiscal_week,
    c.fiscal_month,

    coalesce(b.comp_sales_cents, 0)             as comp_sales_cents,
    coalesce(ly.comp_sales_cents, 0)            as comp_sales_cents_ly,
    c.comp_date_ly,
    coalesce(b.comp_store_count, 0)             as comp_store_count,
    coalesce(ly.comp_store_count, 0)            as comp_store_count_ly,
    b.acquired_comp_store_count,

    b.total_sales_cents,
    b.store_count,

    coalesce(b.comp_sales_cents, 0) - coalesce(ly.comp_sales_cents, 0) as comp_change_cents,
    cast(round(
        (cast(coalesce(b.comp_sales_cents, 0) as decimal(38, 4)) - coalesce(ly.comp_sales_cents, 0))
        * 10000 / nullif(ly.comp_sales_cents, 0), 0
    ) as integer)                               as comp_change_bps

from by_region_day b
join calendar c on c.cal_date = b.ds
left join by_region_day ly on ly.ds = c.comp_date_ly and ly.region = b.region
