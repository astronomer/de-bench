{{ config(materialized='table') }}

{#-
    The feature table. contracts/feature_customer_daily.yml, consumer C-9.

    Point-in-time by construction: every column is "as of `ds`", computed from
    events on or before `ds`, with no column that could only be known later. A
    feature that leaks the future scores beautifully in training and does
    nothing in production, and this table has done that once already.

    The window is the trailing fourteen days. The whole history at this grain is
    a row per account per day for the life of the estate, which nothing reads
    and everything waits for.
-#}

with spine as (

    select cal_date as ds
    from {{ ref('dim_date') }}
    where cal_date between {{ ds_minus(14) }} and {{ ds() }}

),

customers as (

    select customer_key, customer_id, region_code, status, source_book, tier
    from {{ ref('dim_customer') }}
    where source_id_shape = 'current'

),

orders as (

    select
        o.customer_ref                          as customer_key,
        o.order_date,
        o.booked_cents,
        o.net_sales_cents
    from {{ ref('int_orders_enriched') }} o
    where o.customer_ref is not null
      and o.order_date > {{ ds_minus(120) }}

)

select
    s.ds,
    c.customer_id,
    coalesce(c.region_code, 'unknown')          as region_as_of,
    c.status                                    as status_as_of,
    c.source_book,
    c.tier                                      as tier_as_of,

    count(o.order_date) filter (where o.order_date between s.ds - 6 and s.ds)   as orders_7d,
    count(o.order_date) filter (where o.order_date between s.ds - 29 and s.ds)  as orders_30d,
    count(o.order_date) filter (where o.order_date between s.ds - 89 and s.ds)  as orders_90d,

    coalesce(sum(o.net_sales_cents) filter (where o.order_date <= s.ds), 0)     as net_sales_cents,
    coalesce(sum(o.net_sales_cents) filter (where o.order_date between s.ds - 29 and s.ds), 0)
                                                                                as net_sales_cents_30d,
    coalesce(sum(o.booked_cents) filter (where o.order_date between s.ds - 29 and s.ds), 0)
                                                                                as booked_cents_30d,

    max(o.order_date) filter (where o.order_date <= s.ds)                       as last_order_date,
    date_diff('day', max(o.order_date) filter (where o.order_date <= s.ds), s.ds) as days_since_last_order

from spine s
cross join customers c
left join orders o on o.customer_key = c.customer_key and o.order_date <= s.ds
group by 1, 2, 3, 4, 5, 6
