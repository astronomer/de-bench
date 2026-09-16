{{ config(materialized='table') }}

{#-
    The feature table. contracts/feature_customer_daily.yml, consumer C-9.

    Point-in-time by construction: every column is "as of `ds`", computed from
    events on or before `ds`, with no column that could only be known later. A
    feature that leaks the future scores beautifully in training and does
    nothing in production, and this table has done that once already.

    **The money is netted against the returns initiated by the row's own date.**
    `net_sales_cents` on `int_orders_enriched` is net of every return ever
    matched to the order, whenever the customer asked for it — the definition in
    docs/semantic-definitions.md carries no date bound and is not meant to.
    Summing it into a row dated before the customer asked puts a refund into a
    feature that could not have been known, which is §FS-1 of
    contracts/feature-store.md and the one thing this table is for. So the
    netting is redone here, at line grain, off `stg_sales__returns` and its
    `initiated_date`, and the bound is the row's `ds` rather than the run's.

    The cap against the line is the same one `int_net_sales_lines` applies: a
    refund carries back the tax the customer paid and the line here is
    tax-exclusive, so a line is netted down to zero and no further.

    The counts and `booked_cents_30d` need no bound. An order counts on its
    order date and `booked_cents` never moves again.

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
        o.booked_cents
    from {{ ref('int_orders_enriched') }} o
    where o.customer_ref is not null
      and o.order_date > {{ ds_minus(120) }}

),

lines as (

    select
        l.order_id,
        l.order_line_id,
        l.customer_ref                          as customer_key,
        l.order_date,
        l.discounted_line_cents
    from {{ ref('int_net_sales_lines') }} l
    where l.customer_ref is not null
      and l.order_date > {{ ds_minus(120) }}

),

refunds as (

    select order_id, order_line_id, initiated_date, refund_cents
    from {{ ref('stg_sales__returns') }}

),

-- One row per line per feature day: what the line was worth once the refunds
-- the customer had asked for by that day are off it, and no others.
line_as_of as (

    select
        s.ds,
        l.customer_key,
        l.order_date,
        l.discounted_line_cents
            - least(coalesce(sum(r.refund_cents), 0), l.discounted_line_cents)
                                                    as net_sales_cents
    from spine s
    join lines l on l.order_date <= s.ds
    left join refunds r
           on r.order_id = l.order_id
          and r.order_line_id = l.order_line_id
          and r.initiated_date <= s.ds
    group by s.ds, l.order_id, l.order_line_id, l.customer_key, l.order_date,
             l.discounted_line_cents

),

money as (

    select
        ds,
        customer_key,
        sum(net_sales_cents)                                                as net_sales_cents,
        coalesce(sum(net_sales_cents) filter (where order_date between ds - 29 and ds), 0)
                                                                            as net_sales_cents_30d
    from line_as_of
    group by 1, 2

),

behaviour as (

    select
        s.ds,
        o.customer_key,
        count(o.order_date) filter (where o.order_date between s.ds - 6 and s.ds)   as orders_7d,
        count(o.order_date) filter (where o.order_date between s.ds - 29 and s.ds)  as orders_30d,
        count(o.order_date) filter (where o.order_date between s.ds - 89 and s.ds)  as orders_90d,
        coalesce(sum(o.booked_cents) filter (where o.order_date between s.ds - 29 and s.ds), 0)
                                                                                    as booked_cents_30d,
        max(o.order_date)                                                           as last_order_date
    from spine s
    join orders o on o.order_date <= s.ds
    group by 1, 2

)

select
    s.ds,
    c.customer_id,
    coalesce(c.region_code, 'unknown')          as region_as_of,
    c.status                                    as status_as_of,
    c.source_book,
    c.tier                                      as tier_as_of,

    coalesce(b.orders_7d, 0)                    as orders_7d,
    coalesce(b.orders_30d, 0)                   as orders_30d,
    coalesce(b.orders_90d, 0)                   as orders_90d,

    coalesce(m.net_sales_cents, 0)              as net_sales_cents,
    coalesce(m.net_sales_cents_30d, 0)          as net_sales_cents_30d,
    coalesce(b.booked_cents_30d, 0)             as booked_cents_30d,

    b.last_order_date,
    date_diff('day', b.last_order_date, s.ds)   as days_since_last_order

from spine s
cross join customers c
left join behaviour b on b.ds = s.ds and b.customer_key = c.customer_key
left join money m on m.ds = s.ds and m.customer_key = c.customer_key
