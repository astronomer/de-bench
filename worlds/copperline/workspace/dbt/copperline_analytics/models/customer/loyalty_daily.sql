{{ config(materialized='table') }}

{#-
    Loyalty members' trading, by day.

    There is no loyalty points ledger. The only trace of the programme in the
    warehouse is `loyalty_id` on the order, so this table reports what members
    bought and cannot report what they earned or redeemed. A task that wants
    points authors the feed first; until then `points_earned` does not exist
    here rather than existing as a zero.

    The denominator matters and it is stated on the row: `order_count`,
    `member_orders`, `trade_orders` and `guest_orders`, so nobody has to guess
    what a rate was divided by. A loyalty share against all orders and a
    loyalty share against consumer orders are different numbers and both get
    called "loyalty share".
-#}

select
    o.order_date                                as date_key,
    o.order_date                                as ds,
    o.channel                                   as channel_key,
    o.market_code                               as geography_key,

    count(*)                                                        as order_count,
    count(*) filter (where o.loyalty_id is not null)                as member_orders,
    count(*) filter (where o.customer_ref is not null)              as trade_orders,
    count(*) filter (where o.loyalty_id is null and o.customer_ref is null) as guest_orders,
    count(distinct o.loyalty_id)                                    as distinct_members,

    sum(o.net_sales_cents)                                          as net_sales_cents,
    sum(o.net_sales_cents) filter (where o.loyalty_id is not null)  as member_net_sales_cents,
    sum(o.booked_cents) filter (where o.loyalty_id is not null)     as member_booked_cents,
    sum(o.net_units) filter (where o.loyalty_id is not null)        as member_units,

    cast(round(
        cast(count(*) filter (where o.loyalty_id is not null) as decimal(38, 4)) * 10000
        / nullif(count(*), 0), 0
    ) as integer)                                                   as member_order_share_bps,

    cast(round(
        cast(sum(o.net_sales_cents) filter (where o.loyalty_id is not null) as decimal(38, 4))
        / nullif(count(*) filter (where o.loyalty_id is not null), 0), 0
    ) as bigint)                                                    as member_average_order_cents

from {{ ref('int_orders_enriched') }} o
group by 1, 2, 3, 4
