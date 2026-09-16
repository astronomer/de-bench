{{ config(materialized='table') }}

{#-
    Sales by day and channel. The four-channel view.

    The comparison this table exists for — store against web against
    marketplace — only works because store and web orders land in the same fact.
    Splitting them into two team projects would have put the comparison across
    an ownership boundary, which is why the order spine is one team's.
-#}

select
    o.order_date                                as date_key,
    o.order_date                                as ds,
    o.channel                                   as channel_key,
    o.channel,
    o.market_code                               as geography_key,
    o.market_code,

    count(*)                                    as order_count,
    count(distinct o.customer_ref) filter (where o.customer_ref is not null) as trade_account_count,
    count(distinct o.loyalty_id) filter (where o.loyalty_id is not null)     as loyalty_member_count,
    count(*) filter (where o.customer_ref is null and o.loyalty_id is null)  as guest_order_count,

    sum(o.ordered_units)                        as ordered_units,
    sum(o.net_units)                            as net_units,

    sum(o.booked_cents)                         as booked_cents,
    sum(o.booked_base_cents)                    as booked_base_cents,
    sum(o.order_discount_cents)                 as order_discount_cents,
    sum(o.discount_cents)                       as discount_cents,
    sum(o.tax_cents)                            as tax_cents,
    sum(o.shipping_cents)                       as shipping_cents,
    sum(o.returned_cents)                       as returned_cents,
    sum(o.net_sales_cents)                      as net_sales_cents,
    sum(o.gift_card_applied_cents)              as gift_card_applied_cents,

    count(*) filter (where o.is_paid)           as paid_order_count,
    count(*) filter (where o.has_return)        as orders_with_return,
    count(*) filter (where o.is_shadow_quarter) as shadow_quarter_orders,

    cast(round(cast(sum(o.booked_cents) as decimal(38, 4)) / nullif(count(*), 0), 0) as bigint)
                                                as average_order_booked_cents

from {{ ref('fct_order') }} o
group by 1, 2, 3, 4, 5, 6
