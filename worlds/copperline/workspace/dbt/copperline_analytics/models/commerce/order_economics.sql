{{ config(materialized='table') }}

{#-
    The order, as merchandising reads it. contracts/order_economics.yml.

    Four reserved names live here and each one answers a different question:

    * `booked_cents`    what the order was worth on the order date. Never moves.
    * `net_sales_cents` gross less discounts and returns, summed from the lines.
    * `merch_margin_cents` net sales less landed cost.
    * The contract also pins the grain — one row per order — and the channel
      list.

    **The contract's channel list has three values and the order book has
    four.** `trade` was added after the contract was written; it is about six per
    cent of orders and about a third of revenue. Those orders are here, because
    a mart that silently drops a third of revenue is worse than a mart that
    fails its contract loudly. Closing the gap is an amendment under
    docs/change-management.md, and until somebody makes it,
    `plat_contracts_enforce` will keep saying so.

    Margin is NULL where the cost is not stated, never zero, and
    `lines_without_cost` says how many lines were unpriced.
-#}

with orders as (

    select * from {{ ref('fct_order') }}

),

line_rollup as (

    select
        order_id,
        sum(net_sales_cents)                        as net_sales_cents,
        sum(extended_cost_cents)                    as cost_cents,
        sum(merch_margin_cents)                     as merch_margin_cents,
        count(*)                                    as line_count,
        count(*) filter (where not cost_is_stated)  as lines_without_cost,
        count(*) filter (where is_catalogued)       as catalogued_lines,
        count(distinct cost_center_key)             as department_count
    from {{ ref('fct_order_line') }}
    group by 1

)

select
    o.order_id,
    o.order_date,
    o.channel,
    o.market_code,
    o.store_key                                 as store_id,
    o.currency_code,
    o.brand,
    o.customer_ref,

    o.booked_cents,
    o.booked_base_cents,
    o.order_discount_cents,
    coalesce(l.net_sales_cents, 0)              as net_sales_cents,
    coalesce(l.cost_cents, 0)                   as cost_cents,
    l.merch_margin_cents,

    o.tax_cents,
    o.shipping_cents,
    o.grand_total_cents,
    o.gift_card_applied_cents,
    o.returned_cents,

    o.fx_rate_ppm,
    o.fx_rate_date,

    coalesce(l.line_count, 0)                   as line_count,
    coalesce(l.lines_without_cost, 0)           as lines_without_cost,
    coalesce(l.catalogued_lines, 0)             as catalogued_lines,
    coalesce(l.department_count, 0)             as department_count,

    o.processor,
    o.settled_net_cents,
    o.is_paid,
    o.has_return,
    o.is_shadow_quarter

from orders o
left join line_rollup l on l.order_id = o.order_id
