{{
    config(
        materialized='view'
    )
}}

{#-
    Net sales at order-line grain.

    `net_sales_cents` is a reserved name. docs/semantic-definitions.md defines it
    as gross less REV-13 discounts and returns, at order-line grain, and this
    model is where it is computed. Four teams read it — commerce, finance,
    growth and customer — and none of them recomputes it.

    The grain is the whole definition. Summing this to order or day or category
    is fine. Computing the same idea at order grain and calling it the same name
    is not, because the discount allocation floors per line and the sum of the
    floors is not the floor of the sum.

    Returns are matched to the line they came off, by the order and the line
    ordinal. A return is netted on the day it was **initiated**, not the day it
    arrived at the warehouse: the customer's decision is the event, and matching
    on arrival moves revenue across a period close for reasons nobody can
    explain to an auditor. `returned_cents` is the whole refund, including the
    tax the customer got back, so it is netted against the tax-exclusive line
    only up to what the line was worth.
-#}

with discounted as (

    select *
    from {{ ref('int_order_lines_discounted') }}

),

returns_by_line as (

    select
        order_id,
        order_line_id,
        sum(refund_cents)               as refund_cents,
        sum(qty)                        as returned_qty,
        min(initiated_date)             as first_return_date,
        max(initiated_date)             as last_return_date,
        count(*)                        as return_count
    from {{ ref('stg_sales__returns') }}
    group by 1, 2

),

orders as (

    select
        order_id,
        order_status,
        brand,
        source_system,
        loyalty_id,
        customer_ref
    from {{ ref('stg_sales__orders') }}

)

select
    d.order_line_key,
    d.order_id,
    d.order_line_id,
    d.line_no,
    d.order_date,
    d.channel,
    d.market_code,
    d.store_id,
    d.sku,
    o.brand,
    o.source_system,
    o.order_status,
    o.customer_ref,
    o.loyalty_id,

    d.qty,
    coalesce(r.returned_qty, 0)                             as returned_qty,
    d.qty - coalesce(r.returned_qty, 0)                      as net_qty,

    d.line_total_cents                                       as gross_line_cents,
    d.line_discount_cents,
    d.header_discount_cents,
    d.total_discount_cents,
    d.discounted_line_cents,

    coalesce(r.refund_cents, 0)                              as refund_cents,

    -- A refund includes the tax that came back. The line here is tax-exclusive,
    -- so the netting is capped at what the line was worth. The tax side of the
    -- refund lands in marts.tax_daily, not here.
    least(coalesce(r.refund_cents, 0), d.discounted_line_cents) as returned_cents,

    d.discounted_line_cents
        - least(coalesce(r.refund_cents, 0), d.discounted_line_cents) as net_sales_cents,

    d.tax_cents,
    r.first_return_date,
    r.last_return_date,
    coalesce(r.return_count, 0)                              as return_count,
    r.order_id is not null                                   as has_return

from discounted d
join orders o on o.order_id = d.order_id
left join returns_by_line r
       on r.order_id = d.order_id
      and r.order_line_id = d.order_line_id
