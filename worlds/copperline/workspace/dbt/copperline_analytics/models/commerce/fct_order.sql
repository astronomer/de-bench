{{ config(materialized='table') }}

{#-
    One row per order. The commerce team's published order spine.

    Everything about the order that a consumer asks for, with the dimensions
    keyed. It is a table and not a view because half the warehouse joins it.
-#}

with orders as (

    select * from {{ ref('int_orders_enriched') }}

),

payments as (

    select
        order_id,
        processor,
        settled_net_cents,
        is_shadow_quarter,
        halcyon_match_is_ambiguous,
        seen_by_meridian,
        seen_by_halcyon
    from {{ ref('int_payment_matched') }}

)

select
    o.order_id                                  as order_key,
    o.order_id,
    o.order_ref,

    o.order_date                                as date_key,
    o.order_date,
    o.channel                                   as channel_key,
    o.channel,
    o.market_code                               as geography_key,
    o.market_code,
    o.store_id                                  as store_key,
    o.store_id,
    o.currency_code                             as currency_key,
    o.currency_code,

    o.brand,
    o.source_system,
    o.order_status,
    o.customer_ref,
    o.loyalty_id,
    o.event_time_utc,
    o.event_time_local,

    o.line_count,
    o.ordered_units,
    o.net_units,

    -- booked_cents never moves, including for a refund.
    o.booked_cents,
    o.booked_base_cents,
    o.order_discount_cents,
    o.discount_cents,
    o.tax_cents,
    o.shipping_cents,
    o.grand_total_cents,
    o.gift_card_applied_cents,
    o.returned_cents,
    o.net_sales_cents,

    o.fx_rate_ppm,
    o.fx_rate_date,
    o.currency_was_defaulted,

    coalesce(p.processor, 'none')               as processor,
    coalesce(p.settled_net_cents, 0)            as settled_net_cents,
    coalesce(p.is_shadow_quarter, false)        as is_shadow_quarter,
    coalesce(p.halcyon_match_is_ambiguous, false) as payment_match_is_ambiguous,
    o.is_paid,
    o.has_return,

    o.updated_at,
    o.loaded_at

from orders o
left join payments p on p.order_id = o.order_id
