{{
    config(
        materialized='view'
    )
}}

{#- The order spine. Everything commerce publishes starts here.

    Three things happen in this view and nowhere else:

    * `is_test` orders are dropped. They are staff transactions against the live
      OMS and they carry real money that nobody owes.
    * `deleted_at` rows are dropped. The OMS deletes rather than cancels, so a
      deleted order is gone upstream and must be gone here too.
    * `currency_code` is NULL on about half the rows and means the market's own
      currency. It is not filled in here — staging says what arrived. The
      currency lands on the order in int_orders_enriched, where the market
      table is available to join.
-#}

select
    order_id,
    order_ref,
    customer_ref                                as customer_ref,
    loyalty_id,
    brand,
    channel,
    store_id,
    market_code,
    order_status,
    source_system,
    event_time_utc,
    event_time_local,
    local_order_date                            as order_date,
    currency_code,
    fx_rate_ppm,
    cast(subtotal_cents as bigint)              as subtotal_cents,
    cast(order_discount_cents as bigint)        as order_discount_cents,
    cast(tax_cents as bigint)                   as tax_cents,
    cast(shipping_cents as bigint)              as shipping_cents,
    cast(grand_total_cents as bigint)           as grand_total_cents,
    cast(gift_card_applied_cents as bigint)     as gift_card_applied_cents,
    updated_at,
    loaded_at
from {{ source('sales', 'orders') }}
where not coalesce(is_test, false)
  and deleted_at is null
