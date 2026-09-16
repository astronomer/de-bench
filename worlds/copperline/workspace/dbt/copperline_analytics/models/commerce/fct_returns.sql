{{ config(materialized='table') }}

{#-
    One row per return authorisation.
-#}

select
    r.rma_id                                    as return_key,
    r.rma_id,
    r.order_id                                  as order_key,
    r.order_line_key,
    r.order_id,
    r.order_line_id,
    r.line_no,

    r.initiated_date                            as date_key,
    r.initiated_date,
    r.received_date,
    r.order_date,
    r.channel                                   as channel_key,
    r.market_code                               as geography_key,
    r.store_id                                  as store_key,
    r.sku,
    p.product_key,
    p.dept_code                                 as cost_center_key,
    p.category_id,

    r.return_reason,
    r.return_channel,
    r.disposition,
    r.refund_method,
    r.restock_flag,

    r.returned_qty,
    r.ordered_qty,
    r.refund_cents,
    r.line_value_cents,

    r.days_to_return,
    r.days_in_transit,
    r.refunded_to_card,
    r.refunded_to_gift_card,
    r.refunded_to_store_credit,
    r.returns_to_stock,
    r.is_in_flight,
    r.line_not_found

from {{ ref('int_return_linked') }} r
left join {{ ref('dim_product') }} p on p.sku = r.sku
