{{
    config(
        materialized='view'
    )
}}

{#- One row per order line.

    `order_line_id` upstream is a line ordinal — L-01, L-02 — and repeats in
    every order. The key of this table is (order_id, line_no), and
    `order_line_key` is that pair spelled once so that nothing downstream has to
    remember it. Returns and promotion applications cite the ordinal, so
    `order_line_id` is kept as well.

    `line_total_cents` is tax-exclusive upstream: qty x unit price less the line
    discount. Tax sits beside it. That is checked, not assumed — see the grain
    test on int_net_sales_lines.
-#}

select
    {{ dbt_utils.surrogate_key(['order_id', 'line_no']) }}  as order_line_key,
    order_id,
    order_line_id,
    cast(line_no as integer)                    as line_no,
    sku,
    cast(qty as decimal(12, 3))                 as qty,
    cast(unit_price_cents as bigint)            as unit_price_cents,
    cast(unit_cost_cents as bigint)             as unit_cost_cents,
    cast(line_discount_cents as bigint)         as line_discount_cents,
    cast(tax_cents as bigint)                   as tax_cents,
    cast(line_total_cents as bigint)            as line_total_cents,
    tax_rate_id,
    fulfillment_type,
    line_status,
    source_system
from {{ source('sales', 'order_lines') }}
