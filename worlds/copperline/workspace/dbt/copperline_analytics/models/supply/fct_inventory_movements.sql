{{ config(materialized='table') }}

{#-
    One row per warehouse movement, keyed to the product where it can be.

    `reference_id` cites the document that caused the movement and it joins
    nothing: the references were minted against a key range the order book never
    used. They are kept because they are what the warehouse system shows on its
    own screens, and `reference_resolves` is false on all of them so that nobody
    spends an afternoon finding out again.
-#}

select
    m.movement_id                               as movement_key,
    m.movement_id,
    m.occurred_date                             as date_key,
    m.occurred_date,
    m.occurred_at,

    m.sku                                       as merch_sku,
    x.catalog_sku,
    x.product_key,
    coalesce(x.dept_code, x.catalog_dept_code)  as cost_center_key,

    m.from_location,
    m.to_location,
    coalesce(m.to_location, m.from_location)    as warehouse_key,
    m.movement_type,
    m.reference_type,
    m.reference_id,
    m.operator_id,

    m.qty,
    m.qty_abs,
    m.qty < 0                                   as is_outbound,

    x.is_matched                                as product_resolves,
    false                                       as reference_resolves,
    m.loaded_at

from {{ ref('stg_inventory__wms_movements') }} m
left join {{ ref('int_sku_crosswalk') }} x on x.merch_sku = m.sku
