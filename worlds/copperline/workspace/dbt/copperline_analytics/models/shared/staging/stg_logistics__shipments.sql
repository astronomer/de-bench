{{
    config(
        materialized='view'
    )
}}

{#- One row per shipment. Freight is not billed here; packages are. -#}

select
    shipment_id,
    order_id,
    carrier_code,
    service_level,
    lane_id,
    origin_dc,
    dest_zip3,
    dest_country,
    brand,
    shipment_status,
    cast(package_count as integer)      as package_count,
    cast(total_weight_g as bigint)      as total_weight_g,
    ship_date,
    ship_time_utc,
    delivered_at,
    cast(delivered_at as date)          as delivered_date,
    loaded_at
from {{ source('logistics', 'shipments') }}
