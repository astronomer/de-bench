{{
    config(
        materialized='view'
    )
}}

{#- One row per package.

    This is the grain freight is billed at. `billed_cents` is the carrier's
    charge for the package and `accessorial_cents` is everything they added to
    it. A shipment-level freight number is a sum over this table, never a column
    on the shipment.
-#}

select
    package_id,
    shipment_id,
    order_id,
    carrier_code,
    service_level,
    lane_id,
    origin_dc,
    dest_zip3,
    dest_country,
    brand,
    cast(zone as integer)                       as zone,
    cast(billed_weight_g as bigint)             as billed_weight_g,
    cast(billed_cents as bigint)                as billed_cents,
    cast(coalesce(accessorial_cents, 0) as bigint) as accessorial_cents,
    cast(billed_cents as bigint)
        + cast(coalesce(accessorial_cents, 0) as bigint) as total_freight_cents,
    ship_date,
    ship_time_utc,
    delivered_at,
    cast(delivered_at as date)                  as delivered_date,
    loaded_at
from {{ source('logistics', 'shipment_packages') }}
