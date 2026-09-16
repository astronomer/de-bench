{{ config(materialized='table') }}

{#-
    One row per shipment. Supply's published delivery table.
-#}

select
    s.shipment_id                               as shipment_key,
    s.shipment_id,
    s.order_id                                  as order_key,
    s.order_id,

    s.ship_date                                 as date_key,
    s.ship_date,
    s.shipped_at,
    s.first_delivered_at,
    s.delivered_at,
    s.delivered_date,

    {{ dbt_utils.surrogate_key(['s.carrier_code', 's.service_level']) }} as carrier_key,
    s.carrier_code,
    s.service_level,
    s.service_family,
    s.lane_id,
    s.origin_dc                                 as warehouse_key,
    s.origin_dc,
    s.dest_region,
    s.dest_country,
    s.region_code,
    s.brand,

    s.package_count,
    s.delivered_package_rows,
    s.total_weight_g,
    s.billed_weight_g,
    s.zone,

    s.transit_hours,
    s.expected_transit_hours,
    s.is_on_time,
    s.is_delivered,
    s.is_part_delivered,
    s.is_open,

    s.total_freight_cents,
    s.freight_billed_cents,
    s.accessorial_cents

from {{ ref('int_scan_normalized') }} s
