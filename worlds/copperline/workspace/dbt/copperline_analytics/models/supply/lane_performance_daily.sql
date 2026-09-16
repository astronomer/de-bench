{{ config(materialized='table') }}

{#-
    Lane performance by day: transit, on-time and cost.

    The on-time denominator excludes shipments still in transit. A lane whose
    parcels have not arrived yet is not a lane at nought per cent on time, and
    every version of this table that counted them that way triggered an alert
    every morning at four.
-#}

select
    l.ds                                        as date_key,
    l.ds,
    l.lane_id,
    l.origin_dc                                 as warehouse_key,
    l.origin_dc,
    l.dest_region,
    l.dest_country,
    {{ dbt_utils.surrogate_key(['l.carrier_code', 'l.service_family']) }} as carrier_family_key,
    l.carrier_code,
    l.service_family,

    l.shipment_count,
    l.package_count,
    l.delivered_count,
    l.open_count,
    l.on_time_count,
    l.total_weight_g,
    l.billed_weight_g,

    l.freight_cents,
    l.accessorial_cents,
    l.cost_per_package_cents,

    l.average_transit_hours,
    l.worst_transit_hours,
    l.expected_transit_hours,

    cast(round(
        cast(l.on_time_count as decimal(38, 4)) * 10000 / nullif(l.delivered_count, 0), 0
    ) as integer)                               as on_time_rate_bps

from {{ ref('int_lane_day') }} l
