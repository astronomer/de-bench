{{ config(materialized='view') }}

{#-
    Carrier scans, normalised across the three carriers.

    Each carrier reports a shipment differently and each one calls the same
    moment something else. This model reduces all of them to two timestamps that
    mean the same thing — when it left, when the last package arrived — and one
    status.

    The service level is normalised to a family (`express`, `ground`,
    `freight`), because the carriers' own names for the same service differ and
    every lane comparison that used the raw names compared the wrong things.

    A shipment in transit has no delivery scan. That is not a late delivery and
    it is not a missing scan; `is_open` says so and every on-time number
    excludes it from the denominator.
-#}

with milestones as (

    select * from {{ ref('int_shipment_milestones') }}

),

lanes as (

    select lane_id, origin_dc, dest_region, dest_country, region_code, origin_tz_name, dest_tz_name
    from {{ ref('stg_logistics__lanes') }}

)

select
    m.shipment_id,
    m.order_id,
    m.carrier_code,
    m.service_level,
    case
        when m.service_level like 'express%' then 'express'
        when m.service_level like 'freight%' then 'freight'
        else 'ground'
    end                                         as service_family,

    m.lane_id,
    l.origin_dc,
    l.dest_region,
    l.dest_country,
    l.region_code,
    l.origin_tz_name,
    l.dest_tz_name,

    m.ship_date,
    m.shipped_at,
    m.first_delivered_at,
    m.delivered_at,
    m.delivered_date,
    m.transit_hours,

    m.package_count,
    m.delivered_package_rows,
    m.total_weight_g,
    m.billed_weight_g,
    m.zone,
    m.total_freight_cents,
    m.freight_billed_cents,
    m.accessorial_cents,

    m.is_delivered,
    m.is_part_delivered,
    not m.is_delivered                          as is_open,
    m.brand,

    -- Expected transit, by family and zone. There is no service-standard feed,
    -- so this is the operations team's own table and it is stated here rather
    -- than in four marts. A change to it moves every on-time number.
    case
        when m.service_level like 'express%' then 24
        when m.service_level like 'freight%' then 120
        when m.service_level = 'ground_2day'  then 48
        else 120
    end                                         as expected_transit_hours,

    case
        when m.is_delivered then m.transit_hours <= case
            when m.service_level like 'express%' then 24
            when m.service_level like 'freight%' then 120
            when m.service_level = 'ground_2day'  then 48
            else 120
        end
    end                                         as is_on_time

from milestones m
left join lanes l on l.lane_id = m.lane_id
