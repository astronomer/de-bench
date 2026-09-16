{{
    config(
        materialized='view'
    )
}}

{#-
    One milestone row per shipment.

    The carrier feeds arrive at package grain: a shipment of four packages is
    four rows, each with its own ship time and its own delivery time. Supply
    reads this to score a lane, and customer reads it to answer "where is my
    order". Both were pivoting the packages themselves and neither was doing it
    the same way — one took the first delivery, the other the last.

    The rule, stated once: **a shipment is delivered when its last package is
    delivered.** A customer with three of four boxes does not have their order.
    `first_delivered_at` is here too, because the partial is what the service
    desk needs, but `is_delivered` and `transit_hours` use the last one.

    A shipment still in transit has no delivery time and no transit hours. It is
    not late and it is not zero; it is not finished.
-#}

with packages as (

    select
        shipment_id,
        package_id,
        billed_weight_g,
        total_freight_cents,
        billed_cents,
        accessorial_cents,
        zone,
        ship_time_utc,
        delivered_at
    from {{ ref('stg_logistics__shipment_packages') }}

),

rolled as (

    select
        shipment_id,
        count(*)                                as package_rows,
        count(delivered_at)                     as delivered_package_rows,
        min(ship_time_utc)                      as first_shipped_at,
        max(ship_time_utc)                      as last_shipped_at,
        min(delivered_at)                       as first_delivered_at,
        max(delivered_at)                       as last_delivered_at,
        sum(billed_weight_g)                    as billed_weight_g,
        sum(billed_cents)                       as freight_billed_cents,
        sum(accessorial_cents)                  as accessorial_cents,
        sum(total_freight_cents)                as total_freight_cents,
        max(zone)                               as max_zone
    from packages
    group by 1

),

shipments as (

    select *
    from {{ ref('stg_logistics__shipments') }}

)

select
    s.shipment_id,
    s.order_id,
    s.carrier_code,
    s.service_level,
    s.lane_id,
    s.origin_dc,
    s.dest_zip3,
    s.dest_country,
    s.brand,
    s.shipment_status,

    s.ship_date,
    coalesce(r.first_shipped_at, s.ship_time_utc)   as shipped_at,
    r.first_delivered_at,
    r.last_delivered_at                             as delivered_at,
    cast(r.last_delivered_at as date)               as delivered_date,

    -- A shipment is delivered when its last package is.
    r.package_rows = r.delivered_package_rows
        and r.delivered_package_rows > 0            as is_delivered,
    r.delivered_package_rows > 0
        and r.package_rows > r.delivered_package_rows as is_part_delivered,

    coalesce(s.package_count, r.package_rows)       as package_count,
    r.package_rows                                  as package_rows_seen,
    r.delivered_package_rows,

    case
        when r.package_rows = r.delivered_package_rows and r.delivered_package_rows > 0
        then date_diff('hour', coalesce(r.first_shipped_at, s.ship_time_utc), r.last_delivered_at)
    end                                             as transit_hours,

    coalesce(s.total_weight_g, r.billed_weight_g)   as total_weight_g,
    r.billed_weight_g,
    r.max_zone                                      as zone,
    coalesce(r.freight_billed_cents, 0)             as freight_billed_cents,
    coalesce(r.accessorial_cents, 0)                as accessorial_cents,
    coalesce(r.total_freight_cents, 0)              as total_freight_cents

from shipments s
left join rolled r on r.shipment_id = s.shipment_id
