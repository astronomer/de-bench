{{ config(materialized='table') }}

{#-
    What each distribution centre held and moved, by day.

    Capacity is not measured anywhere upstream — there is no square-footage or
    slot feed — so this table reports throughput and holding, and calls neither
    of them capacity. `units_held` and `outbound_units` are what the DC did;
    what it could have done is a question this warehouse cannot answer.
-#}

with holding as (

    select
        ds,
        location_id,
        location_kind,
        count(distinct sku)                     as skus_held,
        sum(on_hand_units)                      as units_held,
        sum(in_transit_units)                   as units_in_transit,
        sum(reserved_units)                     as units_reserved
    from {{ ref('inventory_position') }}
    group by 1, 2, 3

),

flow as (

    select
        occurred_date                           as ds,
        coalesce(from_location, to_location)    as location_id,
        count(*)                                as movement_count,
        sum(case when qty < 0 then qty_abs else 0 end) as outbound_units,
        sum(case when qty > 0 then qty_abs else 0 end) as inbound_units,
        count(distinct operator_id)             as operator_count
    from {{ ref('stg_inventory__wms_movements') }}
    where occurred_date > {{ ds_minus(120) }}
    group by 1, 2

),

shipping as (

    select
        ds,
        origin_dc                               as location_id,
        sum(shipment_count)                     as shipment_count,
        sum(package_count)                      as package_count,
        sum(freight_cents)                      as freight_cents
    from {{ ref('int_lane_day') }}
    group by 1, 2

)

select
    h.ds                                        as date_key,
    h.ds,
    h.location_id                               as warehouse_key,
    h.location_id,
    h.location_kind,

    h.skus_held,
    cast(round(h.units_held, 0) as bigint)          as units_held,
    cast(round(h.units_in_transit, 0) as bigint)    as units_in_transit,
    cast(round(h.units_reserved, 0) as bigint)      as units_reserved,

    coalesce(f.movement_count, 0)               as movement_count,
    cast(round(coalesce(f.outbound_units, 0), 0) as bigint) as outbound_units,
    cast(round(coalesce(f.inbound_units, 0), 0) as bigint)  as inbound_units,
    coalesce(f.operator_count, 0)               as operator_count,

    coalesce(s.shipment_count, 0)               as shipment_count,
    coalesce(s.package_count, 0)                as package_count,
    coalesce(s.freight_cents, 0)                as freight_cents

from holding h
left join flow f on f.ds = h.ds and f.location_id = h.location_id
left join shipping s on s.ds = h.ds and s.location_id = h.location_id
where h.location_kind <> 'store'
