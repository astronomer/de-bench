{{
    config(
        materialized='table'
    )
}}

{#-
    Every place stock can sit.

    Four kinds, and the id prefix says which: `DC-` a distribution centre,
    `HUB-` a cross-dock, `RET-` the returns centre, `S-` a store back room. They
    are one dimension because inventory moves between them and a movement whose
    two ends live in different dimensions cannot be joined.

    The lane table is the second source: an origin DC that has never held stock
    still ships from somewhere. A location present in one source and not the
    other is not an error; `seen_in_inventory` and `seen_in_lanes` say which.
-#}

with inventory_locations as (

    select
        location_id,
        location_kind,
        max(dept_code)              as sample_dept_code,
        count(distinct sku)         as skus_held,
        min(snapshot_date)          as first_snapshot_date,
        max(snapshot_date)          as last_snapshot_date
    from {{ ref('stg_inventory__snapshots') }}
    group by 1, 2

),

lane_origins as (

    select
        origin_dc                   as location_id,
        count(*)                    as lane_count,
        count(distinct dest_country) as countries_served,
        min(origin_tz_name)         as tz_name
    from {{ ref('stg_logistics__lanes') }}
    where is_active
    group by 1

),

all_locations as (

    select location_id from inventory_locations
    union
    select location_id from lane_origins

)

select
    a.location_id                               as warehouse_key,
    a.location_id,

    coalesce(
        i.location_kind,
        case
            when a.location_id like 'S-%'   then 'store'
            when a.location_id like 'HUB-%' then 'hub'
            when a.location_id like 'RET-%' then 'returns'
            else 'dc'
        end
    )                                           as location_kind,

    l.tz_name,
    coalesce(l.lane_count, 0)                   as outbound_lane_count,
    coalesce(l.countries_served, 0)             as countries_served,
    coalesce(i.skus_held, 0)                    as skus_held,
    i.first_snapshot_date,
    i.last_snapshot_date,

    i.location_id is not null                   as seen_in_inventory,
    l.location_id is not null                   as seen_in_lanes,
    l.location_id is not null                   as is_shipping_origin

from all_locations a
left join inventory_locations i on i.location_id = a.location_id
left join lane_origins l on l.location_id = a.location_id
