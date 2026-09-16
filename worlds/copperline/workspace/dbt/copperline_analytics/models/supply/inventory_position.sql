{{ config(materialized='table') }}

{#-
    The published position. contracts/inventory_position.yml, consumer C-7.

    The snapshot governs; the movement replay is reporting. That decision is
    docs/inventory-policy.md IP-2 and it is made in
    int_inventory_position_daily, not here.

    `demand_units` is a reserved name: units ordered, returns not deducted, at
    SKU-day grain — docs/semantic-definitions.md. Getting it onto this table
    means crossing the two SKU spellings through int_sku_crosswalk, and for most
    SKUs the crossing fails. `demand_is_known` says which rows have a real
    demand number and which have a zero that means "no match", and the
    replenishment feed reads that column before it reads the demand.

    The contract pins non-negative on both unit columns, so the position is
    floored at zero. A negative on-hand is a data problem and it is reported in
    `snap_inventory_daily`, not published here.
-#}

with position as (

    select *
    from {{ ref('int_inventory_position_daily') }}
    where ds > {{ ds_minus(120) }}

),

crosswalk as (

    select merch_sku, catalog_sku, is_matched
    from {{ ref('int_sku_crosswalk') }}

),

demand as (

    -- Units ordered, from the shared line model rather than from commerce's
    -- daily product mart: `demand_units` is a reserved name and both teams have
    -- to compute it from the same place. Rule 2.
    select
        order_date                              as ds,
        sku                                     as catalog_sku,
        sum(qty)                                as demand_units
    from {{ ref('int_net_sales_lines') }}
    where order_date > {{ ds_minus(120) }}
    group by 1, 2

)

select
    p.ds,
    p.sku,
    p.location_id,
    p.dept_code                                 as cost_center_key,
    p.location_kind,

    greatest(cast(round(p.on_hand_units, 0) as integer), 0)     as on_hand_units,
    greatest(cast(round(p.available_units, 0) as integer), 0)   as available_units,
    greatest(cast(round(p.reserved_units, 0) as integer), 0)    as reserved_units,
    greatest(cast(round(p.in_transit_units, 0) as integer), 0)  as in_transit_units,

    greatest(cast(round(coalesce(d.demand_units, 0), 0) as integer), 0) as demand_units,
    x.is_matched                                as demand_is_known,
    x.catalog_sku,

    p.position_as_of,
    p.position_age_days,
    p.is_carried_forward,
    p.unit_cost_cents,
    p.cost_complement_bps

from position p
left join crosswalk x on x.merch_sku = p.sku
left join demand d on d.ds = p.ds and d.catalog_sku = x.catalog_sku
