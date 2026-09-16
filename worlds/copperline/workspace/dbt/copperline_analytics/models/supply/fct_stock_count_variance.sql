{{ config(materialized='table') }}

{#-
    Cycle counts against the position they were counted against.

    A cycle-count movement is the warehouse correcting itself. The size and the
    direction of the correction is the measurement that matters — a location
    that corrects downward every week is losing stock, and a location that
    corrects in both directions is miscounting.
-#}

with counts as (

    select
        m.movement_id,
        m.occurred_date                         as ds,
        m.sku,
        coalesce(m.from_location, m.to_location) as location_id,
        m.qty                                   as count_adjustment_units,
        m.operator_id
    from {{ ref('stg_inventory__wms_movements') }} m
    where m.movement_type in ('cycle_count', 'adjust')

),

position as (

    select ds, sku, location_id, on_hand_units, cost_center_key, unit_cost_cents
    from {{ ref('inventory_position') }}

)

select
    c.movement_id                               as count_key,
    c.ds                                        as date_key,
    c.ds,
    c.sku,
    c.location_id                               as warehouse_key,
    c.location_id,
    p.cost_center_key,
    c.operator_id,

    c.count_adjustment_units,
    abs(c.count_adjustment_units)               as absolute_adjustment_units,
    p.on_hand_units                             as position_units,

    case
        when p.unit_cost_cents is not null
        then cast(round(c.count_adjustment_units * p.unit_cost_cents, 0) as bigint)
    end                                         as adjustment_value_cents,

    c.count_adjustment_units < 0                as is_shrink,
    c.count_adjustment_units > 0                as is_gain,
    p.ds is null                                as no_position_on_day

from counts c
left join position p on p.ds = c.ds and p.sku = c.sku and p.location_id = c.location_id
