{{ config(materialized='table') }}

{#-
    The position with the movement replay beside it, and the gap between them.

    This is the table supply works. `variance_units` is how far the movement log
    has drifted from the snapshot for a SKU at a location, and a location whose
    variance is growing every day has usually stopped posting a movement type
    rather than lost the stock.

    Negatives are here on purpose. inventory_position floors them because its
    contract says non-negative, and this is where the floored rows can be found.
-#}

select
    p.ds                                        as date_key,
    p.ds,
    p.sku,
    p.location_id                               as warehouse_key,
    p.location_id,
    p.dept_code                                 as cost_center_key,
    p.location_kind,

    p.on_hand_units,
    p.reserved_units,
    p.in_transit_units,
    p.available_units,
    p.replay_units,
    p.variance_units,
    p.movement_units,
    p.movement_count,

    p.position_as_of,
    p.position_age_days,
    p.is_carried_forward,
    p.last_counted_at,
    date_diff('day', cast(p.last_counted_at as date), p.ds) as days_since_count,

    p.unit_cost_cents,
    p.retail_value_cents,
    p.cost_complement_bps,

    p.on_hand_units < 0                         as is_negative_on_hand,
    abs(p.variance_units) > 0                   as has_variance

from {{ ref('int_inventory_position_daily') }} p
