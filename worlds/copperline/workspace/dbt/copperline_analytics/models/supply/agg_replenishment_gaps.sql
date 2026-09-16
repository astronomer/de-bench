{{ config(materialized='table') }}

{#-
    Where stock is about to run out, and where it already has.

    Cover is on-hand divided by the recent daily run rate. A location with no
    run rate has infinite cover and is excluded rather than shown as a hundred
    years, because a list sorted by cover with those rows in it is unreadable.

    There is no purchase-order feed, so this model cannot say whether a
    replenishment is already on its way. `in_transit_units` is the closest
    available answer and it comes from the snapshot, not from an order.
-#}

with recent as (

    select
        sku,
        location_id,
        cost_center_key,
        avg(demand_units)                       as daily_demand_units,
        max(demand_units)                       as peak_demand_units,
        sum(demand_units)                       as demand_units_28d,
        bool_or(demand_is_known)                as demand_is_known
    from {{ ref('inventory_position') }}
    where ds > {{ ds_minus(28) }}
    group by 1, 2, 3

),

latest as (

    select
        sku,
        location_id,
        on_hand_units,
        available_units,
        in_transit_units,
        position_as_of,
        position_age_days
    from {{ ref('inventory_position') }}
    where ds = {{ ds() }}

)

select
    {{ ds() }}                                  as ds,
    l.sku,
    l.location_id                               as warehouse_key,
    l.location_id,
    r.cost_center_key,

    l.on_hand_units,
    l.available_units,
    l.in_transit_units,
    l.position_as_of,
    l.position_age_days,

    cast(round(r.daily_demand_units, 2) as decimal(12, 2)) as daily_demand_units,
    r.peak_demand_units,
    r.demand_units_28d,
    r.demand_is_known,

    case
        when r.daily_demand_units > 0
        then cast(round(l.available_units / r.daily_demand_units, 1) as decimal(8, 1))
    end                                         as days_of_cover,

    l.available_units <= 0                      as is_out_of_stock,
    r.daily_demand_units > 0
        and l.available_units / nullif(r.daily_demand_units, 0) < 7 as is_below_one_week,
    r.daily_demand_units > 0
        and l.available_units / nullif(r.daily_demand_units, 0) < 14 as is_below_two_weeks

from latest l
left join recent r on r.sku = l.sku and r.location_id = l.location_id
where coalesce(r.daily_demand_units, 0) > 0
   or l.available_units <= 0
