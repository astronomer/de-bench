{{ config(materialized='table') }}

{#-
    What the stock is worth, under the method that applies on the day.

    The valuation method changed at the start of FY2026 and the change is at
    **department** grain, not company grain — docs/inventory-policy.md. Before
    it, the inventory feed carried a unit cost and the value is units times that
    cost. From it, the feed carries a cost complement in basis points and the
    value is the retail value taken down by the complement.

    Neither column is complete on its own: the unit cost stops arriving and the
    complement starts. `valuation_method` says which one produced each row, so a
    period that spans the change can be read without anyone having to remember
    the date.

    A row with neither column is not valued. It is not zero.
-#}

with position as (

    select *
    from {{ ref('inventory_position') }}

),

detail as (

    select
        p.ds,
        p.sku,
        p.location_id,
        p.cost_center_key,
        p.on_hand_units,
        s.unit_cost_cents,
        s.retail_value_cents,
        s.cost_complement_bps
    from position p
    join {{ ref('snap_inventory_daily') }} s
      on s.ds = p.ds and s.sku = p.sku and s.location_id = p.location_id

)

select
    d.ds                                        as date_key,
    d.ds,
    d.sku,
    d.location_id                               as warehouse_key,
    d.location_id,
    d.cost_center_key,

    d.on_hand_units,
    d.unit_cost_cents,
    d.retail_value_cents,
    coalesce(d.cost_complement_bps, c.current_cost_complement_bps) as cost_complement_bps,

    case
        when d.unit_cost_cents is not null then 'unit_cost'
        when coalesce(d.cost_complement_bps, c.current_cost_complement_bps) is not null
             and d.retail_value_cents is not null then 'cost_complement'
        else 'not_valued'
    end                                         as valuation_method,

    case
        when d.unit_cost_cents is not null
            then cast(d.on_hand_units as bigint) * d.unit_cost_cents
        when coalesce(d.cost_complement_bps, c.current_cost_complement_bps) is not null
             and d.retail_value_cents is not null
            then {{ apply_bps('d.retail_value_cents', 'coalesce(d.cost_complement_bps, c.current_cost_complement_bps)') }}
    end                                         as valuation_cents,

    d.retail_value_cents                        as retail_cents

from detail d
left join {{ ref('dim_cost_center') }} c on c.dept_code = d.cost_center_key
