{{
    config(
        materialized='table'
    )
}}

{#-
    The daily inventory position, by SKU and location.

    Two sources say what is on hand and they disagree. The snapshot is what the
    warehouse management system believes; the movement log is what it says it
    did. Replay the movements from a snapshot and you land somewhere else,
    because adjustments are posted against the snapshot and not against the
    movement log.

    **Which one is authoritative is a business decision and it was made once.**
    docs/inventory-policy.md IP-2: the snapshot governs. Every downstream team
    reads the position from this model and never reads the movements. The
    replayed number is here beside it, as `replay_units` and `variance_units`,
    because the size of the gap is how supply-chain finds a location that has
    stopped posting — but it is reporting, not the answer.

    The snapshot cadence is daily for the A-class SKUs and weekly for the tail.
    A weekly SKU has a position on the six days between its snapshots and the
    position is the last one taken, carried forward. That is the whole reason
    this model is a table and not a view: the carry-forward is a range join and
    every reader would pay for it.

    The window is the trailing 400 days from `var('ds')`, which is what
    docs/retention-policy.md RET-2 keeps. Older positions are in the snapshots
    and are not carried forward.
-#}

with spine as (

    select cal_date as ds
    from {{ ref('stg_reference__fiscal_calendar') }}
    where cal_date between {{ ds_minus(400) }} and {{ ds() }}

),

snapshots as (

    select
        snapshot_date,
        sku,
        location_id,
        dept_code,
        location_kind,
        on_hand_units,
        reserved_units,
        in_transit_units,
        unit_cost_cents,
        retail_value_cents,
        cost_complement_bps,
        last_counted_at,
        lead(snapshot_date) over (
            partition by sku, location_id order by snapshot_date
        ) as next_snapshot_date
    from {{ ref('stg_inventory__snapshots') }}
    where snapshot_date >= {{ ds_minus(460) }}

),

carried as (

    select
        s.ds,
        p.sku,
        p.location_id,
        p.dept_code,
        p.location_kind,
        p.snapshot_date                                     as position_as_of,
        date_diff('day', p.snapshot_date, s.ds)             as position_age_days,
        p.on_hand_units,
        p.reserved_units,
        p.in_transit_units,
        p.on_hand_units - p.reserved_units                  as available_units,
        p.unit_cost_cents,
        p.retail_value_cents,
        p.cost_complement_bps,
        p.last_counted_at
    from spine s
    join snapshots p
      on s.ds >= p.snapshot_date
     and s.ds < coalesce(p.next_snapshot_date, {{ ds() }} + 1)

),

-- The replay. Movements are signed at the location they leave and the location
-- they arrive at, so a transfer is two rows here and nets to nothing across the
-- estate. Anything with no location on the relevant side is skipped.
movement_rows as (

    select occurred_date as ds, sku, from_location as location_id, -qty_abs as delta_units
    from {{ ref('stg_inventory__wms_movements') }}
    where from_location is not null
      and occurred_date between {{ ds_minus(400) }} and {{ ds() }}

    union all

    select occurred_date as ds, sku, to_location as location_id, qty_abs as delta_units
    from {{ ref('stg_inventory__wms_movements') }}
    where to_location is not null
      and occurred_date between {{ ds_minus(400) }} and {{ ds() }}

),

movements_daily as (

    select
        ds,
        sku,
        location_id,
        sum(delta_units)    as movement_units,
        count(*)            as movement_count
    from movement_rows
    group by 1, 2, 3

),

-- A running total per SKU and location, so that "everything the log says has
-- happened since the snapshot" is one subtraction rather than a range join.
movements_cumulative as (

    select
        ds,
        sku,
        location_id,
        movement_units,
        movement_count,
        sum(movement_units) over (
            partition by sku, location_id
            order by ds
            rows between unbounded preceding and current row
        ) as cumulative_units
    from movements_daily

),

replayed as (

    select
        c.*,
        coalesce(m.movement_units, 0)   as movement_units,
        coalesce(m.movement_count, 0)   as movement_count,
        -- Position at the last snapshot, plus the log's net movement between
        -- that snapshot and today. This is the number the snapshot is checked
        -- against; it is not the position.
        c.on_hand_units
            + coalesce(asof_now.cumulative_units, 0)
            - coalesce(asof_snap.cumulative_units, 0) as replay_units
    from carried c
    left join movements_daily m
           on m.ds = c.ds and m.sku = c.sku and m.location_id = c.location_id
    asof left join movements_cumulative asof_now
           on asof_now.sku = c.sku
          and asof_now.location_id = c.location_id
          and asof_now.ds <= c.ds
    asof left join movements_cumulative asof_snap
           on asof_snap.sku = c.sku
          and asof_snap.location_id = c.location_id
          and asof_snap.ds <= c.position_as_of

)

select
    ds,
    sku,
    location_id,
    dept_code,
    location_kind,
    position_as_of,
    position_age_days,
    position_age_days > 0                       as is_carried_forward,

    -- The authoritative position. IP-2.
    cast(on_hand_units as decimal(12, 3))       as on_hand_units,
    cast(reserved_units as decimal(12, 3))      as reserved_units,
    cast(in_transit_units as decimal(12, 3))    as in_transit_units,
    cast(available_units as decimal(12, 3))     as available_units,

    -- The movement log's opinion, and the gap. Reporting, not the answer.
    cast(replay_units as decimal(12, 3))                    as replay_units,
    cast(replay_units - on_hand_units as decimal(12, 3))    as variance_units,
    movement_units,
    movement_count,

    unit_cost_cents,
    retail_value_cents,
    cost_complement_bps,
    last_counted_at

from replayed
