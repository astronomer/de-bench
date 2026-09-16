{{
    config(
        materialized='view'
    )
}}

{#- Position by date, SKU and location.

    Two things a reader has to know:

    * The SKU here is the merchandising code — BLD-1024-CLA-4PK-19024 — and not
      the catalog key the order lines use. They are different spellings of the
      same estate and they do not join. int_sku_crosswalk in models/supply is
      where the two are reconciled, as far as they can be.
    * `unit_cost_cents` stops arriving at the FY2026 valuation change and
      `cost_complement_bps` starts. Neither column is complete on its own and
      neither is filled in here. docs/inventory-policy.md has the method.

    Cadence is daily for the A-class SKUs and weekly for the tail, so a date with
    a big row count is a tail date, not a defect.
-#}

select
    snapshot_date,
    sku,
    location_id,
    dept_code,
    cast(on_hand_units as decimal(12, 3))       as on_hand_units,
    cast(reserved_units as decimal(12, 3))      as reserved_units,
    cast(in_transit_units as decimal(12, 3))    as in_transit_units,
    cast(unit_cost_cents as bigint)             as unit_cost_cents,
    cast(retail_value_cents as bigint)          as retail_value_cents,
    cast(cost_complement_bps as integer)        as cost_complement_bps,
    case
        when location_id like 'S-%' then 'store'
        when location_id like 'HUB-%' then 'hub'
        when location_id like 'RET-%' then 'returns'
        else 'dc'
    end                                         as location_kind,
    last_counted_at,
    loaded_at
from {{ source('inventory', 'inventory_snapshots') }}
