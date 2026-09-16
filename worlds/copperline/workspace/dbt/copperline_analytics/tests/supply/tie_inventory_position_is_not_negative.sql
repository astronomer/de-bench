{#-
    Tie: the published position never goes negative.

    contracts/inventory_position.yml pins non-negative on both unit columns, so
    the mart floors them. This test is the check that the flooring is doing its
    job; the rows it would have floored are visible in snap_inventory_daily,
    which is where they are meant to be worked.
-#}

select
    ds,
    sku,
    location_id,
    on_hand_units,
    demand_units
from {{ ref('inventory_position') }}
where on_hand_units < 0
   or demand_units < 0
