{{
    config(
        materialized='view'
    )
}}

{#- The cost complement per department per fiscal period.

    In basis points, from the FY2026 valuation change. Before that change the
    inventory feed carried a unit cost and this table did not apply.
    docs/inventory-policy.md has the method and the date.
-#}

select
    dept_code,
    fiscal_period                       as fiscal_month,
    cast(cost_complement_bps as integer) as cost_complement_bps,
    approved_by,
    effective_from
from {{ source('reference', 'dept_cost_complement') }}
