select
    branch_code,
    branch_name,
    region,
    state_code
from {{ ref('stg_nwv_branches') }}
