select
    branch_code,
    branch_name,
    region,
    state_code,
    opened_on,
    closed_on
from {{ source('nwv', 'branches') }}
