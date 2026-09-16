select
    branch_code,
    branch_name,
    total_amount,
    total_amount * 1.05 as next_year_target
from {{ ref('nwv_branch_performance') }}
