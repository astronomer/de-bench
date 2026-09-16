-- Ad hoc, run once for the 2024 board pack. Left here because the numbers in
-- the deck came from it and somebody may ask.
select
    region,
    sum(total_amount) as total_amount
from {{ ref('nwv_branch_performance') }}
group by 1
order by 2 desc
