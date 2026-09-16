select
    r.rep_id,
    r.rep_name,
    c.book_value,
    c.commission,
    row_number() over (order by c.book_value desc) as rank
from {{ ref('stg_nwv_reps') }} r
join {{ ref('nwv_rep_commission') }} c on c.rep_id = r.rep_id
