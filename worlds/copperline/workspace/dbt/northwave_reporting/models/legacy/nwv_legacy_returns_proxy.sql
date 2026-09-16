select
    month_start,
    credit_amount
from {{ ref('nwv_credit_note_summary') }}
where reason in ('return', 'damaged')
