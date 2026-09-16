select
    rep_id,
    rep_name,
    region,
    hired_on,
    terminated_on
from {{ source('nwv', 'reps') }}
