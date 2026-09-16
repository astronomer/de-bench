select
    account_id,
    account_name,
    tax_id,
    billing_city,
    billing_state,
    country,
    opened_on,
    status,
    owner_rep
from {{ source('nwv', 'accounts') }}
