{{
    config(
        materialized='view'
    )
}}

{#- Northwave merge decisions.

    The source of record for whether a Northwave account and a Copperline
    account are the same customer. A decision with a `decided_by` has been made
    by a person; one without has not. Nothing merges an account on confidence
    alone.
-#}

select
    customer_id,
    nwv_account_id,
    cast(confidence as decimal(5, 4))   as confidence,
    method,
    decided_by,
    decided_on,
    decided_by is not null              as is_decided,
    note
from {{ source('ops', 'merge_candidates') }}
