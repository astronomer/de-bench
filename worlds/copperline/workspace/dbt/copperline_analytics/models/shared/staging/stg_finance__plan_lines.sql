{{
    config(
        materialized='view'
    )
}}

{#- Trade agreements and their terms.

    A cancelled plan keeps its row and its `cancelled_on`. A plan that replaced
    another names it in `changed_from_plan_line_id`, which is the only way to
    follow an agreement across an amendment.
-#}

select
    plan_line_id,
    plan_id,
    customer_ref,
    nwv_account_id,
    plan_type,
    billing_era,
    billing_frequency,
    currency_code,
    changed_from_plan_line_id,
    cast(amount_cents as bigint)        as amount_cents,
    term_start,
    term_end,
    cancelled_on,
    cancelled_on is not null            as is_cancelled,
    date_diff('day', term_start, term_end) + 1 as term_days
from {{ source('finance', 'plan_lines') }}
