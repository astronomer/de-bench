{{
    config(
        materialized='view'
    )
}}

{#- Service-desk tickets.

    `party_ref` is read against `party_source`: an `oms` ticket names a trade
    account, an `nwv` ticket names a Northwave account. Reading either without
    the other puts a ticket on the wrong customer, and the two id spaces overlap
    enough that the join succeeds while being wrong.

    `category` is free text with a convention — "Area - Detail" — so the area is
    split out here and the full string is kept.
-#}

select
    ticket_id,
    party_ref,
    party_source,
    order_id,
    channel,
    category,
    trim(split_part(category, ' - ', 1)) as category_area,
    cast(csat as integer)                as csat,
    opened_at,
    closed_at,
    cast(opened_at as date)              as opened_date,
    cast(closed_at as date)              as closed_date,
    closed_at is null                    as is_open
from {{ source('support', 'support_tickets') }}
