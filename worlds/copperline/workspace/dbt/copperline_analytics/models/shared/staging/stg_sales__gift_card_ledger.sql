{{
    config(
        materialized='view'
    )
}}

{#- Gift-card entries.

    **The feed signs its own entries and nothing here signs them again.** An
    issue is positive, a redemption and a breakage are negative, so a card's
    balance is `sum(amount_cents)` and not a case statement in four places.
    `magnitude_cents` is the amount without the sign, for the models that state
    the direction themselves.

    Every redemption carries the order it paid for. Most issues carry the order
    the card was bought on, and some do not — a card handed out at a store event
    was never sold. Breakage carries no order at all, because nobody was there.
    None of those gaps is a defect.
-#}

select
    entry_id,
    card_id,
    order_id,
    entry_type,
    entity_code,
    cast(amount_cents as bigint)        as amount_cents,
    abs(cast(amount_cents as bigint)) as magnitude_cents,
    cast(amount_cents as bigint) < 0  as is_money_out,
    occurred_at,
    cast(occurred_at as date)           as occurred_date
from {{ source('sales', 'gift_card_ledger') }}
