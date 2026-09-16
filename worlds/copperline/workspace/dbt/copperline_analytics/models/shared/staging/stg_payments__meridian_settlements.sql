{{
    config(
        materialized='view'
    )
}}

{#- Meridian's event stream.

    Restatements: a row with `restates_event_id` set supersedes the row it
    names. Both are kept — the superseded row is what the processor sent and
    dropping it here would make the two feeds impossible to reconcile — and
    `is_restated` marks the ones that have been superseded.

    `deleted_at` rows are tombstones and are dropped: the processor retracts an
    event by deleting it, and a retracted event never happened.

    **The feed signs its own amounts.** A capture is positive, a refund and a
    chargeback are negative. `magnitude_cents` is the amount without the sign.
-#}

with events as (

    select *
    from {{ source('payments', 'pay_meridian_settlements') }}
    where deleted_at is null

),

superseded as (

    select distinct restates_event_id as event_id
    from events
    where restates_event_id is not null

)

select
    e.event_id,
    e.payment_id,
    e.intent_id,
    e.order_ref,
    e.processor_txn_id,
    e.event_type,
    e.currency_code,
    cast(e.amount_cents as bigint)      as amount_cents,
    abs(cast(e.amount_cents as bigint)) as magnitude_cents,
    cast(e.amount_cents as bigint) < 0  as is_money_out,
    cast(e.attempt_no as integer)       as attempt_no,
    e.restates_event_id,
    s.event_id is not null              as is_restated,
    e.event_time_utc,
    e.settlement_date,
    e.loaded_at
from events e
left join superseded s on s.event_id = e.event_id
